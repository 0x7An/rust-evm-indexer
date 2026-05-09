use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::decoder::{RpcLog, TokenStandard, supported_topic0_values};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    pub hash: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct EvmRpcClient {
    client: Client,
    url: String,
}

impl EvmRpcClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            url: url.into(),
        }
    }

    pub async fn block_number(&self) -> Result<u64> {
        let value = self.call("eth_blockNumber", json!([])).await?;
        let block = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("eth_blockNumber result was not a string"))?;
        super::decoder::parse_hex_u64(block)
    }

    pub async fn code_at(&self, contract_address: &str, block: u64) -> Result<String> {
        let value = self
            .call(
                "eth_getCode",
                json!([contract_address, format!("0x{block:x}")]),
            )
            .await?;
        value
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!("eth_getCode result was not a string"))
    }

    pub async fn block_hash(&self, block: u64) -> Result<String> {
        Ok(self.block_metadata(block).await?.hash)
    }

    pub async fn block_timestamp(&self, block: u64) -> Result<DateTime<Utc>> {
        Ok(self.block_metadata(block).await?.timestamp)
    }

    pub async fn block_metadata(&self, block: u64) -> Result<BlockMetadata> {
        let value = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;

        let hash = value
            .get("hash")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| anyhow::anyhow!("eth_getBlockByNumber result missing hash"))?;
        let timestamp_hex = value
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("eth_getBlockByNumber result missing timestamp"))?;
        let timestamp_seconds = super::decoder::parse_hex_u64(timestamp_hex)
            .with_context(|| format!("parse block timestamp for block {block}"))?;
        let timestamp_seconds = i64::try_from(timestamp_seconds)
            .with_context(|| format!("block {block} timestamp is too large"))?;
        let timestamp = DateTime::<Utc>::from_timestamp(timestamp_seconds, 0)
            .ok_or_else(|| anyhow::anyhow!("block {block} timestamp is out of range"))?;

        Ok(BlockMetadata { hash, timestamp })
    }

    pub async fn logs(
        &self,
        contract_address: &str,
        standard: TokenStandard,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<RpcLog>> {
        let topics = supported_topic0_values(standard);
        let params = json!([{
            "address": contract_address,
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
            "topics": [topics],
        }]);

        let value = self.call("eth_getLogs", params).await?;
        serde_json::from_value(value).context("decode eth_getLogs response")
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let response = self
            .client
            .post(&self.url)
            .json(&JsonRpcRequest {
                jsonrpc: "2.0",
                id: 1,
                method,
                params,
            })
            .send()
            .await
            .with_context(|| format!("send {method} request"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .with_context(|| format!("decode {method} response"))?;
        let rpc_response = serde_json::from_str::<JsonRpcResponse>(&body);

        if !status.is_success() {
            if let Ok(response) = rpc_response {
                if let Some(error) = response.error {
                    bail!("RPC {method} error {}: {}", error.code, error.message);
                }
            }
            bail!(
                "RPC {method} HTTP status {status}: {}",
                truncate_body(&body)
            );
        }

        let response = rpc_response.with_context(|| format!("decode {method} response"))?;

        if let Some(error) = response.error {
            bail!("RPC {method} error {}: {}", error.code, error.message);
        }

        response
            .result
            .ok_or_else(|| anyhow::anyhow!("RPC {method} response missing result"))
    }
}

fn truncate_body(body: &str) -> String {
    const LIMIT: usize = 500;
    if body.chars().count() <= LIMIT {
        return body.to_string();
    }

    format!("{}...", body.chars().take(LIMIT).collect::<String>())
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}
