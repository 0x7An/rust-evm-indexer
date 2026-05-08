use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::decoder::{RpcLog, TokenStandard, supported_topic0_values};

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
