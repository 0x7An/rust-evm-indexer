use std::time::Duration as StdDuration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::application::{
    evm::{RpcLog, TokenStandard, parse_hex_u64, supported_topic0_values},
    ports::{ChainRpc, TransactionReceipt},
};
use crate::infra::telemetry::metrics;

pub type RpcTransactionReceipt = TransactionReceipt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    pub hash: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct EvmRpcClient {
    client: Client,
    url: String,
    metric_chain_id: Option<String>,
}

impl EvmRpcClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(StdDuration::from_secs(DEFAULT_RPC_TIMEOUT_SECONDS))
                .build()
                .expect("build EVM RPC HTTP client"),
            url: url.into(),
            metric_chain_id: None,
        }
    }

    pub fn with_metric_chain_id(mut self, chain_id: i64) -> Self {
        self.metric_chain_id = Some(chain_id.to_string());
        self
    }

    pub async fn block_number(&self) -> Result<u64> {
        let value = self.call("eth_blockNumber", json!([])).await?;
        let block = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("eth_blockNumber result was not a string"))?;
        parse_hex_u64(block)
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
        let timestamp_seconds = parse_hex_u64(timestamp_hex)
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

    pub async fn transaction_receipt(&self, transaction_hash: &str) -> Result<TransactionReceipt> {
        let value = self
            .call("eth_getTransactionReceipt", json!([transaction_hash]))
            .await?;
        if value.is_null() {
            bail!("eth_getTransactionReceipt returned null for {transaction_hash}");
        }

        let fields = serde_json::from_value::<RpcTransactionReceiptFields>(value.clone())
            .context("decode eth_getTransactionReceipt response")?;
        Ok(TransactionReceipt {
            transaction_hash: fields.transaction_hash,
            transaction_index: fields.transaction_index,
            block_hash: fields.block_hash,
            block_number: fields.block_number,
            from: fields.from,
            to: fields.to,
            contract_address: fields.contract_address,
            status: fields.status,
            gas_used: fields.gas_used,
            cumulative_gas_used: fields.cumulative_gas_used,
            effective_gas_price: fields.effective_gas_price,
            transaction_type: fields.transaction_type,
            raw: value,
        })
    }

    #[tracing::instrument(
        name = "rpc_fetch",
        skip_all,
        fields(
            rpc_method = method,
            chain_id = self.metric_chain_id(),
        )
    )]
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let result = self.call_inner(method, params).await;
        if let Err(error) = &result {
            metrics::metrics().inc_rpc_error(self.metric_chain_id(), "RpcError");
            tracing::warn!(
                error_class = "RpcError",
                error = %error,
                "RPC request failed"
            );
        }
        result
    }

    fn metric_chain_id(&self) -> &str {
        self.metric_chain_id.as_deref().unwrap_or("unknown")
    }

    async fn call_inner(&self, method: &str, params: Value) -> Result<Value> {
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
            if let Ok(response) = rpc_response
                && let Some(error) = response.error
            {
                bail!("RPC {method} error {}: {}", error.code, error.message);
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

impl ChainRpc for EvmRpcClient {
    async fn block_number(&self) -> Result<u64> {
        EvmRpcClient::block_number(self).await
    }

    async fn code_at(&self, contract_address: &str, block: u64) -> Result<String> {
        EvmRpcClient::code_at(self, contract_address, block).await
    }

    async fn block_hash(&self, block: u64) -> Result<String> {
        EvmRpcClient::block_hash(self, block).await
    }

    async fn block_timestamp(&self, block: u64) -> Result<chrono::DateTime<Utc>> {
        EvmRpcClient::block_timestamp(self, block).await
    }

    async fn logs(
        &self,
        contract_address: &str,
        standard: TokenStandard,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<RpcLog>> {
        EvmRpcClient::logs(self, contract_address, standard, from_block, to_block).await
    }

    async fn transaction_receipt(&self, transaction_hash: &str) -> Result<TransactionReceipt> {
        EvmRpcClient::transaction_receipt(self, transaction_hash).await
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

const DEFAULT_RPC_TIMEOUT_SECONDS: u64 = 60;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcTransactionReceiptFields {
    transaction_hash: String,
    transaction_index: String,
    block_hash: String,
    block_number: String,
    from: String,
    to: Option<String>,
    contract_address: Option<String>,
    status: Option<String>,
    gas_used: String,
    cumulative_gas_used: String,
    effective_gas_price: Option<String>,
    #[serde(rename = "type")]
    transaction_type: Option<String>,
}
