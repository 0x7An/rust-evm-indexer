use anyhow::{Context, Result, bail};

use crate::infra::{
    evm::{
        decoder::{RpcLog, TokenStandard, decode_log},
        rpc::EvmRpcClient,
    },
    postgres::{
        ledger_repository::{LedgerRepository, ScanSummary},
        models::SourceRow,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRange {
    pub from: u64,
    pub to: u64,
    pub finalized_head: u64,
}

pub async fn resolve_finalized_range(
    rpc: &EvmRpcClient,
    from_block: Option<&str>,
    to_block: &str,
    lookback: u64,
    finality_confirmations: i64,
) -> Result<ResolvedRange> {
    if finality_confirmations < 0 {
        bail!("finality-confirmations cannot be negative");
    }

    let head = rpc.block_number().await.context("fetch head block")?;
    let finalized_head = head.saturating_sub(finality_confirmations as u64);
    let to = parse_block_arg(to_block, finalized_head)?;
    let from = match from_block {
        Some(value) => parse_block_arg(value, finalized_head)?,
        None => to.saturating_sub(lookback),
    };

    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }

    Ok(ResolvedRange {
        from,
        to,
        finalized_head,
    })
}

pub async fn ingest_source_range(
    rpc: &EvmRpcClient,
    ledger: &LedgerRepository,
    source: &SourceRow,
    from: u64,
    to: u64,
    chunk_size: u64,
) -> Result<ScanSummary> {
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }
    if chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let standard = source
        .token_standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {}", source.token_standard))?;

    let code = rpc
        .code_at(&source.contract_address, to)
        .await
        .with_context(|| format!("fetch contract code at block {to}"))?;
    if code == "0x" {
        bail!(
            "no contract code at {} on chain {} at block {to}",
            source.contract_address,
            source.chain_id
        );
    }

    let logs = fetch_logs_in_chunks(
        rpc,
        &source.contract_address,
        standard,
        from,
        to,
        chunk_size,
    )
    .await?;
    let mut decoded = Vec::new();
    for log in logs {
        if let Some(decoded_log) = decode_log(&log, standard).context("decode log")? {
            decoded.push((log, decoded_log));
        }
    }

    ledger
        .persist_decoded_logs(source, &decoded)
        .context("persist ledger")
}

pub async fn fetch_logs_in_chunks(
    rpc: &EvmRpcClient,
    contract: &str,
    standard: TokenStandard,
    from: u64,
    to: u64,
    chunk_size: u64,
) -> Result<Vec<RpcLog>> {
    let mut logs = Vec::new();
    let mut chunk_from = from;

    while chunk_from <= to {
        let chunk_to = chunk_from.saturating_add(chunk_size - 1).min(to);
        let chunk_logs = rpc
            .logs(contract, standard, chunk_from, chunk_to)
            .await
            .with_context(|| format!("fetch contract logs {chunk_from}..={chunk_to}"))?;
        logs.extend(chunk_logs);

        if chunk_to == u64::MAX {
            break;
        }
        chunk_from = chunk_to + 1;
    }

    Ok(logs)
}

pub fn parse_block_arg(value: &str, latest: u64) -> Result<u64> {
    if value == "latest" {
        return Ok(latest);
    }

    if let Some(hex) = value.strip_prefix("0x") {
        return u64::from_str_radix(hex, 16).with_context(|| format!("parse hex block {value}"));
    }

    value
        .parse::<u64>()
        .with_context(|| format!("parse decimal block {value}"))
}

pub fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if normalized.len() != 42
        || !normalized.starts_with("0x")
        || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        bail!("invalid EVM address: {value}");
    }
    Ok(normalized)
}

pub fn redact_rpc_url(value: &str) -> String {
    for marker in ["/v2/", "/v3/"] {
        if let Some(index) = value.find(marker) {
            let end = index + marker.len();
            return format!("{}<redacted>", &value[..end]);
        }
    }

    "<configured>".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_provider_key_from_rpc_url() {
        assert_eq!(
            redact_rpc_url("https://eth-mainnet.g.alchemy.com/v2/example-key"),
            "https://eth-mainnet.g.alchemy.com/v2/<redacted>"
        );
    }
}
