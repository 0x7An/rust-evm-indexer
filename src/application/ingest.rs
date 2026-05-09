use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use crate::infra::{
    evm::{
        decoder::{RpcLog, TokenStandard, decode_log, parse_hex_u64},
        rpc::{EvmRpcClient, RpcTransactionReceipt},
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestOptions {
    pub include_transaction_receipts: bool,
    pub progress: bool,
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
    options: IngestOptions,
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

    let mut logs = fetch_logs_in_chunks_with_progress(
        rpc,
        &source.contract_address,
        standard,
        from,
        to,
        chunk_size,
        options.progress,
    )
    .await?;
    attach_block_timestamps(rpc, &mut logs, options.progress).await?;

    let mut decoded = Vec::new();
    let mut transaction_hashes = BTreeSet::new();
    for log in logs {
        if let Some(decoded_log) = decode_log(&log, standard).context("decode log")? {
            transaction_hashes.insert(log.transaction_hash.to_ascii_lowercase());
            decoded.push((log, decoded_log));
        }
    }
    if options.progress {
        println!(
            "Decoded {} matching transfer logs with {} unique transactions.",
            decoded.len(),
            transaction_hashes.len()
        );
    }

    let mut summary = ledger
        .persist_decoded_logs(source, &decoded)
        .context("persist ledger")?;

    if options.include_transaction_receipts {
        let receipts = fetch_transaction_receipts(rpc, transaction_hashes, options.progress)
            .await
            .context("fetch transaction receipts")?;
        summary.transaction_receipts_persisted = ledger
            .persist_transaction_receipts(source.chain_id, &receipts)
            .context("persist transaction receipts")?;
    }

    Ok(summary)
}

pub async fn fetch_logs_in_chunks(
    rpc: &EvmRpcClient,
    contract: &str,
    standard: TokenStandard,
    from: u64,
    to: u64,
    chunk_size: u64,
) -> Result<Vec<RpcLog>> {
    fetch_logs_in_chunks_with_progress(rpc, contract, standard, from, to, chunk_size, false).await
}

async fn fetch_logs_in_chunks_with_progress(
    rpc: &EvmRpcClient,
    contract: &str,
    standard: TokenStandard,
    from: u64,
    to: u64,
    chunk_size: u64,
    progress: bool,
) -> Result<Vec<RpcLog>> {
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }
    if chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let mut logs = Vec::new();
    let mut chunk_from = from;
    let total_chunks = (((to - from) / chunk_size) + 1) as usize;
    let mut chunk_number = 0usize;

    while chunk_from <= to {
        let chunk_to = chunk_from.saturating_add(chunk_size - 1).min(to);
        chunk_number += 1;
        let chunk_logs = rpc
            .logs(contract, standard, chunk_from, chunk_to)
            .await
            .with_context(|| format!("fetch contract logs {chunk_from}..={chunk_to}"))?;
        logs.extend(chunk_logs);
        if progress && should_report_progress(chunk_number, total_chunks) {
            println!(
                "Fetched log chunk {chunk_number}/{total_chunks} for blocks {chunk_from}..={chunk_to}; logs so far: {}.",
                logs.len()
            );
        }

        if chunk_to == u64::MAX {
            break;
        }
        chunk_from = chunk_to + 1;
    }

    Ok(logs)
}

async fn attach_block_timestamps(
    rpc: &EvmRpcClient,
    logs: &mut [RpcLog],
    progress: bool,
) -> Result<()> {
    let mut timestamps = BTreeMap::new();
    for log in logs.iter() {
        let block = parse_hex_u64(&log.block_number).context("parse log block number")?;
        timestamps.entry(block).or_insert(None);
    }

    if progress && !timestamps.is_empty() {
        println!(
            "Fetching block timestamps for {} event blocks.",
            timestamps.len()
        );
    }
    let total_blocks = timestamps.len();
    let mut block_number = 0;
    for (block, timestamp) in timestamps.iter_mut() {
        block_number += 1;
        *timestamp = Some(
            rpc.block_timestamp(*block)
                .await
                .with_context(|| format!("fetch block timestamp for block {block}"))?,
        );
        if progress && should_report_progress(block_number, total_blocks) {
            println!("Fetched block timestamp {block_number}/{total_blocks}.");
        }
    }

    for log in logs {
        let block = parse_hex_u64(&log.block_number).context("parse log block number")?;
        log.block_timestamp = timestamps.get(&block).cloned().flatten();
    }

    Ok(())
}

async fn fetch_transaction_receipts(
    rpc: &EvmRpcClient,
    transaction_hashes: BTreeSet<String>,
    progress: bool,
) -> Result<Vec<RpcTransactionReceipt>> {
    let mut receipts = Vec::with_capacity(transaction_hashes.len());
    let total_receipts = transaction_hashes.len();
    if progress && total_receipts > 0 {
        println!("Fetching transaction receipts for {total_receipts} unique transactions.");
    }
    for (index, transaction_hash) in transaction_hashes.into_iter().enumerate() {
        receipts.push(
            rpc.transaction_receipt(&transaction_hash)
                .await
                .with_context(|| format!("fetch transaction receipt {transaction_hash}"))?,
        );
        let receipt_number = index + 1;
        if progress && should_report_progress(receipt_number, total_receipts) {
            println!("Fetched transaction receipt {receipt_number}/{total_receipts}.");
        }
    }

    Ok(receipts)
}

fn should_report_progress(done: usize, total: usize) -> bool {
    done == 1 || done == total || done.is_multiple_of(100)
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
