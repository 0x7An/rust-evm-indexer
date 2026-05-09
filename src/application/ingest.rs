use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use crate::infra::{
    evm::{
        decoder::{
            RpcLog, TokenStandard, decode_log, detect_token_standard_from_log, parse_hex_u64,
        },
        rpc::{EvmRpcClient, RpcTransactionReceipt},
    },
    postgres::{
        ledger_repository::{LedgerRepository, PersistDecodedLogsOptions, ScanSummary},
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
    pub restore_orphaned_conflicts: bool,
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

    let mut standard = source
        .token_standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {}", source.token_standard))?;
    if standard.is_auto() {
        standard = detect_token_standard(
            rpc,
            &source.contract_address,
            from,
            to,
            chunk_size,
            options.progress,
        )
        .await
        .context("detect source token standard")?;
    }

    let chain_label = format!("chain {}", source.chain_id);
    validate_contract_code_at_boundaries(rpc, &source.contract_address, &chain_label, from, to)
        .await?;

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
        .persist_decoded_logs_with_options(
            source,
            &decoded,
            PersistDecodedLogsOptions {
                restore_orphaned_conflicts: options.restore_orphaned_conflicts,
            },
        )
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

pub async fn validate_contract_code_at_boundaries(
    rpc: &EvmRpcClient,
    contract: &str,
    chain_label: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }

    validate_contract_code_at_block(rpc, contract, chain_label, from).await?;
    if to != from {
        validate_contract_code_at_block(rpc, contract, chain_label, to).await?;
    }

    Ok(())
}

async fn validate_contract_code_at_block(
    rpc: &EvmRpcClient,
    contract: &str,
    chain_label: &str,
    block: u64,
) -> Result<()> {
    let code = rpc
        .code_at(contract, block)
        .await
        .with_context(|| format!("fetch contract code at block {block}"))?;
    if code.trim() == "0x" {
        bail!("no contract code at {contract} on {chain_label} at boundary block {block}");
    }

    Ok(())
}

pub async fn detect_token_standard(
    rpc: &EvmRpcClient,
    contract: &str,
    from: u64,
    to: u64,
    chunk_size: u64,
    progress: bool,
) -> Result<TokenStandard> {
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }
    if chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let mut chunk_from = from;
    let total_chunks = (((to - from) / chunk_size) + 1) as usize;
    let mut chunk_number = 0usize;

    while chunk_from <= to {
        let chunk_to = chunk_from.saturating_add(chunk_size - 1).min(to);
        chunk_number += 1;
        let logs = rpc
            .logs(contract, TokenStandard::Auto, chunk_from, chunk_to)
            .await
            .with_context(|| {
                format!(
                    "fetch contract logs for token standard detection {chunk_from}..={chunk_to}"
                )
            })?;
        if let Some(standard) = detect_token_standard_from_logs(&logs)? {
            return Ok(standard);
        }
        if progress && should_report_progress(chunk_number, total_chunks) {
            println!(
                "Checked token standard detection chunk {chunk_number}/{total_chunks} for blocks {chunk_from}..={chunk_to}."
            );
        }

        if chunk_to == u64::MAX {
            break;
        }
        chunk_from = chunk_to + 1;
    }

    bail!(
        "could not auto-detect token standard for {contract} over blocks {from}..={to}; \
         no ERC-20, ERC-721, or ERC-1155 transfer logs were found"
    )
}

pub fn detect_token_standard_from_logs(logs: &[RpcLog]) -> Result<Option<TokenStandard>> {
    let mut detected: Option<TokenStandard> = None;
    for log in logs {
        let Some(candidate) = detect_token_standard_from_log(log)? else {
            continue;
        };
        if let Some(existing) = detected {
            if existing != candidate {
                bail!(
                    "conflicting token standards detected in logs: {} and {}",
                    existing.as_str(),
                    candidate.as_str()
                );
            }
        } else {
            detected = Some(candidate);
        }
    }
    Ok(detected)
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
    use crate::infra::evm::decoder::event_topic;

    #[test]
    fn redacts_provider_key_from_rpc_url() {
        assert_eq!(
            redact_rpc_url("https://eth-mainnet.g.alchemy.com/v2/example-key"),
            "https://eth-mainnet.g.alchemy.com/v2/<redacted>"
        );
    }

    #[test]
    fn detects_erc721_from_auto_transfer_shape() {
        let logs = vec![test_log(
            vec![
                event_topic("Transfer(address,address,uint256)"),
                topic_address_word("11"),
                topic_address_word("22"),
                format!("0x{:064x}", 42),
            ],
            "0x",
        )];

        assert_eq!(
            detect_token_standard_from_logs(&logs).unwrap(),
            Some(TokenStandard::Erc721)
        );
    }

    #[test]
    fn detects_erc20_from_auto_transfer_shape() {
        let logs = vec![test_log(
            vec![
                event_topic("Transfer(address,address,uint256)"),
                topic_address_word("11"),
                topic_address_word("22"),
            ],
            &format!("0x{:064x}", 1_000),
        )];

        assert_eq!(
            detect_token_standard_from_logs(&logs).unwrap(),
            Some(TokenStandard::Erc20)
        );
    }

    #[test]
    fn detects_erc1155_from_auto_transfer_shape() {
        let logs = vec![test_log(
            vec![
                event_topic("TransferSingle(address,address,address,uint256,uint256)"),
                topic_address_word("11"),
                topic_address_word("22"),
                topic_address_word("33"),
            ],
            &format!("0x{:064x}{:064x}", 42, 3),
        )];

        assert_eq!(
            detect_token_standard_from_logs(&logs).unwrap(),
            Some(TokenStandard::Erc1155)
        );
    }

    #[test]
    fn auto_detection_returns_none_without_supported_logs() {
        let logs = vec![test_log(
            vec![event_topic("Approval(address,address,uint256)")],
            "0x",
        )];

        assert_eq!(detect_token_standard_from_logs(&logs).unwrap(), None);
    }

    fn topic_address_word(byte: &str) -> String {
        format!("0x{}{}", "00".repeat(12), byte.repeat(20))
    }

    fn test_log(topics: Vec<String>, data: &str) -> RpcLog {
        RpcLog {
            address: "0x1000000000000000000000000000000000000000".to_string(),
            topics,
            data: data.to_string(),
            block_number: "0x1".to_string(),
            transaction_hash: format!("0x{}", "aa".repeat(32)),
            transaction_index: Some("0x0".to_string()),
            log_index: "0x0".to_string(),
            block_hash: format!("0x{}", "bb".repeat(32)),
            block_timestamp: None,
        }
    }
}
