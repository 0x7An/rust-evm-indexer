use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::json;
use uuid::Uuid;

use crate::application::ports::{
    ChainRpc, IndexedBlockHash, ReorgEventInsert, ReorgRepository, SourceDescriptor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgVerification {
    pub source_id: Uuid,
    pub checked_blocks: usize,
    pub mismatches: Vec<ReorgMismatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgMismatch {
    pub block_number: i64,
    pub expected_block_hash: String,
    pub actual_block_hash: String,
}

pub async fn verify_source_reorgs(
    rpc: &impl ChainRpc,
    ledger: &impl ReorgRepository,
    source: &impl SourceDescriptor,
    from_block: u64,
    to_block: u64,
) -> Result<ReorgVerification> {
    if from_block > to_block {
        bail!("from-block {from_block} cannot be greater than to-block {to_block}");
    }
    let from = i64::try_from(from_block).context("from-block exceeds postgres bigint storage")?;
    let to = i64::try_from(to_block).context("to-block exceeds postgres bigint storage")?;

    let mut expected = ledger
        .indexed_block_hashes(source.source_id(), from, to)
        .context("load indexed block hashes")?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(checkpoint) = ledger
        .checkpoint_for_source(source.source_id())
        .context("load checkpoint for reorg verification")?
        && checkpoint.processed_block >= from
        && checkpoint.processed_block <= to
    {
        expected.insert(IndexedBlockHash {
            block_number: checkpoint.processed_block,
            block_hash: checkpoint.processed_block_hash,
        });
    }

    let mut mismatches = Vec::new();
    let checked_blocks = expected.len();
    for indexed in expected {
        let actual = rpc
            .block_hash(indexed.block_number as u64)
            .await
            .with_context(|| format!("fetch canonical block hash {}", indexed.block_number))?
            .to_ascii_lowercase();
        let expected_hash = indexed.block_hash.to_ascii_lowercase();
        if expected_hash != actual {
            mismatches.push(ReorgMismatch {
                block_number: indexed.block_number,
                expected_block_hash: expected_hash,
                actual_block_hash: actual,
            });
        }
    }
    record_reorg_ranges(ledger, source, &mismatches)?;

    Ok(ReorgVerification {
        source_id: source.source_id(),
        checked_blocks,
        mismatches,
    })
}

fn record_reorg_ranges(
    ledger: &impl ReorgRepository,
    source: &impl SourceDescriptor,
    mismatches: &[ReorgMismatch],
) -> Result<()> {
    let Some(first) = mismatches.first() else {
        return Ok(());
    };

    let mut range_start = 0usize;
    let mut previous = first;
    for (index, mismatch) in mismatches.iter().enumerate().skip(1) {
        if mismatch.block_number == previous.block_number + 1 {
            previous = mismatch;
            continue;
        }

        record_reorg_range(ledger, source, &mismatches[range_start..index])?;
        range_start = index;
        previous = mismatch;
    }
    record_reorg_range(ledger, source, &mismatches[range_start..])
}

fn record_reorg_range(
    ledger: &impl ReorgRepository,
    source: &impl SourceDescriptor,
    range: &[ReorgMismatch],
) -> Result<()> {
    let first = range.first().context("reorg range cannot be empty")?;
    let last = range.last().context("reorg range cannot be empty")?;
    ledger
        .record_reorg_event(ReorgEventInsert {
            source_id: source.source_id(),
            chain_id: source.chain_id(),
            from_block: first.block_number,
            to_block: last.block_number,
            expected_block_hash: Some(first.expected_block_hash.clone()),
            actual_block_hash: Some(first.actual_block_hash.clone()),
            replay_job_id: None,
            mismatches: json!(
                range
                    .iter()
                    .map(|mismatch| {
                        json!({
                            "block_number": mismatch.block_number,
                            "expected_block_hash": mismatch.expected_block_hash,
                            "actual_block_hash": mismatch.actual_block_hash,
                        })
                    })
                    .collect::<Vec<_>>()
            ),
        })
        .context("record reorg event")
        .map(|_| ())
}
