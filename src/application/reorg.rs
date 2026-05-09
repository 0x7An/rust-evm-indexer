use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use uuid::Uuid;

use crate::infra::{
    evm::rpc::EvmRpcClient,
    postgres::{
        ledger_repository::{IndexedBlockHash, LedgerRepository, ReorgEventInsert},
        models::SourceRow,
    },
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
    rpc: &EvmRpcClient,
    ledger: &LedgerRepository,
    source: &SourceRow,
    from_block: u64,
    to_block: u64,
) -> Result<ReorgVerification> {
    if from_block > to_block {
        bail!("from-block {from_block} cannot be greater than to-block {to_block}");
    }
    let from = i64::try_from(from_block).context("from-block exceeds postgres bigint storage")?;
    let to = i64::try_from(to_block).context("to-block exceeds postgres bigint storage")?;

    let mut expected = ledger
        .indexed_block_hashes(source.id, from, to)
        .context("load indexed block hashes")?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(checkpoint) = ledger
        .checkpoint_for_source(source.id)
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
        source_id: source.id,
        checked_blocks,
        mismatches,
    })
}

fn record_reorg_ranges(
    ledger: &LedgerRepository,
    source: &SourceRow,
    mismatches: &[ReorgMismatch],
) -> Result<()> {
    let Some(first) = mismatches.first() else {
        return Ok(());
    };

    let mut range_start = first;
    let mut previous = first;
    for mismatch in &mismatches[1..] {
        if mismatch.block_number == previous.block_number + 1 {
            previous = mismatch;
            continue;
        }

        record_reorg_range(ledger, source, range_start, previous)?;
        range_start = mismatch;
        previous = mismatch;
    }
    record_reorg_range(ledger, source, range_start, previous)
}

fn record_reorg_range(
    ledger: &LedgerRepository,
    source: &SourceRow,
    first: &ReorgMismatch,
    last: &ReorgMismatch,
) -> Result<()> {
    ledger
        .record_reorg_event(ReorgEventInsert {
            source_id: source.id,
            chain_id: source.chain_id,
            from_block: first.block_number,
            to_block: last.block_number,
            expected_block_hash: Some(first.expected_block_hash.clone()),
            actual_block_hash: Some(first.actual_block_hash.clone()),
            replay_job_id: None,
        })
        .context("record reorg event")
        .map(|_| ())
}
