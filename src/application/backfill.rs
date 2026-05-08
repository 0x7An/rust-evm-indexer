use anyhow::{Context, Result, bail};
use uuid::Uuid;

use crate::{
    domain::job::JobType,
    infra::postgres::{
        job_repository::{EnqueueResult, NewJob},
        models::SourceRow,
        repositories::PostgresRepositories,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillRange {
    pub from: u64,
    pub to: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPlan {
    pub source_id: Uuid,
    pub requested_from: u64,
    pub requested_to: u64,
    pub planned_from: Option<u64>,
    pub planned_to: Option<u64>,
    pub range_size: u64,
    pub ranges: Vec<BackfillRange>,
    pub inserted_jobs: usize,
    pub existing_jobs: usize,
}

impl BackfillPlan {
    pub fn total_jobs(&self) -> usize {
        self.inserted_jobs + self.existing_jobs
    }
}

pub fn plan_backfill_jobs(
    repositories: &PostgresRepositories,
    source: &SourceRow,
    from: u64,
    to: u64,
    range_size: u64,
    max_attempts: i32,
) -> Result<BackfillPlan> {
    if max_attempts <= 0 {
        bail!("max-attempts must be greater than zero");
    }
    if range_size == 0 {
        bail!("range-size must be greater than zero");
    }
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }
    ensure_block_fits_i64(from)?;
    ensure_block_fits_i64(to)?;
    let requested_from = from;
    let requested_to = to;

    let checkpoint = repositories
        .ledger()
        .checkpoint_for_source(source.id)
        .context("load source checkpoint")?;
    let from = match checkpoint {
        Some(checkpoint) if checkpoint.processed_block >= 0 => {
            from.max((checkpoint.processed_block as u64).saturating_add(1))
        }
        _ => from,
    };

    let ranges = if from > to {
        Vec::new()
    } else {
        split_inclusive_range(from, to, range_size)?
    };

    let mut inserted_jobs = 0;
    let mut existing_jobs = 0;

    for range in &ranges {
        let idempotency_key = format!("ingest:{}:{}:{}", source.id, range.from, range.to);
        let result = repositories
            .jobs()
            .enqueue(
                NewJob::new(JobType::IngestRange, source.chain_id, idempotency_key)
                    .with_source(source.id)
                    .with_range(block_to_i64(range.from)?, block_to_i64(range.to)?)
                    .with_max_attempts(max_attempts),
            )
            .context("enqueue backfill ingest job")?;

        match result {
            EnqueueResult::Inserted(_) => inserted_jobs += 1,
            EnqueueResult::Existing(_) => existing_jobs += 1,
        }
    }

    Ok(BackfillPlan {
        source_id: source.id,
        requested_from,
        requested_to,
        planned_from: ranges.first().map(|range| range.from),
        planned_to: ranges.last().map(|range| range.to),
        range_size,
        ranges,
        inserted_jobs,
        existing_jobs,
    })
}

pub fn split_inclusive_range(from: u64, to: u64, range_size: u64) -> Result<Vec<BackfillRange>> {
    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }
    if range_size == 0 {
        bail!("range-size must be greater than zero");
    }

    let mut ranges = Vec::new();
    let mut range_from = from;
    while range_from <= to {
        let range_to = range_from.saturating_add(range_size - 1).min(to);
        ranges.push(BackfillRange {
            from: range_from,
            to: range_to,
        });

        if range_to == u64::MAX {
            break;
        }
        range_from = range_to + 1;
    }

    Ok(ranges)
}

fn block_to_i64(block: u64) -> Result<i64> {
    ensure_block_fits_i64(block)?;
    Ok(block as i64)
}

fn ensure_block_fits_i64(block: u64) -> Result<()> {
    if block > i64::MAX as u64 {
        bail!("block {block} is too large for postgres bigint storage");
    }
    Ok(())
}
