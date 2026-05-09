//! Thin job worker adapter for durable ingestion jobs.

use anyhow::{Context, Result, bail};
use chrono::Duration;
use uuid::Uuid;

use crate::{
    application::ingest::{IngestOptions, ingest_source_range},
    domain::job::{JobStatus, JobType},
    infra::{
        evm::rpc::EvmRpcClient,
        postgres::{
            ledger_repository::ScanSummary, models::JobRow, repositories::PostgresRepositories,
        },
    },
};

#[derive(Clone)]
pub struct IngestWorker {
    repositories: PostgresRepositories,
    rpc: EvmRpcClient,
    worker_id: String,
    lease_for: Duration,
    chunk_size: u64,
    chain_id: Option<i64>,
    include_transaction_receipts: bool,
    progress: bool,
}

impl IngestWorker {
    pub fn new(
        repositories: PostgresRepositories,
        rpc: EvmRpcClient,
        worker_id: impl Into<String>,
        lease_for: Duration,
        chunk_size: u64,
    ) -> Self {
        Self {
            repositories,
            rpc,
            worker_id: worker_id.into(),
            lease_for,
            chunk_size,
            chain_id: None,
            include_transaction_receipts: false,
            progress: false,
        }
    }

    pub fn with_chain_id(mut self, chain_id: i64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    pub fn with_transaction_receipts(mut self, include_transaction_receipts: bool) -> Self {
        self.include_transaction_receipts = include_transaction_receipts;
        self
    }

    pub fn with_progress(mut self, progress: bool) -> Self {
        self.progress = progress;
        self
    }

    pub async fn run_once(&self) -> Result<WorkerOutcome> {
        if self.chunk_size == 0 {
            bail!("chunk-size must be greater than zero");
        }

        let leased = match self.chain_id {
            Some(chain_id) => self.repositories.jobs().lease_next_for_type_and_chain(
                &self.worker_id,
                self.lease_for,
                JobType::IngestRange,
                chain_id,
            ),
            None => self.repositories.jobs().lease_next_for_type(
                &self.worker_id,
                self.lease_for,
                JobType::IngestRange,
            ),
        }
        .context("lease next job")?;

        let Some(leased) = leased else {
            return Ok(WorkerOutcome::NoJob);
        };

        let running = self
            .repositories
            .jobs()
            .mark_running(leased.id)
            .context("mark job running")?;

        match self.execute_job(&running).await {
            Ok(summary) => {
                let succeeded = self
                    .repositories
                    .jobs()
                    .mark_succeeded(running.id)
                    .context("mark job succeeded")?;
                Ok(WorkerOutcome::Processed {
                    job_id: succeeded.id,
                    summary,
                })
            }
            Err(error) => {
                let message = format_error_chain(&error);
                let failed = self
                    .repositories
                    .jobs()
                    .mark_failed(running.id, "IngestJobError", &message)
                    .context("mark job failed")?;
                Ok(WorkerOutcome::Failed {
                    job_id: failed.id,
                    status: failed.status,
                    error: message,
                })
            }
        }
    }

    pub async fn run_until_idle(&self, max_jobs: Option<usize>) -> Result<WorkerRunSummary> {
        let mut summary = WorkerRunSummary::default();

        loop {
            if max_jobs.is_some_and(|max_jobs| summary.attempted_jobs() >= max_jobs) {
                summary.stop_reason = WorkerRunStopReason::MaxJobsReached;
                return Ok(summary);
            }

            match self.run_once().await? {
                WorkerOutcome::NoJob => {
                    summary.stop_reason = WorkerRunStopReason::Idle;
                    return Ok(summary);
                }
                WorkerOutcome::Processed { .. } => {
                    summary.processed_jobs += 1;
                }
                WorkerOutcome::Failed { .. } => {
                    summary.failed_jobs += 1;
                }
            }
        }
    }

    async fn execute_job(&self, job: &JobRow) -> Result<ScanSummary> {
        let job_type = job
            .job_type
            .parse::<JobType>()
            .with_context(|| format!("parse job type {}", job.job_type))?;
        if job_type != JobType::IngestRange {
            bail!("unsupported worker job type {}", job.job_type);
        }

        let source_id = job.source_id.context("ingest job is missing source_id")?;
        let from = job.from_block.context("ingest job is missing from_block")?;
        let to = job.to_block.context("ingest job is missing to_block")?;
        if from < 0 || to < 0 {
            bail!("ingest job range cannot be negative");
        }

        let source = self
            .repositories
            .ledger()
            .source_by_id(source_id)
            .context("load ingest source")?
            .context("ingest source not found")?;
        if source.chain_id != job.chain_id {
            bail!(
                "job chain_id {} does not match source chain_id {}",
                job.chain_id,
                source.chain_id
            );
        }

        let summary = ingest_source_range(
            &self.rpc,
            self.repositories.ledger(),
            &source,
            from as u64,
            to as u64,
            self.chunk_size,
            IngestOptions {
                include_transaction_receipts: self.include_transaction_receipts,
                progress: self.progress,
            },
        )
        .await?;

        let mut completed_range = Some((from, to));
        while let Some(target) = self
            .repositories
            .ledger()
            .next_contiguous_checkpoint_target(&source, completed_range)
            .context("compute checkpoint target")?
        {
            let processed_block_hash =
                self.rpc.block_hash(target as u64).await.with_context(|| {
                    format!("fetch block hash for checkpoint at block {target}")
                })?;
            self.repositories
                .ledger()
                .advance_checkpoint(source.id, target, &processed_block_hash, target)
                .context("advance source checkpoint")?;
            completed_range = None;
        }

        Ok(summary)
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRunSummary {
    pub processed_jobs: usize,
    pub failed_jobs: usize,
    pub stop_reason: WorkerRunStopReason,
}

impl WorkerRunSummary {
    pub fn attempted_jobs(&self) -> usize {
        self.processed_jobs + self.failed_jobs
    }
}

impl Default for WorkerRunSummary {
    fn default() -> Self {
        Self {
            processed_jobs: 0,
            failed_jobs: 0,
            stop_reason: WorkerRunStopReason::Idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRunStopReason {
    Idle,
    MaxJobsReached,
}

#[derive(Debug, Clone)]
pub enum WorkerOutcome {
    NoJob,
    Processed {
        job_id: Uuid,
        summary: ScanSummary,
    },
    Failed {
        job_id: Uuid,
        status: String,
        error: String,
    },
}

impl WorkerOutcome {
    pub fn is_terminal_failure(&self) -> bool {
        matches!(
            self,
            Self::Failed { status, .. } if status == JobStatus::DeadLettered.as_str()
        )
    }
}
