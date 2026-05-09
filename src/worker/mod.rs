//! Thin job worker adapter for durable ingestion jobs.

use std::future::Future;

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
        let Some(running) = self.lease_and_mark_running()? else {
            return Ok(WorkerOutcome::NoJob);
        };

        let result = self.execute_job(&running).await;
        self.finish_running_job(&running, result)
    }

    pub async fn run_once_until_shutdown(
        &self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<WorkerOutcome> {
        let Some(running) = self.lease_and_mark_running()? else {
            return Ok(WorkerOutcome::NoJob);
        };

        tokio::pin!(shutdown);
        tokio::select! {
            result = self.execute_job(&running) => self.finish_running_job(&running, result),
            _ = &mut shutdown => {
                let interrupted = self
                    .repositories
                    .jobs()
                    .mark_interrupted_for_retry(
                        running.id,
                        "worker shutdown requested before job completed",
                    )
                    .context("release interrupted job")?;
                Ok(WorkerOutcome::Interrupted {
                    job_id: interrupted.id,
                    status: interrupted.status,
                })
            }
        }
    }

    fn lease_and_mark_running(&self) -> Result<Option<JobRow>> {
        if self.chunk_size == 0 {
            bail!("chunk-size must be greater than zero");
        }

        let leased = match self
            .lease_next_supported_type(JobType::IngestRange)
            .context("lease next ingest job")?
        {
            Some(job) => Some(job),
            None => self
                .lease_next_supported_type(JobType::ReplayRange)
                .context("lease next replay job")?,
        };

        let Some(leased) = leased else {
            return Ok(None);
        };

        let running = self
            .repositories
            .jobs()
            .mark_running(leased.id)
            .context("mark job running")?;

        Ok(Some(running))
    }

    fn lease_next_supported_type(&self, job_type: JobType) -> Result<Option<JobRow>> {
        match self.chain_id {
            Some(chain_id) => self.repositories.jobs().lease_next_for_type_and_chain(
                &self.worker_id,
                self.lease_for,
                job_type,
                chain_id,
            ),
            None => self.repositories.jobs().lease_next_for_type(
                &self.worker_id,
                self.lease_for,
                job_type,
            ),
        }
        .context("lease next supported job")
    }

    fn finish_running_job(
        &self,
        running: &JobRow,
        result: Result<ScanSummary>,
    ) -> Result<WorkerOutcome> {
        match result {
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
                WorkerOutcome::Interrupted { .. } => {
                    summary.interrupted_jobs += 1;
                    summary.stop_reason = WorkerRunStopReason::Interrupted;
                    return Ok(summary);
                }
            }
        }
    }

    async fn execute_job(&self, job: &JobRow) -> Result<ScanSummary> {
        let job_type = job
            .job_type
            .parse::<JobType>()
            .with_context(|| format!("parse job type {}", job.job_type))?;
        if !matches!(job_type, JobType::IngestRange | JobType::ReplayRange) {
            bail!("unsupported worker job type {}", job.job_type);
        }

        let source_id = job.source_id.context("range job is missing source_id")?;
        let from = job.from_block.context("range job is missing from_block")?;
        let to = job.to_block.context("range job is missing to_block")?;
        if from < 0 || to < 0 {
            bail!("range job range cannot be negative");
        }

        let source = self
            .repositories
            .ledger()
            .source_by_id(source_id)
            .context("load job source")?
            .context("job source not found")?;
        if source.chain_id != job.chain_id {
            bail!(
                "job chain_id {} does not match source chain_id {}",
                job.chain_id,
                source.chain_id
            );
        }
        let observed_finalized_block = self.observed_finalized_block(source.chain_id).await?;
        if to > observed_finalized_block {
            bail!(
                "job target block {to} is newer than observed finalized block {observed_finalized_block}"
            );
        }

        if job_type == JobType::ReplayRange {
            let orphaned = self
                .repositories
                .ledger()
                .orphan_source_range(&source, from, to)
                .context("orphan replay range")?;
            if self.progress {
                println!(
                    "Replay orphaned {} events and {} ledger entries for blocks {from}..={to}.",
                    orphaned.events_orphaned, orphaned.ledger_entries_orphaned
                );
            }

            return ingest_source_range(
                &self.rpc,
                self.repositories.ledger(),
                &source,
                from as u64,
                to as u64,
                self.chunk_size,
                IngestOptions {
                    include_transaction_receipts: self.include_transaction_receipts,
                    progress: self.progress,
                    restore_orphaned_conflicts: true,
                },
            )
            .await;
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
                restore_orphaned_conflicts: false,
            },
        )
        .await?;

        if let Some(target) = self
            .repositories
            .ledger()
            .next_contiguous_checkpoint_target(&source, Some((from, to)))
            .context("compute checkpoint target")?
        {
            if target > observed_finalized_block {
                return Ok(summary);
            }

            let processed_block_hash =
                self.rpc.block_hash(target as u64).await.with_context(|| {
                    format!("fetch block hash for checkpoint at block {target}")
                })?;
            self.repositories
                .ledger()
                .advance_checkpoint(
                    source.id,
                    target,
                    &processed_block_hash,
                    observed_finalized_block,
                )
                .context("advance source checkpoint")?;
        }

        Ok(summary)
    }

    async fn observed_finalized_block(&self, chain_id: i64) -> Result<i64> {
        let chain = self
            .repositories
            .ledger()
            .chain_by_chain_id(chain_id)
            .context("load chain for finalized checkpoint")?
            .context("chain configuration not found")?;
        if chain.finality_confirmations < 0 {
            bail!("chain finality_confirmations cannot be negative");
        }

        let head = self.rpc.block_number().await.context("fetch head block")?;
        let finalized = head.saturating_sub(chain.finality_confirmations as u64);
        i64::try_from(finalized).context("finalized block exceeds postgres bigint storage")
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
    pub interrupted_jobs: usize,
    pub stop_reason: WorkerRunStopReason,
}

impl WorkerRunSummary {
    pub fn attempted_jobs(&self) -> usize {
        self.processed_jobs + self.failed_jobs + self.interrupted_jobs
    }
}

impl Default for WorkerRunSummary {
    fn default() -> Self {
        Self {
            processed_jobs: 0,
            failed_jobs: 0,
            interrupted_jobs: 0,
            stop_reason: WorkerRunStopReason::Idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRunStopReason {
    Idle,
    MaxJobsReached,
    Interrupted,
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
    Interrupted {
        job_id: Uuid,
        status: String,
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
