use super::connection::PgPool;
use super::job_repository::{EnqueueResult, JobRepository, NewJob};
use super::ledger_repository::LedgerRepository;
use crate::application::ports::{
    BackfillRepository, EnqueueRangeJobResult, NewRangeJob, SourceCheckpoint,
};

#[derive(Clone)]
pub struct PostgresRepositories {
    pool: PgPool,
    jobs: JobRepository,
    ledger: LedgerRepository,
}

impl PostgresRepositories {
    pub fn new(pool: PgPool) -> Self {
        Self {
            jobs: JobRepository::new(pool.clone()),
            ledger: LedgerRepository::new(pool.clone()),
            pool,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn jobs(&self) -> &JobRepository {
        &self.jobs
    }

    pub fn ledger(&self) -> &LedgerRepository {
        &self.ledger
    }
}

impl BackfillRepository for PostgresRepositories {
    fn checkpoint_for_source(
        &self,
        source_id: uuid::Uuid,
    ) -> anyhow::Result<Option<SourceCheckpoint>> {
        self.ledger
            .checkpoint_for_source(source_id)
            .map(|checkpoint| {
                checkpoint.map(|checkpoint| SourceCheckpoint {
                    processed_block: checkpoint.processed_block,
                    processed_block_hash: checkpoint.processed_block_hash,
                    finalized_block: checkpoint.finalized_block,
                })
            })
    }

    fn enqueue_range_job(&self, job: NewRangeJob) -> anyhow::Result<EnqueueRangeJobResult> {
        let result = self.jobs.enqueue(
            NewJob::new(job.job_type, job.chain_id, job.idempotency_key)
                .with_source(job.source_id)
                .with_range(job.from_block, job.to_block)
                .with_max_attempts(job.max_attempts),
        )?;

        Ok(match result {
            EnqueueResult::Inserted(_) => EnqueueRangeJobResult::Inserted,
            EnqueueResult::Existing(_) => EnqueueRangeJobResult::Existing,
        })
    }
}
