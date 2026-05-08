use super::connection::PgPool;
use super::job_repository::JobRepository;
use super::ledger_repository::LedgerRepository;

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
