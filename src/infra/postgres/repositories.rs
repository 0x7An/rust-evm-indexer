use super::connection::PgPool;
use super::job_repository::JobRepository;

#[derive(Clone)]
pub struct PostgresRepositories {
    pool: PgPool,
    jobs: JobRepository,
}

impl PostgresRepositories {
    pub fn new(pool: PgPool) -> Self {
        Self {
            jobs: JobRepository::new(pool.clone()),
            pool,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn jobs(&self) -> &JobRepository {
        &self.jobs
    }
}
