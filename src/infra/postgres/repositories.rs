use super::connection::PgPool;

#[derive(Clone)]
pub struct PostgresRepositories {
    pool: PgPool,
}

impl PostgresRepositories {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
