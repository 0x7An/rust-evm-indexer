use diesel::{
    PgConnection,
    r2d2::{ConnectionManager, Pool, PoolError},
};

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

pub fn build_pool(database_url: &str) -> Result<PgPool, PoolError> {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder().build(manager)
}
