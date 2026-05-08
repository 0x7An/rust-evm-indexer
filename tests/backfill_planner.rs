use std::sync::{Mutex, MutexGuard};

use diesel::{Connection, PgConnection, RunQueryDsl, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    application::backfill::{BackfillRange, plan_backfill_jobs},
    domain::job::JobStatus,
    infra::{
        evm::decoder::TokenStandard,
        postgres::{
            connection::{PgPool, build_pool},
            repositories::PostgresRepositories,
            schema::{chains, jobs, sources},
        },
    },
};
use uuid::Uuid;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestContext {
    _guard: MutexGuard<'static, ()>,
    pool: PgPool,
    repositories: PostgresRepositories,
    chain_id: i64,
    contract: String,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if let Ok(mut conn) = self.pool.get() {
            cleanup_chain(&mut conn, self.chain_id);
        }
    }
}

fn setup() -> TestContext {
    let guard = TEST_LOCK
        .lock()
        .expect("backfill planner test lock poisoned");
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://indexer:indexer@localhost:5432/indexer_rs".to_string());

    let mut migration_conn =
        PgConnection::establish(&database_url).expect("connect to postgres for migrations");
    migration_conn
        .run_pending_migrations(MIGRATIONS)
        .expect("run pending migrations");

    let pool = build_pool(&database_url).expect("build postgres pool");
    let chain_id = random_chain_id();
    let contract = random_contract();

    {
        let mut conn = pool.get().expect("get postgres connection for cleanup");
        cleanup_chain(&mut conn, chain_id);
    }

    TestContext {
        _guard: guard,
        repositories: PostgresRepositories::new(pool.clone()),
        pool,
        chain_id,
        contract,
    }
}

#[test]
fn backfill_creates_deterministic_idempotent_jobs() {
    let ctx = setup();
    let source = seed_source(&ctx);

    let plan =
        plan_backfill_jobs(&ctx.repositories, &source, 100, 109, 4, 3).expect("plan backfill jobs");
    assert_eq!(plan.source_id, source.id);
    assert_eq!(plan.requested_from, 100);
    assert_eq!(plan.requested_to, 109);
    assert_eq!(plan.planned_from, Some(100));
    assert_eq!(plan.planned_to, Some(109));
    assert_eq!(plan.ranges, ranges(&[(100, 103), (104, 107), (108, 109)]));
    assert_eq!(plan.inserted_jobs, 3);
    assert_eq!(plan.existing_jobs, 0);
    assert_eq!(plan.total_jobs(), 3);

    let repeated = plan_backfill_jobs(&ctx.repositories, &source, 100, 109, 4, 3)
        .expect("repeat backfill plan");
    assert_eq!(
        repeated.ranges,
        ranges(&[(100, 103), (104, 107), (108, 109)])
    );
    assert_eq!(repeated.inserted_jobs, 0);
    assert_eq!(repeated.existing_jobs, 3);
    assert_eq!(repeated.total_jobs(), 3);

    let jobs = ctx
        .repositories
        .jobs()
        .jobs_for_source(source.id)
        .expect("load planned jobs");
    assert_eq!(jobs.len(), 3);
    assert_eq!(
        jobs.iter()
            .map(|job| (job.from_block.unwrap(), job.to_block.unwrap()))
            .collect::<Vec<_>>(),
        vec![(100, 103), (104, 107), (108, 109)]
    );
    assert!(jobs.iter().all(|job| job.chain_id == ctx.chain_id));
    assert!(jobs.iter().all(|job| job.max_attempts == 3));
    assert!(
        jobs.iter()
            .all(|job| job.status == JobStatus::Queued.to_string())
    );
}

#[test]
fn backfill_resumes_after_checkpoint() {
    let ctx = setup();
    let source = seed_source(&ctx);
    ctx.repositories
        .ledger()
        .advance_checkpoint(source.id, 103, &format!("0x{}", "44".repeat(32)), 109)
        .expect("seed checkpoint");

    let plan = plan_backfill_jobs(&ctx.repositories, &source, 100, 109, 4, 3)
        .expect("plan resumed backfill jobs");
    assert_eq!(plan.requested_from, 100);
    assert_eq!(plan.requested_to, 109);
    assert_eq!(plan.planned_from, Some(104));
    assert_eq!(plan.planned_to, Some(109));
    assert_eq!(plan.ranges, ranges(&[(104, 107), (108, 109)]));
    assert_eq!(plan.inserted_jobs, 2);
    assert_eq!(plan.existing_jobs, 0);
}

#[test]
fn checkpoint_target_waits_for_contiguous_ranges() {
    let ctx = setup();
    let source = seed_source(&ctx);
    plan_backfill_jobs(&ctx.repositories, &source, 100, 299, 100, 3).expect("plan backfill jobs");

    let jobs = ctx
        .repositories
        .jobs()
        .jobs_for_source(source.id)
        .expect("load planned jobs");
    let later_job = jobs
        .iter()
        .find(|job| job.from_block == Some(200) && job.to_block == Some(299))
        .expect("later job exists");
    ctx.repositories
        .jobs()
        .mark_succeeded(later_job.id)
        .expect("mark later range succeeded");

    let target = ctx
        .repositories
        .ledger()
        .next_contiguous_checkpoint_target(&source, Some((200, 299)))
        .expect("compute checkpoint target");
    assert_eq!(target, None);

    let target = ctx
        .repositories
        .ledger()
        .next_contiguous_checkpoint_target(&source, Some((100, 199)))
        .expect("compute catch-up checkpoint target");
    assert_eq!(target, Some(299));
}

fn seed_source(ctx: &TestContext) -> indexer_rs::infra::postgres::models::SourceRow {
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("backfill-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    ctx.repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "backfill-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source")
}

fn ranges(values: &[(u64, u64)]) -> Vec<BackfillRange> {
    values
        .iter()
        .map(|(from, to)| BackfillRange {
            from: *from,
            to: *to,
        })
        .collect()
}

fn cleanup_chain(conn: &mut PgConnection, chain_id: i64) {
    diesel::sql_query(
        "DELETE FROM job_attempts
         USING jobs
         WHERE job_attempts.job_id = jobs.id
           AND jobs.chain_id = $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(chain_id)
    .execute(conn)
    .expect("delete test job attempts");
    diesel::delete(jobs::table.filter(jobs::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test jobs");
    diesel::sql_query(
        "DELETE FROM checkpoints
         USING sources
         WHERE checkpoints.source_id = sources.id
           AND sources.chain_id = $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(chain_id)
    .execute(conn)
    .expect("delete test checkpoints");
    diesel::delete(sources::table.filter(sources::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test sources");
    diesel::delete(chains::table.filter(chains::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test chain");
}

fn random_chain_id() -> i64 {
    11_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
}

fn random_contract() -> String {
    let hex = format!("{}00000000", Uuid::new_v4().simple());
    format!("0x{}", &hex[..40])
}
