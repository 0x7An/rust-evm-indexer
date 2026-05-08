use std::sync::{Mutex, MutexGuard};

use chrono::Duration;
use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    domain::job::{JobStatus, JobType},
    infra::postgres::{
        connection::build_pool,
        job_repository::{EnqueueResult, JobRepository, NewJob},
    },
};
use uuid::Uuid;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestContext {
    _guard: MutexGuard<'static, ()>,
    repo: JobRepository,
}

fn setup() -> TestContext {
    let guard = TEST_LOCK.lock().expect("job repository test lock poisoned");
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://indexer:indexer@localhost:5432/indexer_rs".to_string());

    let mut migration_conn =
        PgConnection::establish(&database_url).expect("connect to postgres for migrations");
    migration_conn
        .run_pending_migrations(MIGRATIONS)
        .expect("run pending migrations");

    let pool = build_pool(&database_url).expect("build postgres pool");
    let mut conn = pool.get().expect("get postgres connection for cleanup");
    cleanup_test_jobs(&mut conn);

    TestContext {
        _guard: guard,
        repo: JobRepository::new(pool),
    }
}

fn cleanup_test_jobs(conn: &mut PgConnection) {
    diesel::sql_query(
        "DELETE FROM job_attempts
         USING jobs
         WHERE job_attempts.job_id = jobs.id
           AND jobs.idempotency_key LIKE 'it:%'",
    )
    .execute(conn)
    .expect("delete test job attempts");

    diesel::sql_query("DELETE FROM jobs WHERE idempotency_key LIKE 'it:%'")
        .execute(conn)
        .expect("delete test jobs");
}

fn key(name: &str) -> String {
    format!("it:{name}:{}", Uuid::new_v4())
}

#[test]
fn enqueue_is_idempotent_by_key() {
    let ctx = setup();
    let key = key("enqueue");

    let first = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, &key).with_range(10, 20))
        .expect("insert job");
    let second = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, &key).with_range(10, 20))
        .expect("find existing job");

    let EnqueueResult::Inserted(first) = first else {
        panic!("first enqueue should insert");
    };
    let EnqueueResult::Existing(second) = second else {
        panic!("second enqueue should return existing row");
    };

    assert_eq!(first.id, second.id);
    assert_eq!(first.idempotency_key, key);
}

#[test]
fn lease_next_assigns_one_worker_and_records_attempt() {
    let ctx = setup();
    let key = key("lease");
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, &key).with_range(1, 5))
        .expect("insert job");

    let leased = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(60))
        .expect("lease query")
        .expect("leased job");

    assert_eq!(leased.status, JobStatus::Leased.to_string());
    assert_eq!(leased.leased_by.as_deref(), Some("worker-a"));
    assert_eq!(leased.attempts, 1);
    assert!(leased.lease_expires_at.is_some());

    let attempts = ctx.repo.attempts_for_job(leased.id).expect("load attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_number, 1);
    assert_eq!(attempts[0].worker_id, "worker-a");
    assert_eq!(attempts[0].status, JobStatus::Leased.to_string());
}

#[test]
fn non_expired_lease_is_not_claimed_twice() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, key("single-owner")).with_range(1, 5))
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(60))
        .expect("lease query")
        .expect("first worker leases job");
    let second = ctx
        .repo
        .lease_next("worker-b", Duration::seconds(60))
        .expect("lease query");

    assert_eq!(first.leased_by.as_deref(), Some("worker-a"));
    assert!(second.is_none());
}

#[test]
fn lease_next_for_type_skips_other_job_types() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::BackfillRange, 84532, key("backfill")).with_range(1, 5))
        .expect("insert backfill job");
    let expected = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, key("ingest")).with_range(10, 20))
        .expect("insert ingest job");
    let expected_id = match expected {
        EnqueueResult::Inserted(job) | EnqueueResult::Existing(job) => job.id,
    };

    let leased = ctx
        .repo
        .lease_next_for_type("worker-a", Duration::seconds(60), JobType::IngestRange)
        .expect("lease query")
        .expect("leased ingest job");

    assert_eq!(leased.id, expected_id);
    assert_eq!(leased.job_type, JobType::IngestRange.to_string());
}

#[test]
fn expired_lease_can_be_reclaimed() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, key("expired")).with_range(1, 5))
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(-1))
        .expect("lease query")
        .expect("first worker leases job");
    let second = ctx
        .repo
        .lease_next("worker-b", Duration::seconds(60))
        .expect("lease query")
        .expect("second worker reclaims expired job");

    assert_eq!(first.id, second.id);
    assert_eq!(second.leased_by.as_deref(), Some("worker-b"));
    assert_eq!(second.attempts, 2);
}

#[test]
fn failed_job_retries_until_dead_lettered() {
    let ctx = setup();
    ctx.repo
        .enqueue(
            NewJob::new(JobType::IngestRange, 84532, key("retry"))
                .with_range(1, 5)
                .with_max_attempts(2),
        )
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(60))
        .expect("lease query")
        .expect("first lease");
    let queued = ctx
        .repo
        .mark_failed(first.id, "RpcError", "temporary failure")
        .expect("mark first attempt failed");

    assert_eq!(queued.status, JobStatus::Queued.to_string());
    assert_eq!(queued.attempts, 1);

    let second = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(60))
        .expect("lease query")
        .expect("second lease");
    let dead = ctx
        .repo
        .mark_failed(second.id, "RpcError", "still failing")
        .expect("dead letter after max attempts");

    assert_eq!(dead.status, JobStatus::DeadLettered.to_string());
    assert_eq!(dead.attempts, 2);

    let attempts = ctx.repo.attempts_for_job(dead.id).expect("load attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].status, JobStatus::Failed.to_string());
    assert_eq!(attempts[1].status, JobStatus::DeadLettered.to_string());
}

#[test]
fn running_and_succeeded_transitions_update_job_and_attempt() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, 84532, key("success")).with_range(1, 5))
        .expect("insert job");

    let leased = ctx
        .repo
        .lease_next("worker-a", Duration::seconds(60))
        .expect("lease query")
        .expect("leased job");
    let running = ctx.repo.mark_running(leased.id).expect("mark running");
    let succeeded = ctx.repo.mark_succeeded(running.id).expect("mark succeeded");

    assert_eq!(running.status, JobStatus::Running.to_string());
    assert_eq!(succeeded.status, JobStatus::Succeeded.to_string());
    assert!(succeeded.leased_by.is_none());
    assert!(succeeded.lease_expires_at.is_none());

    let attempts = ctx
        .repo
        .attempts_for_job(succeeded.id)
        .expect("load attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, JobStatus::Succeeded.to_string());
    assert!(attempts[0].finished_at.is_some());
}
