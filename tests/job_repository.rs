use std::sync::{Mutex, MutexGuard};

use chrono::Duration;
use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    domain::job::{JobStatus, JobType},
    infra::postgres::{
        connection::build_pool,
        job_repository::{EnqueueResult, JobRepository, JobStatusCount, NewJob},
    },
};
use uuid::Uuid;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestContext {
    _guard: MutexGuard<'static, ()>,
    repo: JobRepository,
    chain_id: i64,
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
        chain_id: random_chain_id(),
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

fn random_chain_id() -> i64 {
    8_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
}

fn count_for(counts: &[JobStatusCount], status: JobStatus) -> i64 {
    counts
        .iter()
        .find(|row| row.status == status.to_string())
        .map(|row| row.count)
        .unwrap_or(0)
}

#[test]
fn enqueue_is_idempotent_by_key() {
    let ctx = setup();
    let key = key("enqueue");

    let first = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, &key).with_range(10, 20))
        .expect("insert job");
    let second = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, &key).with_range(10, 20))
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
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, &key).with_range(1, 5))
        .expect("insert job");

    let leased = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
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
        .enqueue(
            NewJob::new(JobType::IngestRange, ctx.chain_id, key("single-owner")).with_range(1, 5),
        )
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query")
        .expect("first worker leases job");
    let second = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-b",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query");

    assert_eq!(first.leased_by.as_deref(), Some("worker-a"));
    assert!(second.is_none());
}

#[test]
fn lease_next_for_type_skips_other_job_types() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::ReplayRange, ctx.chain_id, key("replay")).with_range(1, 5))
        .expect("insert replay job");
    let expected = ctx
        .repo
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, key("ingest")).with_range(10, 20))
        .expect("insert ingest job");
    let expected_id = match expected {
        EnqueueResult::Inserted(job) | EnqueueResult::Existing(job) => job.id,
    };

    let leased = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query")
        .expect("leased ingest job");

    assert_eq!(leased.id, expected_id);
    assert_eq!(leased.job_type, JobType::IngestRange.to_string());
}

#[test]
fn status_counts_group_and_filter_jobs() {
    let ctx = setup();
    let chain_id = ctx.chain_id;
    let other_chain_id = ctx.chain_id + 1;

    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, chain_id, key("status-a")).with_range(1, 5))
        .expect("insert first ingest job");
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, chain_id, key("status-b")).with_range(6, 10))
        .expect("insert second ingest job");
    ctx.repo
        .enqueue(NewJob::new(
            JobType::ReplayRange,
            chain_id,
            key("status-replay"),
        ))
        .expect("insert replay job");
    ctx.repo
        .enqueue(NewJob::new(
            JobType::IngestRange,
            other_chain_id,
            key("status-other-chain"),
        ))
        .expect("insert other chain job");

    let leased = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            chain_id,
        )
        .expect("lease query")
        .expect("lease ingest job");
    ctx.repo
        .mark_running(leased.id)
        .expect("mark leased job running");
    ctx.repo
        .mark_succeeded(leased.id)
        .expect("mark leased job succeeded");

    let ingest_counts = ctx
        .repo
        .status_counts(Some(chain_id), None, Some(JobType::IngestRange))
        .expect("load ingest status counts");
    assert_eq!(count_for(&ingest_counts, JobStatus::Queued), 1);
    assert_eq!(count_for(&ingest_counts, JobStatus::Succeeded), 1);
    assert_eq!(ingest_counts.iter().map(|row| row.count).sum::<i64>(), 2);

    let chain_counts = ctx
        .repo
        .status_counts(Some(chain_id), None, None)
        .expect("load chain status counts");
    assert_eq!(count_for(&chain_counts, JobStatus::Queued), 2);
    assert_eq!(count_for(&chain_counts, JobStatus::Succeeded), 1);
    assert_eq!(chain_counts.iter().map(|row| row.count).sum::<i64>(), 3);
}

#[test]
fn expired_lease_can_be_reclaimed() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, key("expired")).with_range(1, 5))
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(-1),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query")
        .expect("first worker leases job");
    let second = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-b",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query")
        .expect("second worker reclaims expired job");

    assert_eq!(first.id, second.id);
    assert_eq!(second.leased_by.as_deref(), Some("worker-b"));
    assert_eq!(second.attempts, 2);

    let attempts = ctx.repo.attempts_for_job(second.id).expect("load attempts");
    assert_eq!(attempts[0].status, JobStatus::Cancelled.to_string());
    assert_eq!(attempts[0].error_class.as_deref(), Some("LeaseExpired"));
}

#[test]
fn failed_job_retries_until_dead_lettered() {
    let ctx = setup();
    ctx.repo
        .enqueue(
            NewJob::new(JobType::IngestRange, ctx.chain_id, key("retry"))
                .with_range(1, 5)
                .with_max_attempts(2),
        )
        .expect("insert job");

    let first = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
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
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
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
fn interrupted_job_is_requeued_and_attempt_is_closed() {
    let ctx = setup();
    ctx.repo
        .enqueue(
            NewJob::new(JobType::IngestRange, ctx.chain_id, key("interrupted")).with_range(1, 5),
        )
        .expect("insert job");

    let leased = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
        .expect("lease query")
        .expect("leased job");
    ctx.repo.mark_running(leased.id).expect("mark running");

    let queued = ctx
        .repo
        .mark_interrupted_for_retry(leased.id, "shutdown requested")
        .expect("mark interrupted");

    assert_eq!(queued.status, JobStatus::Queued.to_string());
    assert!(queued.leased_by.is_none());
    assert!(queued.lease_expires_at.is_none());
    assert_eq!(queued.error_class.as_deref(), Some("WorkerInterrupted"));
    assert_eq!(queued.error_message.as_deref(), Some("shutdown requested"));

    let attempts = ctx.repo.attempts_for_job(leased.id).expect("load attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, JobStatus::Cancelled.to_string());
    assert!(attempts[0].finished_at.is_some());
    assert_eq!(
        attempts[0].error_class.as_deref(),
        Some("WorkerInterrupted")
    );
}

#[test]
fn running_and_succeeded_transitions_update_job_and_attempt() {
    let ctx = setup();
    ctx.repo
        .enqueue(NewJob::new(JobType::IngestRange, ctx.chain_id, key("success")).with_range(1, 5))
        .expect("insert job");

    let leased = ctx
        .repo
        .lease_next_for_type_and_chain(
            "worker-a",
            Duration::seconds(60),
            JobType::IngestRange,
            ctx.chain_id,
        )
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
