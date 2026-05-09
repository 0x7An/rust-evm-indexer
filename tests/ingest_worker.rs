use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicUsize, Ordering},
};

use axum::{Json, Router, extract::State, routing::post};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Duration, Utc};
use diesel::{Connection, PgConnection, RunQueryDsl, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    domain::job::{JobStatus, JobType},
    infra::{
        evm::{
            decoder::{DecodedLedgerEntry, DecodedLog, RpcLog, TokenStandard, event_topic},
            rpc::EvmRpcClient,
        },
        postgres::{
            connection::{PgPool, build_pool},
            job_repository::{EnqueueResult, NewJob},
            repositories::PostgresRepositories,
            schema::{
                chains, events, jobs, ledger_entries, sources, token_balances, transaction_receipts,
            },
        },
    },
    worker::{IngestWorker, WorkerOutcome, WorkerRunStopReason},
};
use serde::Deserialize;
use serde_json::{Value, json};
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
    let guard = TEST_LOCK.lock().expect("ingest worker test lock poisoned");
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
        cleanup_test_jobs(&mut conn);
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

#[tokio::test]
async fn worker_processes_queued_ingest_job() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    let source = ctx
        .repositories
        .ledger()
        .ensure_chain(
            &format!("worker-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    assert_eq!(source.chain_id, ctx.chain_id);
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");

    let enqueue_result = ctx
        .repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::IngestRange,
                ctx.chain_id,
                format!("it:worker:{}:100:100", source.id),
            )
            .with_source(source.id)
            .with_range(100, 100),
        )
        .expect("enqueue ingest job");
    let job_id = match enqueue_result {
        EnqueueResult::Inserted(job) => job.id,
        EnqueueResult::Existing(job) => job.id,
    };

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let outcome = worker.run_once().await.expect("run worker once");

    let WorkerOutcome::Processed {
        job_id: processed_id,
        summary,
    } = &outcome
    else {
        panic!("worker should process one job, got {outcome:?}");
    };
    assert_eq!(*processed_id, job_id);
    assert_eq!(summary.events_seen, 1);
    assert_eq!(summary.ledger_entries_persisted, 1);
    assert_eq!(summary.holder_count, 1);

    let job = ctx.repositories.jobs().get(job_id).expect("load job");
    assert_eq!(job.status, JobStatus::Succeeded.to_string());
    assert!(job.leased_by.is_none());
    assert!(job.lease_expires_at.is_none());

    let attempts = ctx
        .repositories
        .jobs()
        .attempts_for_job(job_id)
        .expect("load attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].worker_id, "worker-it");
    assert_eq!(attempts[0].status, JobStatus::Succeeded.to_string());

    let contract_summary = ctx
        .repositories
        .ledger()
        .contract_summary(ctx.chain_id, &ctx.contract)
        .expect("load summary")
        .expect("summary exists");
    assert_eq!(contract_summary.event_count, 1);
    assert_eq!(contract_summary.ledger_entry_count, 1);
    assert_eq!(contract_summary.holder_count, 1);
    assert_eq!(contract_summary.checkpoint_processed_block, Some(100));
    assert_eq!(contract_summary.checkpoint_finalized_block, Some(108));

    let mut conn = ctx.pool.get().expect("get postgres connection");
    let (block_timestamp, transaction_index, topics, data) = events::table
        .filter(events::source_id.eq(source.id))
        .select((
            events::block_timestamp,
            events::transaction_index,
            events::topics,
            events::data,
        ))
        .first::<(Option<DateTime<Utc>>, Option<i32>, Value, String)>(&mut conn)
        .expect("load persisted event metadata");
    assert_eq!(block_timestamp, Some(block_timestamp_for(100)));
    assert_eq!(transaction_index, Some(1));
    assert_eq!(topics.as_array().expect("topics").len(), 4);
    assert_eq!(data, "0x");

    let checkpoint = ctx
        .repositories
        .ledger()
        .checkpoint_for_source(source.id)
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.processed_block, 100);
    assert_eq!(
        checkpoint.processed_block_hash,
        format!("0x{}", "ef".repeat(32))
    );
    assert_eq!(checkpoint.finalized_block, 108);
}

#[tokio::test]
async fn worker_persists_transaction_receipts_when_enabled() {
    let ctx = setup();
    let (rpc_url, receipt_requests) = start_fake_rpc_with_receipts(ctx.contract.clone()).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-receipt-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-receipt-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");

    ctx.repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::IngestRange,
                ctx.chain_id,
                format!("it:worker-receipt:{}:100:100", source.id),
            )
            .with_source(source.id)
            .with_range(100, 100),
        )
        .expect("enqueue ingest job");

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-receipt-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id)
    .with_transaction_receipts(true);
    let outcome = worker.run_once().await.expect("run worker once");

    let WorkerOutcome::Processed { summary, .. } = outcome else {
        panic!("worker should process receipt job, got {outcome:?}");
    };
    assert_eq!(summary.transaction_receipts_persisted, 1);
    assert_eq!(receipt_requests.load(Ordering::SeqCst), 1);

    let mut conn = ctx.pool.get().expect("get postgres connection");
    let (from_address, to_address, status, gas_used, effective_gas_price) =
        transaction_receipts::table
            .filter(transaction_receipts::chain_id.eq(ctx.chain_id))
            .select((
                transaction_receipts::from_address,
                transaction_receipts::to_address,
                transaction_receipts::status,
                transaction_receipts::gas_used,
                transaction_receipts::effective_gas_price,
            ))
            .first::<(
                String,
                Option<String>,
                Option<i32>,
                BigDecimal,
                Option<BigDecimal>,
            )>(&mut conn)
            .expect("load transaction receipt");
    assert_eq!(from_address, address("aa"));
    assert_eq!(to_address, Some(ctx.contract.clone()));
    assert_eq!(status, Some(1));
    assert_eq!(gas_used.to_string(), "21000");
    assert_eq!(
        effective_gas_price
            .expect("effective gas price")
            .to_string(),
        "1000000000"
    );
}

#[tokio::test]
async fn worker_returns_no_job_when_queue_is_empty() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);

    let outcome = worker.run_once().await.expect("run worker once");
    assert!(matches!(outcome, WorkerOutcome::NoJob));
}

#[tokio::test]
async fn worker_runs_until_queue_is_idle() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-loop-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-loop-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");

    for block in [100, 101] {
        ctx.repositories
            .jobs()
            .enqueue(
                NewJob::new(
                    JobType::IngestRange,
                    ctx.chain_id,
                    format!("it:worker-loop:{}:{block}:{block}", source.id),
                )
                .with_source(source.id)
                .with_range(block, block),
            )
            .expect("enqueue ingest job");
    }

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-loop-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let summary = worker
        .run_until_idle(None)
        .await
        .expect("run worker until idle");

    assert_eq!(summary.processed_jobs, 2);
    assert_eq!(summary.failed_jobs, 0);
    assert_eq!(summary.stop_reason, WorkerRunStopReason::Idle);

    let jobs = ctx
        .repositories
        .jobs()
        .jobs_for_source(source.id)
        .expect("load source jobs");
    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.status == JobStatus::Succeeded.to_string())
    );

    let checkpoint = ctx
        .repositories
        .ledger()
        .checkpoint_for_source(source.id)
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.processed_block, 101);
}

#[tokio::test]
async fn worker_prioritizes_replay_jobs_over_ingest_jobs() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-priority-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-priority-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");
    let ingest_job = ctx
        .repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::IngestRange,
                ctx.chain_id,
                format!("it:worker-priority-ingest:{}:100:100", source.id),
            )
            .with_source(source.id)
            .with_range(100, 100),
        )
        .expect("enqueue ingest job");
    let replay_job = ctx
        .repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::ReplayRange,
                ctx.chain_id,
                format!("it:worker-priority-replay:{}:101:101", source.id),
            )
            .with_source(source.id)
            .with_range(101, 101),
        )
        .expect("enqueue replay job");
    let ingest_job_id = enqueued_job_id(ingest_job);
    let replay_job_id = enqueued_job_id(replay_job);

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-priority-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let outcome = worker.run_once().await.expect("run worker once");

    let WorkerOutcome::Processed { job_id, .. } = outcome else {
        panic!("worker should process replay job, got {outcome:?}");
    };
    assert_eq!(job_id, replay_job_id);

    let jobs = ctx
        .repositories
        .jobs()
        .jobs_for_source(source.id)
        .expect("load jobs");
    let ingest_status = jobs
        .iter()
        .find(|job| job.id == ingest_job_id)
        .map(|job| job.status.as_str())
        .expect("ingest job exists");
    let replay_status = jobs
        .iter()
        .find(|job| job.id == replay_job_id)
        .map(|job| job.status.as_str())
        .expect("replay job exists");
    assert_eq!(ingest_status, JobStatus::Queued.as_str());
    assert_eq!(replay_status, JobStatus::Succeeded.as_str());
}

#[tokio::test]
async fn worker_does_not_let_failing_replay_starve_ingest_forever() {
    let ctx = setup();
    let rpc_url = start_fake_rpc_with_missing_code(ctx.contract.clone(), 100).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-replay-fairness-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-replay-fairness-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");
    let replay_job_id = enqueued_job_id(
        ctx.repositories
            .jobs()
            .enqueue(
                NewJob::new(
                    JobType::ReplayRange,
                    ctx.chain_id,
                    format!("it:worker-replay-fairness-replay:{}:100:100", source.id),
                )
                .with_source(source.id)
                .with_range(100, 100),
            )
            .expect("enqueue replay job"),
    );
    let ingest_job_id = enqueued_job_id(
        ctx.repositories
            .jobs()
            .enqueue(
                NewJob::new(
                    JobType::IngestRange,
                    ctx.chain_id,
                    format!("it:worker-replay-fairness-ingest:{}:101:101", source.id),
                )
                .with_source(source.id)
                .with_range(101, 101),
            )
            .expect("enqueue ingest job"),
    );

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-replay-fairness-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let first = worker
        .run_once_until_shutdown(std::future::pending())
        .await
        .expect("run worker once");
    let WorkerOutcome::Failed { .. } = first else {
        panic!("first production worker pass should fail replay, got {first:?}");
    };

    let second = worker
        .run_once_until_shutdown(std::future::pending())
        .await
        .expect("run worker again");
    let WorkerOutcome::Processed { job_id, .. } = second else {
        panic!("second production worker pass should process ingest, got {second:?}");
    };
    assert_eq!(job_id, ingest_job_id);

    let jobs = ctx
        .repositories
        .jobs()
        .jobs_for_source(source.id)
        .expect("load jobs");
    let replay = jobs
        .iter()
        .find(|job| job.id == replay_job_id)
        .expect("replay job exists");
    let ingest = jobs
        .iter()
        .find(|job| job.id == ingest_job_id)
        .expect("ingest job exists");
    assert_eq!(replay.status, JobStatus::Queued.as_str());
    assert_eq!(replay.attempts, 1);
    assert_eq!(ingest.status, JobStatus::Succeeded.as_str());
}

#[tokio::test]
async fn worker_replays_range_by_orphaning_existing_rows_and_ingesting_canonical_logs() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-replay-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-replay-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");
    ctx.repositories
        .ledger()
        .persist_decoded_logs(&source, &[decoded_transfer_log(&ctx.contract, 100)])
        .expect("persist old decoded log");

    ctx.repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::ReplayRange,
                ctx.chain_id,
                format!("it:worker-replay:{}:100:100", source.id),
            )
            .with_source(source.id)
            .with_range(100, 100),
        )
        .expect("enqueue replay job");

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-replay-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let outcome = worker.run_once().await.expect("run replay worker once");

    let WorkerOutcome::Processed { summary, .. } = outcome else {
        panic!("worker should process replay job, got {outcome:?}");
    };
    assert_eq!(summary.events_seen, 1);
    assert_eq!(summary.ledger_entries_persisted, 1);
    assert_eq!(summary.holder_count, 1);

    let mut conn = ctx.pool.get().expect("get postgres connection");
    let active_events = events::table
        .filter(events::source_id.eq(source.id))
        .filter(events::orphaned.eq(false))
        .count()
        .get_result::<i64>(&mut conn)
        .expect("count active events");
    let orphaned_events = events::table
        .filter(events::source_id.eq(source.id))
        .filter(events::orphaned.eq(true))
        .count()
        .get_result::<i64>(&mut conn)
        .expect("count orphaned events");
    assert_eq!(active_events, 1);
    assert_eq!(orphaned_events, 1);

    let old_row_orphaned = ledger_entries::table
        .filter(ledger_entries::source_id.eq(source.id))
        .filter(ledger_entries::transaction_hash.eq(format!("0x{}", "aa".repeat(32))))
        .select(ledger_entries::orphaned)
        .first::<bool>(&mut conn)
        .expect("load old ledger row");
    assert!(old_row_orphaned);

    let holders = ctx
        .repositories
        .ledger()
        .holders(ctx.chain_id, &ctx.contract, 10)
        .expect("load holders")
        .expect("holders exist");
    assert_eq!(holders.len(), 1);
    assert_eq!(holders[0].holder_address, address("11"));
    assert_eq!(holders[0].token_id, "42");
    assert_eq!(holders[0].balance, "1");
}

#[tokio::test]
async fn worker_replay_reactivates_identical_canonical_log() {
    let ctx = setup();
    let rpc_url = start_fake_rpc(ctx.contract.clone()).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-replay-same-log-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-replay-same-log-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");
    let canonical_tx_hash = format!("0x{:064x}", 100);
    ctx.repositories
        .ledger()
        .persist_decoded_logs(
            &source,
            &[decoded_transfer_log_with_tx_and_holder(
                &ctx.contract,
                100,
                &canonical_tx_hash,
                "11",
            )],
        )
        .expect("persist canonical decoded log");

    ctx.repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::ReplayRange,
                ctx.chain_id,
                format!("it:worker-replay-same-log:{}:100:100", source.id),
            )
            .with_source(source.id)
            .with_range(100, 100),
        )
        .expect("enqueue replay job");

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-replay-same-log-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let outcome = worker.run_once().await.expect("run replay worker once");

    let WorkerOutcome::Processed { summary, .. } = outcome else {
        panic!("worker should process replay job, got {outcome:?}");
    };
    assert_eq!(summary.events_seen, 1);
    assert_eq!(summary.holder_count, 1);

    let mut conn = ctx.pool.get().expect("get postgres connection");
    let active_ledger_entries = ledger_entries::table
        .filter(ledger_entries::source_id.eq(source.id))
        .filter(ledger_entries::orphaned.eq(false))
        .count()
        .get_result::<i64>(&mut conn)
        .expect("count active ledger entries");
    let orphaned_ledger_entries = ledger_entries::table
        .filter(ledger_entries::source_id.eq(source.id))
        .filter(ledger_entries::orphaned.eq(true))
        .count()
        .get_result::<i64>(&mut conn)
        .expect("count orphaned ledger entries");
    assert_eq!(active_ledger_entries, 1);
    assert_eq!(orphaned_ledger_entries, 0);

    let event_orphaned = events::table
        .filter(events::source_id.eq(source.id))
        .filter(events::transaction_hash.eq(canonical_tx_hash))
        .select(events::orphaned)
        .first::<bool>(&mut conn)
        .expect("load replayed event");
    assert!(!event_orphaned);

    let holders = ctx
        .repositories
        .ledger()
        .holders(ctx.chain_id, &ctx.contract, 10)
        .expect("load holders")
        .expect("holders exist");
    assert_eq!(holders.len(), 1);
    assert_eq!(holders[0].holder_address, address("11"));
    assert_eq!(holders[0].balance, "1");
}

#[tokio::test]
async fn worker_rejects_range_when_contract_code_is_missing_at_from_boundary() {
    let ctx = setup();
    let rpc_url = start_fake_rpc_with_missing_code(ctx.contract.clone(), 100).await;
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("worker-code-boundary-test-chain-{}", ctx.chain_id),
            ctx.chain_id,
            "<test>",
            12,
        )
        .expect("ensure chain");
    let source = ctx
        .repositories
        .ledger()
        .ensure_source(
            ctx.chain_id,
            "worker-code-boundary-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");
    ctx.repositories
        .jobs()
        .enqueue(
            NewJob::new(
                JobType::IngestRange,
                ctx.chain_id,
                format!("it:worker-code-boundary:{}:100:101", source.id),
            )
            .with_source(source.id)
            .with_range(100, 101),
        )
        .expect("enqueue ingest job");

    let worker = IngestWorker::new(
        ctx.repositories.clone(),
        EvmRpcClient::new(rpc_url),
        "worker-code-boundary-it",
        Duration::seconds(60),
        10,
    )
    .with_chain_id(ctx.chain_id);
    let outcome = worker.run_once().await.expect("run worker once");

    let WorkerOutcome::Failed { error, .. } = outcome else {
        panic!("worker should reject missing boundary code, got {outcome:?}");
    };
    assert!(error.contains("boundary block 100"));
    assert!(error.contains(&ctx.contract));
}

async fn start_fake_rpc(contract: String) -> String {
    let (url, _) = start_fake_rpc_inner(contract, false).await;
    url
}

async fn start_fake_rpc_with_missing_code(contract: String, missing_code_block: u64) -> String {
    let (url, _) =
        start_fake_rpc_inner_with_options(contract, false, Some(missing_code_block)).await;
    url
}

async fn start_fake_rpc_with_receipts(contract: String) -> (String, Arc<AtomicUsize>) {
    start_fake_rpc_inner(contract, true).await
}

async fn start_fake_rpc_inner(
    contract: String,
    support_receipts: bool,
) -> (String, Arc<AtomicUsize>) {
    start_fake_rpc_inner_with_options(contract, support_receipts, None).await
}

async fn start_fake_rpc_inner_with_options(
    contract: String,
    support_receipts: bool,
    missing_code_block: Option<u64>,
) -> (String, Arc<AtomicUsize>) {
    let receipt_requests = Arc::new(AtomicUsize::new(0));
    let state = FakeRpcState {
        contract: Arc::new(contract),
        support_receipts,
        receipt_requests: Arc::clone(&receipt_requests),
        missing_code_block,
    };
    let app = Router::new()
        .route("/", post(fake_rpc_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake rpc");
    let addr = listener.local_addr().expect("fake rpc local addr");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve fake rpc");
    });

    (format!("http://{addr}"), receipt_requests)
}

#[derive(Clone)]
struct FakeRpcState {
    contract: Arc<String>,
    support_receipts: bool,
    receipt_requests: Arc<AtomicUsize>,
    missing_code_block: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    id: Value,
    method: String,
    params: Value,
}

async fn fake_rpc_handler(
    State(state): State<FakeRpcState>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<Value> {
    let result = match request.method.as_str() {
        "eth_blockNumber" => json!("0x78"),
        "eth_getCode" => {
            if state.missing_code_block == Some(requested_block_param(&request.params, 1)) {
                json!("0x")
            } else {
                json!("0x6000")
            }
        }
        "eth_getBlockByNumber" => json!({
            "hash": format!("0x{}", "ef".repeat(32)),
            "timestamp": "0x5fee6600",
        }),
        "eth_getLogs" => json!([erc721_transfer_log(
            &state.contract,
            requested_from_block(&request.params)
        )]),
        "eth_getTransactionReceipt" if state.support_receipts => {
            state.receipt_requests.fetch_add(1, Ordering::SeqCst);
            json!(transaction_receipt(
                &state.contract,
                requested_transaction_hash(&request.params)
            ))
        }
        _ => {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {
                    "code": -32601,
                    "message": format!("unsupported method {}", request.method),
                }
            }));
        }
    };

    Json(json!({
        "jsonrpc": "2.0",
        "id": request.id,
        "result": result,
    }))
}

fn transaction_receipt(contract: &str, transaction_hash: &str) -> Value {
    json!({
        "transactionHash": transaction_hash,
        "transactionIndex": "0x1",
        "blockHash": format!("0x{}", "ef".repeat(32)),
        "blockNumber": "0x64",
        "from": address("aa"),
        "to": contract,
        "contractAddress": null,
        "status": "0x1",
        "gasUsed": "0x5208",
        "cumulativeGasUsed": "0x5208",
        "effectiveGasPrice": "0x3b9aca00",
        "type": "0x2",
    })
}

fn erc721_transfer_log(contract: &str, block_number: u64) -> Value {
    json!({
        "address": contract,
        "topics": [
            event_topic("Transfer(address,address,uint256)"),
            topic_address_word("00"),
            topic_address_word("11"),
            format!("0x{:064x}", 42),
        ],
        "data": "0x",
        "blockNumber": format!("0x{block_number:x}"),
        "transactionHash": format!("0x{block_number:064x}"),
        "transactionIndex": "0x1",
        "logIndex": "0x0",
        "blockHash": format!("0x{:064x}", block_number + 1),
    })
}

fn decoded_transfer_log(contract: &str, block_number: u64) -> (RpcLog, DecodedLog) {
    decoded_transfer_log_with_tx_and_holder(
        contract,
        block_number,
        &format!("0x{}", "aa".repeat(32)),
        "22",
    )
}

fn decoded_transfer_log_with_tx_and_holder(
    contract: &str,
    block_number: u64,
    transaction_hash: &str,
    holder_byte: &str,
) -> (RpcLog, DecodedLog) {
    (
        RpcLog {
            address: contract.to_string(),
            topics: Vec::new(),
            data: "0x".to_string(),
            block_number: format!("0x{block_number:x}"),
            transaction_hash: transaction_hash.to_string(),
            transaction_index: Some("0x0".to_string()),
            log_index: "0x0".to_string(),
            block_hash: format!("0x{}", "bb".repeat(32)),
            block_timestamp: Some(block_timestamp_for(block_number)),
        },
        DecodedLog {
            event_name: "Transfer".to_string(),
            entries: vec![DecodedLedgerEntry {
                event_name: "Transfer".to_string(),
                token_standard: TokenStandard::Erc721,
                operator: None,
                from: address("00"),
                to: address(holder_byte),
                token_id: "42".to_string(),
                amount: "1".to_string(),
                batch_index: 0,
            }],
        },
    )
}

fn block_timestamp_for(_block_number: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_609_459_200, 0).expect("test block timestamp")
}

fn requested_transaction_hash(params: &Value) -> &str {
    params
        .get(0)
        .and_then(Value::as_str)
        .expect("eth_getTransactionReceipt transaction hash")
}

fn requested_from_block(params: &Value) -> u64 {
    params
        .get(0)
        .and_then(|filter| filter.get("fromBlock"))
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .expect("eth_getLogs fromBlock")
}

fn requested_block_param(params: &Value, index: usize) -> u64 {
    params
        .get(index)
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .expect("hex block param")
}

fn topic_address_word(byte: &str) -> String {
    format!("0x{}{}", "00".repeat(12), byte.repeat(20))
}

fn address(byte: &str) -> String {
    format!("0x{}", byte.repeat(20))
}

fn cleanup_chain(conn: &mut PgConnection, chain_id: i64) {
    diesel::delete(transaction_receipts::table.filter(transaction_receipts::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test transaction receipts");
    diesel::delete(token_balances::table.filter(token_balances::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test balances");
    diesel::delete(ledger_entries::table.filter(ledger_entries::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test ledger entries");
    diesel::delete(events::table.filter(events::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test events");
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

fn cleanup_test_jobs(conn: &mut PgConnection) {
    diesel::sql_query(
        "DELETE FROM job_attempts
         USING jobs
         WHERE job_attempts.job_id = jobs.id
           AND jobs.idempotency_key LIKE 'it:%'",
    )
    .execute(conn)
    .expect("delete integration test job attempts");

    diesel::sql_query("DELETE FROM jobs WHERE idempotency_key LIKE 'it:%'")
        .execute(conn)
        .expect("delete integration test jobs");
}

fn enqueued_job_id(result: EnqueueResult) -> Uuid {
    match result {
        EnqueueResult::Inserted(job) | EnqueueResult::Existing(job) => job.id,
    }
}

fn random_chain_id() -> i64 {
    10_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
}

fn random_contract() -> String {
    let hex = format!("{}00000000", Uuid::new_v4().simple());
    format!("0x{}", &hex[..40])
}
