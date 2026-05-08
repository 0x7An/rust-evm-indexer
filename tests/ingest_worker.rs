use std::sync::{Arc, Mutex, MutexGuard};

use axum::{Json, Router, extract::State, routing::post};
use chrono::Duration;
use diesel::{Connection, PgConnection, RunQueryDsl, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    domain::job::{JobStatus, JobType},
    infra::{
        evm::{
            decoder::{TokenStandard, event_topic},
            rpc::EvmRpcClient,
        },
        postgres::{
            connection::{PgPool, build_pool},
            job_repository::{EnqueueResult, NewJob},
            repositories::PostgresRepositories,
            schema::{chains, events, jobs, ledger_entries, sources, token_balances},
        },
    },
    worker::{IngestWorker, WorkerOutcome},
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
    assert_eq!(contract_summary.checkpoint_finalized_block, Some(100));

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
    assert_eq!(checkpoint.finalized_block, 100);
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

async fn start_fake_rpc(contract: String) -> String {
    let state = FakeRpcState {
        contract: Arc::new(contract),
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

    format!("http://{addr}")
}

#[derive(Clone)]
struct FakeRpcState {
    contract: Arc<String>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    id: Value,
    method: String,
}

async fn fake_rpc_handler(
    State(state): State<FakeRpcState>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<Value> {
    let result = match request.method.as_str() {
        "eth_blockNumber" => json!("0x70"),
        "eth_getCode" => json!("0x6000"),
        "eth_getBlockByNumber" => json!({
            "hash": format!("0x{}", "ef".repeat(32)),
        }),
        "eth_getLogs" => json!([erc721_transfer_log(&state.contract)]),
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

fn erc721_transfer_log(contract: &str) -> Value {
    json!({
        "address": contract,
        "topics": [
            event_topic("Transfer(address,address,uint256)"),
            topic_address_word("00"),
            topic_address_word("11"),
            format!("0x{:064x}", 42),
        ],
        "data": "0x",
        "blockNumber": "0x64",
        "transactionHash": format!("0x{}", "ab".repeat(32)),
        "logIndex": "0x0",
        "blockHash": format!("0x{}", "cd".repeat(32)),
    })
}

fn topic_address_word(byte: &str) -> String {
    format!("0x{}{}", "00".repeat(12), byte.repeat(20))
}

fn cleanup_chain(conn: &mut PgConnection, chain_id: i64) {
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

fn random_chain_id() -> i64 {
    10_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
}

fn random_contract() -> String {
    let hex = format!("{}00000000", Uuid::new_v4().simple());
    format!("0x{}", &hex[..40])
}
