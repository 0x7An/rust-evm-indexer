use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use axum::{Json, Router, extract::State, routing::post};
use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection, RunQueryDsl, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    application::reorg::verify_source_reorgs,
    infra::{
        evm::{
            decoder::{DecodedLedgerEntry, DecodedLog, RpcLog, TokenStandard},
            rpc::EvmRpcClient,
        },
        postgres::{
            connection::{PgPool, build_pool},
            ledger_repository::LedgerRepository,
            repositories::PostgresRepositories,
            schema::{
                chains, events, ledger_entries, reorg_events, sources, token_balances,
                transaction_receipts,
            },
        },
    },
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
    let guard = TEST_LOCK.lock().expect("reorg test lock poisoned");
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

#[tokio::test]
async fn records_mismatched_indexed_and_checkpoint_hashes() {
    let ctx = setup();
    let source = seed_source_with_indexed_block(&ctx);
    ctx.repositories
        .ledger()
        .advance_checkpoint(source.id, 101, &block_hash("22"), 110)
        .expect("insert checkpoint");
    let rpc_url = start_fake_rpc(HashMap::from([
        (100, block_hash("aa")),
        (101, block_hash("bb")),
    ]))
    .await;

    let verification = verify_source_reorgs(
        &EvmRpcClient::new(rpc_url),
        ctx.repositories.ledger(),
        &source,
        100,
        101,
    )
    .await
    .expect("verify reorgs");

    assert_eq!(verification.checked_blocks, 2);
    assert_eq!(verification.mismatches.len(), 2);
    assert!(
        verification
            .mismatches
            .iter()
            .any(|mismatch| mismatch.block_number == 100
                && mismatch.expected_block_hash == block_hash("11")
                && mismatch.actual_block_hash == block_hash("aa"))
    );
    assert!(
        verification
            .mismatches
            .iter()
            .any(|mismatch| mismatch.block_number == 101
                && mismatch.expected_block_hash == block_hash("22")
                && mismatch.actual_block_hash == block_hash("bb"))
    );

    let rows = ctx
        .repositories
        .ledger()
        .reorg_events_for_source(source.id)
        .expect("load reorg events");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.chain_id == ctx.chain_id));
    assert!(rows.iter().all(|row| row.from_block == row.to_block));
}

#[tokio::test]
async fn matching_block_hashes_do_not_record_reorg_events() {
    let ctx = setup();
    let source = seed_source_with_indexed_block(&ctx);
    ctx.repositories
        .ledger()
        .advance_checkpoint(source.id, 101, &block_hash("22"), 110)
        .expect("insert checkpoint");
    let rpc_url = start_fake_rpc(HashMap::from([
        (100, block_hash("11")),
        (101, block_hash("22")),
    ]))
    .await;

    let verification = verify_source_reorgs(
        &EvmRpcClient::new(rpc_url),
        ctx.repositories.ledger(),
        &source,
        100,
        101,
    )
    .await
    .expect("verify reorgs");

    assert_eq!(verification.checked_blocks, 2);
    assert!(verification.mismatches.is_empty());
    assert!(
        ctx.repositories
            .ledger()
            .reorg_events_for_source(source.id)
            .expect("load reorg events")
            .is_empty()
    );
}

fn seed_source_with_indexed_block(
    ctx: &TestContext,
) -> indexer_rs::infra::postgres::models::SourceRow {
    ctx.repositories
        .ledger()
        .ensure_chain(
            &format!("reorg-test-chain-{}", ctx.chain_id),
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
            "reorg-test-source",
            &ctx.contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("ensure source");

    LedgerRepository::new(ctx.pool.clone())
        .persist_decoded_logs(&source, &[transfer_log(&ctx.contract, 100, "01", "11")])
        .expect("persist decoded logs");

    source
}

async fn start_fake_rpc(block_hashes: HashMap<u64, String>) -> String {
    let state = FakeRpcState {
        block_hashes: Arc::new(block_hashes),
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
    block_hashes: Arc<HashMap<u64, String>>,
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
        "eth_getBlockByNumber" => {
            let block = requested_block(&request.params);
            json!({
                "hash": state
                    .block_hashes
                    .get(&block)
                    .cloned()
                    .unwrap_or_else(|| block_hash("ff")),
                "timestamp": "0x5fee6600",
            })
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

fn transfer_log(
    contract: &str,
    block_number: u64,
    tx_byte: &str,
    block_hash_byte: &str,
) -> (RpcLog, DecodedLog) {
    (
        RpcLog {
            address: contract.to_string(),
            topics: Vec::new(),
            data: "0x".to_string(),
            block_number: format!("0x{block_number:x}"),
            transaction_hash: format!("0x{}", tx_byte.repeat(32)),
            transaction_index: Some("0x1".to_string()),
            log_index: "0x0".to_string(),
            block_hash: block_hash(block_hash_byte),
            block_timestamp: Some(block_timestamp(block_number)),
        },
        DecodedLog {
            event_name: "Transfer".to_string(),
            entries: vec![DecodedLedgerEntry {
                event_name: "Transfer".to_string(),
                token_standard: TokenStandard::Erc721,
                operator: None,
                from: zero_address(),
                to: address("11"),
                token_id: "42".to_string(),
                amount: "1".to_string(),
                batch_index: 0,
            }],
        },
    )
}

fn requested_block(params: &Value) -> u64 {
    params
        .get(0)
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .expect("eth_getBlockByNumber block")
}

fn block_timestamp(block_number: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_609_459_200 + block_number as i64, 0)
        .expect("test block timestamp")
}

fn cleanup_chain(conn: &mut PgConnection, chain_id: i64) {
    diesel::delete(reorg_events::table.filter(reorg_events::chain_id.eq(chain_id)))
        .execute(conn)
        .expect("delete test reorg events");
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
    12_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
}

fn random_contract() -> String {
    let hex = format!("{}00000000", Uuid::new_v4().simple());
    format!("0x{}", &hex[..40])
}

fn address(byte: &str) -> String {
    format!("0x{}", byte.repeat(20))
}

fn zero_address() -> String {
    address("00")
}

fn block_hash(byte: &str) -> String {
    format!("0x{}", byte.repeat(32))
}
