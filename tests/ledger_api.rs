use std::sync::{Mutex, MutexGuard};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use indexer_rs::{
    api,
    infra::{
        evm::{
            decoder::{DecodedLedgerEntry, DecodedLog, RpcLog, TokenStandard},
            rpc::RpcTransactionReceipt,
        },
        postgres::{
            connection::{PgPool, build_pool},
            ledger_repository::LedgerRepository,
            repositories::PostgresRepositories,
            schema::{
                chains, events, ledger_entries, sources, token_balances, transaction_receipts,
            },
        },
    },
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestContext {
    _guard: MutexGuard<'static, ()>,
    app: Router,
    pool: PgPool,
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
    let guard = TEST_LOCK.lock().expect("ledger api test lock poisoned");
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

    seed_ledger(pool.clone(), chain_id, &contract);

    TestContext {
        _guard: guard,
        app: api::router(pool.clone()),
        pool,
        chain_id,
        contract,
    }
}

#[tokio::test]
async fn exposes_summary_holders_minters_transfers_and_token_path() {
    let ctx = setup();

    let summary_uri = format!(
        "/chains/{}/contracts/{}/summary",
        ctx.chain_id, ctx.contract
    );
    let (status, summary) = get_json(&ctx.app, &summary_uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(summary["chain_id"], ctx.chain_id);
    assert_eq!(summary["contract_address"], ctx.contract);
    assert_eq!(summary["token_standard"], "erc721");
    assert_eq!(summary["event_count"], 3);
    assert_eq!(summary["ledger_entry_count"], 3);
    assert_eq!(summary["holder_count"], 2);
    assert_eq!(summary["minter_count"], 2);
    assert_eq!(summary["first_indexed_block"], 100);
    assert_eq!(summary["last_indexed_block"], 102);
    assert_eq!(summary["checkpoint_processed_block"], 102);
    assert_eq!(summary["checkpoint_finalized_block"], 110);

    let holders_uri = format!(
        "/chains/{}/contracts/{}/holders?limit=10",
        ctx.chain_id, ctx.contract
    );
    let (status, holders) = get_json(&ctx.app, &holders_uri).await;
    assert_eq!(status, StatusCode::OK);
    let holders = holders["items"].as_array().expect("holders items");
    assert_eq!(holders.len(), 2);
    assert!(holders.iter().any(|item| {
        item["holder_address"] == address("22")
            && item["token_id"] == "42"
            && item["balance"] == "1"
    }));
    assert!(holders.iter().any(|item| {
        item["holder_address"] == address("33") && item["token_id"] == "7" && item["balance"] == "1"
    }));

    let minters_uri = format!(
        "/chains/{}/contracts/{}/minters?limit=10",
        ctx.chain_id, ctx.contract
    );
    let (status, minters) = get_json(&ctx.app, &minters_uri).await;
    assert_eq!(status, StatusCode::OK);
    let minters = minters["items"].as_array().expect("minter items");
    assert_eq!(minters.len(), 2);
    assert!(
        minters
            .iter()
            .any(|item| item["minter_address"] == address("11"))
    );
    assert!(
        minters
            .iter()
            .any(|item| item["minter_address"] == address("33"))
    );

    let transfers_uri = format!(
        "/chains/{}/contracts/{}/transfers?limit=2",
        ctx.chain_id, ctx.contract
    );
    let (status, transfers) = get_json(&ctx.app, &transfers_uri).await;
    assert_eq!(status, StatusCode::OK);
    let transfers = transfers["items"].as_array().expect("transfer items");
    assert_eq!(transfers.len(), 2);
    assert_eq!(transfers[0]["block_number"], 102);
    assert_eq!(transfers[0]["block_timestamp"], "2021-01-01T00:01:42Z");
    assert_eq!(transfers[0]["transaction_index"], 2);
    assert_eq!(transfers[1]["block_number"], 101);

    let path_uri = format!(
        "/chains/{}/contracts/{}/tokens/42/path",
        ctx.chain_id, ctx.contract
    );
    let (status, path) = get_json(&ctx.app, &path_uri).await;
    assert_eq!(status, StatusCode::OK);
    let path = path["items"].as_array().expect("path items");
    assert_eq!(path.len(), 2);
    assert_eq!(path[0]["movement_type"], "mint");
    assert_eq!(path[0]["to_address"], address("11"));
    assert_eq!(path[1]["movement_type"], "transfer");
    assert_eq!(path[1]["from_address"], address("11"));
    assert_eq!(path[1]["to_address"], address("22"));
}

#[tokio::test]
async fn paginates_transfers_without_duplicates() {
    let ctx = setup();

    let first_page_uri = format!(
        "/chains/{}/contracts/{}/transfers?limit=2",
        ctx.chain_id, ctx.contract
    );
    let (status, first_page) = get_json(&ctx.app, &first_page_uri).await;
    assert_eq!(status, StatusCode::OK);
    let first_items = first_page["items"].as_array().expect("first page items");
    assert_eq!(
        first_items
            .iter()
            .map(|item| item["block_number"].as_i64().expect("block number"))
            .collect::<Vec<_>>(),
        vec![102, 101]
    );
    let cursor = first_page["next_cursor"]
        .as_str()
        .expect("first page next cursor");
    assert_eq!(cursor, "101:0:0");

    let second_page_uri = format!(
        "/chains/{}/contracts/{}/transfers?limit=2&cursor={cursor}",
        ctx.chain_id, ctx.contract
    );
    let (status, second_page) = get_json(&ctx.app, &second_page_uri).await;
    assert_eq!(status, StatusCode::OK);
    let second_items = second_page["items"].as_array().expect("second page items");
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["block_number"], 100);
    assert!(second_page["next_cursor"].is_null());

    let first_hashes = first_items
        .iter()
        .map(|item| item["transaction_hash"].as_str().expect("tx hash"))
        .collect::<Vec<_>>();
    let second_hashes = second_items
        .iter()
        .map(|item| item["transaction_hash"].as_str().expect("tx hash"))
        .collect::<Vec<_>>();
    assert!(
        first_hashes
            .iter()
            .all(|hash| !second_hashes.contains(hash))
    );
}

#[tokio::test]
async fn filters_transfers_by_range_holder_token_and_movement() {
    let ctx = setup();

    let uri = format!(
        "/chains/{}/contracts/{}/transfers?from_block=100&to_block=101&holder={}&token_id=42&movement_type=transfer",
        ctx.chain_id,
        ctx.contract,
        address("11")
    );
    let (status, page) = get_json(&ctx.app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let items = page["items"].as_array().expect("filtered transfer items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["block_number"], 101);
    assert_eq!(items[0]["movement_type"], "transfer");
    assert_eq!(items[0]["from_address"], address("11"));
    assert_eq!(items[0]["to_address"], address("22"));
    assert_eq!(items[0]["token_id"], "42");
    assert!(page["next_cursor"].is_null());
}

#[tokio::test]
async fn paginates_token_path_in_chronological_order() {
    let ctx = setup();

    let first_page_uri = format!(
        "/chains/{}/contracts/{}/tokens/42/path?limit=1",
        ctx.chain_id, ctx.contract
    );
    let (status, first_page) = get_json(&ctx.app, &first_page_uri).await;
    assert_eq!(status, StatusCode::OK);
    let first_items = first_page["items"].as_array().expect("first path items");
    assert_eq!(first_items.len(), 1);
    assert_eq!(first_items[0]["block_number"], 100);
    assert_eq!(first_items[0]["movement_type"], "mint");
    let cursor = first_page["next_cursor"]
        .as_str()
        .expect("path next cursor");
    assert_eq!(cursor, "100:0:0");

    let second_page_uri = format!(
        "/chains/{}/contracts/{}/tokens/42/path?limit=1&cursor={cursor}",
        ctx.chain_id, ctx.contract
    );
    let (status, second_page) = get_json(&ctx.app, &second_page_uri).await;
    assert_eq!(status, StatusCode::OK);
    let second_items = second_page["items"].as_array().expect("second path items");
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["block_number"], 101);
    assert_eq!(second_items[0]["movement_type"], "transfer");
    assert!(second_page["next_cursor"].is_null());
}

#[tokio::test]
async fn returns_clear_client_errors() {
    let ctx = setup();

    let missing_uri = format!(
        "/chains/{}/contracts/{}/summary",
        ctx.chain_id,
        address("ff")
    );
    let (status, body) = get_json(&ctx.app, &missing_uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "contract source not found");

    let invalid_address_uri = format!("/chains/{}/contracts/not-an-address/summary", ctx.chain_id);
    let (status, body) = get_json(&ctx.app, &invalid_address_uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid EVM contract address");

    let invalid_limit_uri = format!(
        "/chains/{}/contracts/{}/holders?limit=0",
        ctx.chain_id, ctx.contract
    );
    let (status, body) = get_json(&ctx.app, &invalid_limit_uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "limit must be between 1 and 100");

    let invalid_cursor_uri = format!(
        "/chains/{}/contracts/{}/transfers?cursor=not-a-cursor",
        ctx.chain_id, ctx.contract
    );
    let (status, body) = get_json(&ctx.app, &invalid_cursor_uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid cursor");
}

#[tokio::test]
async fn lists_transaction_hashes_missing_receipts() {
    let ctx = setup();
    let repo = LedgerRepository::new(ctx.pool.clone());
    let source = repo
        .source_by_contract(ctx.chain_id, &ctx.contract)
        .expect("load source")
        .expect("source exists");

    let missing = repo
        .transaction_hashes_missing_receipts(source.id, 10)
        .expect("load missing receipt hashes");
    assert_eq!(
        missing,
        vec![
            format!("0x{}", "01".repeat(32)),
            format!("0x{}", "02".repeat(32)),
            format!("0x{}", "03".repeat(32)),
        ]
    );

    repo.persist_transaction_receipts(
        ctx.chain_id,
        &[receipt(
            &ctx.contract,
            &format!("0x{}", "01".repeat(32)),
            100,
        )],
    )
    .expect("persist receipt");

    let missing = repo
        .transaction_hashes_missing_receipts(source.id, 10)
        .expect("load missing receipt hashes");
    assert_eq!(
        missing,
        vec![
            format!("0x{}", "02".repeat(32)),
            format!("0x{}", "03".repeat(32)),
        ]
    );
}

#[tokio::test]
async fn reprocessing_logs_does_not_double_count_balances() {
    let ctx = setup();
    let repo = LedgerRepository::new(ctx.pool.clone());
    let source = repo
        .source_by_contract(ctx.chain_id, &ctx.contract)
        .expect("load source")
        .expect("source exists");
    let logs = seeded_logs(&ctx.contract);

    let summary = repo
        .persist_decoded_logs(&source, &logs)
        .expect("reprocess decoded logs");
    assert_eq!(summary.holder_count, 2);

    let holders = repo
        .holders(ctx.chain_id, &ctx.contract, 10)
        .expect("load holders")
        .expect("holders exist");
    assert_eq!(holders.len(), 2);
    assert!(holders.iter().any(|holder| {
        holder.holder_address == address("22") && holder.token_id == "42" && holder.balance == "1"
    }));
    assert!(holders.iter().any(|holder| {
        holder.holder_address == address("33") && holder.token_id == "7" && holder.balance == "1"
    }));
    assert!(
        holders
            .iter()
            .all(|holder| holder.holder_address != address("11"))
    );
}

#[tokio::test]
async fn rejects_balance_updates_that_would_go_negative() {
    let ctx = setup();
    let repo = LedgerRepository::new(ctx.pool.clone());
    let source = repo
        .source_by_contract(ctx.chain_id, &ctx.contract)
        .expect("load source")
        .expect("source exists");
    let logs = vec![transfer_log(
        &ctx.contract,
        103,
        "04",
        0,
        &address("44"),
        &address("55"),
        "999",
    )];

    let error = repo
        .persist_decoded_logs(&source, &logs)
        .expect_err("negative balance should fail");
    assert!(
        format!("{error:#}").contains("would become negative"),
        "unexpected error: {error:#}"
    );

    let path = repo
        .token_path(ctx.chain_id, &ctx.contract, "999", 10)
        .expect("load token path")
        .expect("source exists");
    assert!(path.is_empty());
}

#[tokio::test]
async fn reprocessing_orphaned_rows_does_not_unorphan_them() {
    let ctx = setup();
    let repo = LedgerRepository::new(ctx.pool.clone());
    let source = repo
        .source_by_contract(ctx.chain_id, &ctx.contract)
        .expect("load source")
        .expect("source exists");
    let transaction_hash = format!("0x{}", "01".repeat(32));

    {
        let mut conn = ctx.pool.get().expect("get postgres connection");
        diesel::update(
            events::table
                .filter(events::chain_id.eq(ctx.chain_id))
                .filter(events::transaction_hash.eq(&transaction_hash))
                .filter(events::log_index.eq(0)),
        )
        .set(events::orphaned.eq(true))
        .execute(&mut conn)
        .expect("mark event orphaned");
        diesel::update(
            ledger_entries::table
                .filter(ledger_entries::chain_id.eq(ctx.chain_id))
                .filter(ledger_entries::transaction_hash.eq(&transaction_hash))
                .filter(ledger_entries::log_index.eq(0))
                .filter(ledger_entries::batch_index.eq(0)),
        )
        .set(ledger_entries::orphaned.eq(true))
        .execute(&mut conn)
        .expect("mark ledger entry orphaned");
    }

    let logs = vec![transfer_log(
        &ctx.contract,
        100,
        "01",
        0,
        &zero_address(),
        &address("11"),
        "42",
    )];
    repo.persist_decoded_logs(&source, &logs)
        .expect("reprocess orphaned log");

    let mut conn = ctx.pool.get().expect("get postgres connection");
    let event_orphaned = events::table
        .filter(events::chain_id.eq(ctx.chain_id))
        .filter(events::transaction_hash.eq(&transaction_hash))
        .select(events::orphaned)
        .first::<bool>(&mut conn)
        .expect("load event orphaned flag");
    let ledger_orphaned = ledger_entries::table
        .filter(ledger_entries::chain_id.eq(ctx.chain_id))
        .filter(ledger_entries::transaction_hash.eq(&transaction_hash))
        .select(ledger_entries::orphaned)
        .first::<bool>(&mut conn)
        .expect("load ledger orphaned flag");
    assert!(event_orphaned);
    assert!(ledger_orphaned);
}

async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read response body");
    let value = serde_json::from_slice(&body).expect("parse JSON response");

    (status, value)
}

fn seed_ledger(pool: PgPool, chain_id: i64, contract: &str) {
    let repositories = PostgresRepositories::new(pool);
    repositories
        .ledger()
        .ensure_chain(
            &format!("api-test-chain-{chain_id}"),
            chain_id,
            "<test>",
            12,
        )
        .expect("insert test chain");
    let source = repositories
        .ledger()
        .ensure_source(
            chain_id,
            "api-test-source",
            contract,
            TokenStandard::Erc721,
            100,
        )
        .expect("insert test source");

    let logs = seeded_logs(contract);
    LedgerRepository::new(repositories.pool().clone())
        .persist_decoded_logs(&source, &logs)
        .expect("persist decoded test logs");
    repositories
        .ledger()
        .advance_checkpoint(source.id, 102, &format!("0x{}", "bb".repeat(32)), 110)
        .expect("insert test checkpoint");
}

fn seeded_logs(contract: &str) -> Vec<(RpcLog, DecodedLog)> {
    vec![
        transfer_log(
            contract,
            100,
            "01",
            0,
            &zero_address(),
            &address("11"),
            "42",
        ),
        transfer_log(contract, 101, "02", 0, &address("11"), &address("22"), "42"),
        transfer_log(contract, 102, "03", 0, &zero_address(), &address("33"), "7"),
    ]
}

fn transfer_log(
    contract: &str,
    block_number: u64,
    tx_byte: &str,
    log_index: u64,
    from: &str,
    to: &str,
    token_id: &str,
) -> (RpcLog, DecodedLog) {
    (
        RpcLog {
            address: contract.to_string(),
            topics: Vec::new(),
            data: "0x".to_string(),
            block_number: format!("0x{block_number:x}"),
            transaction_hash: format!("0x{}", tx_byte.repeat(32)),
            transaction_index: Some("0x2".to_string()),
            log_index: format!("0x{log_index:x}"),
            block_hash: format!("0x{}", "aa".repeat(32)),
            block_timestamp: Some(block_timestamp(block_number)),
        },
        DecodedLog {
            event_name: "Transfer".to_string(),
            entries: vec![DecodedLedgerEntry {
                event_name: "Transfer".to_string(),
                token_standard: TokenStandard::Erc721,
                operator: None,
                from: from.to_string(),
                to: to.to_string(),
                token_id: token_id.to_string(),
                amount: "1".to_string(),
                batch_index: 0,
            }],
        },
    )
}

fn receipt(contract: &str, tx_hash: &str, block_number: u64) -> RpcTransactionReceipt {
    RpcTransactionReceipt {
        transaction_hash: tx_hash.to_string(),
        transaction_index: "0x2".to_string(),
        block_hash: format!("0x{}", "aa".repeat(32)),
        block_number: format!("0x{block_number:x}"),
        from: address("11"),
        to: Some(contract.to_string()),
        contract_address: None,
        status: Some("0x1".to_string()),
        gas_used: "0x5208".to_string(),
        cumulative_gas_used: "0x5208".to_string(),
        effective_gas_price: Some("0x10000000000000000".to_string()),
        transaction_type: Some("0x2".to_string()),
        raw: json!({ "transactionHash": tx_hash }),
    }
}

fn block_timestamp(block_number: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_609_459_200 + block_number as i64, 0)
        .expect("test block timestamp")
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
    9_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i64
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
