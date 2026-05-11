use indexer_rs::application::{
    evm::TokenStandard,
    ingest::{IngestOptions, ingest_source_range, resolve_finalized_range},
};

use crate::support::{FakeChainRpc, FakeIngestRepository, erc721_transfer_log, receipt, source};

#[tokio::test]
async fn ingests_and_decodes_source_range_without_postgres_or_live_rpc() {
    let source = source(TokenStandard::Erc721);
    let rpc = FakeChainRpc::default().with_head(250);
    let ledger = FakeIngestRepository::default();
    let tx_hash = format!("0x{}", "ab".repeat(32));

    rpc.add_logs(
        TokenStandard::Erc721,
        100,
        101,
        vec![erc721_transfer_log(100, &tx_hash, 42)],
    );
    rpc.add_receipt(receipt(&tx_hash, 100));

    let range = resolve_finalized_range(&rpc, Some("100"), "101", 0, 12)
        .await
        .expect("resolve finalized range");
    let summary = ingest_source_range(
        &rpc,
        &ledger,
        &source,
        range.from,
        range.to,
        2,
        IngestOptions {
            include_transaction_receipts: true,
            progress: false,
            restore_orphaned_conflicts: true,
            prefetched_logs: None,
        },
    )
    .await
    .expect("ingest source range");

    assert_eq!(summary.events_seen, 1);
    assert_eq!(summary.events_persisted, 1);
    assert_eq!(summary.ledger_entries_persisted, 1);
    assert_eq!(summary.transaction_receipts_persisted, 1);

    let persisted = ledger.persisted();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].1.entries[0].token_id, "42");
    assert_eq!(
        persisted[0].0.block_timestamp.unwrap().timestamp(),
        1_700_000_100
    );
    assert!(ledger.persist_options()[0].restore_orphaned_conflicts);
    assert_eq!(ledger.receipts()[0].transaction_hash, tx_hash);
}

#[tokio::test]
async fn auto_detects_standard_and_reuses_prefetched_chunk_without_live_rpc() {
    let source = source(TokenStandard::Auto);
    let rpc = FakeChainRpc::default();
    let ledger = FakeIngestRepository::default();
    let tx_hash = format!("0x{}", "cd".repeat(32));

    rpc.add_logs(
        TokenStandard::Auto,
        100,
        100,
        vec![erc721_transfer_log(100, &tx_hash, 7)],
    );

    let summary = ingest_source_range(
        &rpc,
        &ledger,
        &source,
        100,
        100,
        1,
        IngestOptions::default(),
    )
    .await
    .expect("ingest with auto detection");

    assert_eq!(summary.events_seen, 1);
    assert_eq!(
        ledger.persisted()[0].1.entries[0].token_standard,
        TokenStandard::Erc721
    );
}
