use indexer_rs::application::{
    evm::TokenStandard,
    ports::{IndexedBlockHash, SourceCheckpoint},
    reorg::verify_source_reorgs,
};

use crate::support::{FakeChainRpc, FakeReorgRepository, block_hash, source};

#[tokio::test]
async fn verifies_reorgs_and_records_ranges_without_postgres_or_live_rpc() {
    let source = source(TokenStandard::Erc721);
    let ledger = FakeReorgRepository::with_indexed(vec![
        IndexedBlockHash {
            block_number: 100,
            block_hash: block_hash("11"),
        },
        IndexedBlockHash {
            block_number: 101,
            block_hash: block_hash("22"),
        },
    ])
    .with_checkpoint(SourceCheckpoint {
        processed_block: 102,
        processed_block_hash: block_hash("33"),
        finalized_block: 110,
    });
    let rpc = FakeChainRpc::default();
    rpc.add_block_hash(100, block_hash("aa"));
    rpc.add_block_hash(101, block_hash("22"));
    rpc.add_block_hash(102, block_hash("bb"));

    let verification = verify_source_reorgs(&rpc, &ledger, &source, 100, 102)
        .await
        .expect("verify source reorgs");

    assert_eq!(verification.checked_blocks, 3);
    assert_eq!(verification.mismatches.len(), 2);

    let events = ledger.reorg_events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].from_block, 100);
    assert_eq!(events[0].to_block, 100);
    assert_eq!(
        events[0].mismatches[0]["actual_block_hash"],
        block_hash("aa")
    );
    assert_eq!(events[1].from_block, 102);
    assert_eq!(events[1].to_block, 102);
    assert_eq!(
        events[1].mismatches[0]["expected_block_hash"],
        block_hash("33")
    );
}
