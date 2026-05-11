use indexer_rs::infra::telemetry::metrics::Metrics;

#[test]
fn prometheus_metrics_encode_counter_and_gauge_samples() {
    let metrics = Metrics::new().expect("create metrics registry");

    metrics.inc_events_processed("1", "0xabc", "erc721", 3);
    metrics.set_head_block("1", 250);
    metrics.set_finalized_block("1", 186);
    metrics.set_source_progress("1", "source-1", 100, 186);
    metrics.inc_reorgs_detected("1", "source-1", 2);

    let encoded = metrics.encode().expect("encode metrics");

    assert!(encoded.contains("indexer_events_processed_total"));
    assert!(encoded.contains("indexer_events_processed_total{chain_id=\"1\",contract=\"0xabc\",token_standard=\"erc721\"} 3"));
    assert!(encoded.contains("indexer_head_block{chain_id=\"1\"} 250"));
    assert!(encoded.contains("indexer_finalized_block{chain_id=\"1\"} 186"));
    assert!(encoded.contains("indexer_processed_block{chain_id=\"1\",source_id=\"source-1\"} 100"));
    assert!(
        encoded.contains("indexer_source_lag_blocks{chain_id=\"1\",source_id=\"source-1\"} 86")
    );
    assert!(
        encoded.contains("indexer_reorgs_detected_total{chain_id=\"1\",source_id=\"source-1\"} 2")
    );
}
