use indexer_rs::infra::telemetry::metrics::Metrics;

#[test]
fn prometheus_metrics_encode_counter_and_gauge_samples() {
    let metrics = Metrics::new().expect("create metrics registry");

    metrics.set_job_backlog("INGEST_RANGE", "queued", 4);
    metrics.inc_failed_job("INGEST_RANGE", "IngestJobError");
    metrics.inc_dead_letter_job("REPLAY_RANGE", "ReplayJobError");
    metrics.inc_worker_lease_failure("worker-1");
    metrics.inc_events_processed("1", "source-1", "Transfer", 3);
    metrics.inc_rpc_error("1", "RpcError");
    metrics.set_head_block("1", 250);
    metrics.set_finalized_block("1", 186);
    metrics.set_source_progress("1", "source-1", 100, 186);
    metrics.inc_reorgs_detected("1", "source-1", 2);

    let encoded = metrics.encode().expect("encode metrics");

    assert!(encoded.contains("indexer_events_processed_total"));
    assert!(encoded.contains("indexer_job_backlog{job_type=\"INGEST_RANGE\",status=\"queued\"} 4"));
    assert!(encoded.contains(
        "indexer_failed_jobs_total{error_class=\"IngestJobError\",job_type=\"INGEST_RANGE\"} 1"
    ));
    assert!(encoded.contains(
        "indexer_dead_letter_jobs_total{error_class=\"ReplayJobError\",job_type=\"REPLAY_RANGE\"} 1"
    ));
    assert!(encoded.contains("indexer_worker_lease_failures_total{worker_id=\"worker-1\"} 1"));
    assert!(encoded.contains(
        "indexer_events_processed_total{chain_id=\"1\",event_name=\"Transfer\",source_id=\"source-1\"} 3"
    ));
    assert!(
        encoded.contains("indexer_rpc_errors_total{chain_id=\"1\",error_class=\"RpcError\"} 1")
    );
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
