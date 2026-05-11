use std::sync::OnceLock;

use anyhow::{Context, Result};
use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| Metrics::new().expect("register Prometheus metrics"))
}

pub async fn prometheus_response() -> Response {
    match metrics().encode() {
        Ok(body) => (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode metrics: {error}"),
        )
            .into_response(),
    }
}

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    job_backlog: IntGaugeVec,
    failed_jobs_total: IntCounterVec,
    dead_letter_jobs_total: IntCounterVec,
    worker_lease_failures_total: IntCounterVec,
    events_processed_total: IntCounterVec,
    rpc_errors_total: IntCounterVec,
    db_write_duration_ms: HistogramVec,
    head_block: IntGaugeVec,
    finalized_block: IntGaugeVec,
    processed_block: IntGaugeVec,
    source_lag_blocks: IntGaugeVec,
    reorgs_detected_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let job_backlog = int_gauge_vec(
            "indexer_job_backlog",
            "Durable jobs by type and status.",
            &["job_type", "status"],
        )?;
        let failed_jobs_total = int_counter_vec(
            "indexer_failed_jobs_total",
            "Failed worker jobs by type and error class.",
            &["job_type", "error_class"],
        )?;
        let dead_letter_jobs_total = int_counter_vec(
            "indexer_dead_letter_jobs_total",
            "Dead-lettered worker jobs by type and error class.",
            &["job_type", "error_class"],
        )?;
        let worker_lease_failures_total = int_counter_vec(
            "indexer_worker_lease_failures_total",
            "Worker lease failures by worker id.",
            &["worker_id"],
        )?;
        let events_processed_total = int_counter_vec(
            "indexer_events_processed_total",
            "Decoded ledger events persisted by chain, source, and event name.",
            &["chain_id", "source_id", "event_name"],
        )?;
        let rpc_errors_total = int_counter_vec(
            "indexer_rpc_errors_total",
            "RPC errors by chain and error class.",
            &["chain_id", "error_class"],
        )?;
        let db_write_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "indexer_db_write_duration_ms",
                "Database write duration in milliseconds by operation.",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 5_000.0,
            ]),
            &["operation"],
        )
        .context("create indexer_db_write_duration_ms")?;
        let head_block = int_gauge_vec(
            "indexer_head_block",
            "Observed chain head block by chain.",
            &["chain_id"],
        )?;
        let finalized_block = int_gauge_vec(
            "indexer_finalized_block",
            "Observed finalized block by chain.",
            &["chain_id"],
        )?;
        let processed_block = int_gauge_vec(
            "indexer_processed_block",
            "Source checkpoint processed block.",
            &["chain_id", "source_id"],
        )?;
        let source_lag_blocks = int_gauge_vec(
            "indexer_source_lag_blocks",
            "Source finalized minus processed block lag.",
            &["chain_id", "source_id"],
        )?;
        let reorgs_detected_total = int_counter_vec(
            "indexer_reorgs_detected_total",
            "Detected reorg mismatches by chain and source.",
            &["chain_id", "source_id"],
        )?;

        for collector in [
            Box::new(job_backlog.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(failed_jobs_total.clone()),
            Box::new(dead_letter_jobs_total.clone()),
            Box::new(worker_lease_failures_total.clone()),
            Box::new(events_processed_total.clone()),
            Box::new(rpc_errors_total.clone()),
            Box::new(db_write_duration_ms.clone()),
            Box::new(head_block.clone()),
            Box::new(finalized_block.clone()),
            Box::new(processed_block.clone()),
            Box::new(source_lag_blocks.clone()),
            Box::new(reorgs_detected_total.clone()),
        ] {
            registry
                .register(collector)
                .context("register Prometheus metric")?;
        }

        Ok(Self {
            registry,
            job_backlog,
            failed_jobs_total,
            dead_letter_jobs_total,
            worker_lease_failures_total,
            events_processed_total,
            rpc_errors_total,
            db_write_duration_ms,
            head_block,
            finalized_block,
            processed_block,
            source_lag_blocks,
            reorgs_detected_total,
        })
    }

    pub fn encode(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .context("encode Prometheus metrics")?;
        String::from_utf8(buffer).context("Prometheus metrics were not UTF-8")
    }

    pub fn set_job_backlog(&self, job_type: &str, status: &str, count: i64) {
        self.job_backlog
            .with_label_values(&[job_type, status])
            .set(count);
    }

    pub fn inc_failed_job(&self, job_type: &str, error_class: &str) {
        self.failed_jobs_total
            .with_label_values(&[job_type, error_class])
            .inc();
    }

    pub fn inc_dead_letter_job(&self, job_type: &str, error_class: &str) {
        self.dead_letter_jobs_total
            .with_label_values(&[job_type, error_class])
            .inc();
    }

    pub fn inc_worker_lease_failure(&self, worker_id: &str) {
        self.worker_lease_failures_total
            .with_label_values(&[worker_id])
            .inc();
    }

    pub fn inc_events_processed(
        &self,
        chain_id: &str,
        source_id: &str,
        event_name: &str,
        count: u64,
    ) {
        if count > 0 {
            self.events_processed_total
                .with_label_values(&[chain_id, source_id, event_name])
                .inc_by(count);
        }
    }

    pub fn inc_rpc_error(&self, chain_id: &str, error_class: &str) {
        self.rpc_errors_total
            .with_label_values(&[chain_id, error_class])
            .inc();
    }

    pub fn observe_db_write_ms(&self, operation: &str, duration_ms: f64) {
        self.db_write_duration_ms
            .with_label_values(&[operation])
            .observe(duration_ms);
    }

    pub fn set_head_block(&self, chain_id: &str, block: u64) {
        self.head_block
            .with_label_values(&[chain_id])
            .set(saturating_i64(block));
    }

    pub fn set_finalized_block(&self, chain_id: &str, block: u64) {
        self.finalized_block
            .with_label_values(&[chain_id])
            .set(saturating_i64(block));
    }

    pub fn set_source_progress(
        &self,
        chain_id: &str,
        source_id: &str,
        processed_block: i64,
        finalized_block: i64,
    ) {
        self.processed_block
            .with_label_values(&[chain_id, source_id])
            .set(processed_block);
        self.source_lag_blocks
            .with_label_values(&[chain_id, source_id])
            .set(finalized_block.saturating_sub(processed_block).max(0));
    }

    pub fn inc_reorgs_detected(&self, chain_id: &str, source_id: &str, count: u64) {
        if count > 0 {
            self.reorgs_detected_total
                .with_label_values(&[chain_id, source_id])
                .inc_by(count);
        }
    }
}

fn int_counter_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntCounterVec> {
    IntCounterVec::new(Opts::new(name, help), labels).with_context(|| format!("create {name}"))
}

fn int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
    IntGaugeVec::new(Opts::new(name, help), labels).with_context(|| format!("create {name}"))
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
