//! Application use cases and ports.
//!
//! This layer owns workflows such as planning ingestion, executing ranges,
//! replaying ranges, and verifying reorgs. Infrastructure implements the
//! ports defined here in later checkpoints.

pub mod backfill;
pub mod evm;
pub mod ingest;
pub mod ports;
pub mod reorg;
