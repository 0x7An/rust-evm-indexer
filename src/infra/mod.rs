//! Infrastructure adapters.
//!
//! Diesel/Postgres repositories, EVM RPC adapters, telemetry, and config
//! loading live here once those checkpoints are implemented.

pub mod evm;
pub mod postgres;
pub mod telemetry;
