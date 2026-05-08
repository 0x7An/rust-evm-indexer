//! Public crate boundary for `indexer-rs`.
//!
//! The crate is organized around clean architecture boundaries. Domain and
//! application code stay independent from infrastructure adapters such as
//! Diesel, Axum, and EVM RPC clients.

pub mod api;
pub mod application;
pub mod cli;
pub mod config;
pub mod domain;
pub mod infra;
pub mod worker;
