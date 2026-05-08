//! Postgres infrastructure adapter boundary.
//!
//! This module owns Diesel-specific schema, row models, connection helpers,
//! and repository implementations. Diesel types must not leak into `domain`
//! or `application`.

pub mod connection;
pub mod models;
pub mod repositories;
pub mod schema;
