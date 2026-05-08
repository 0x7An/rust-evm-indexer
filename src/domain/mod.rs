//! Pure domain types and invariants.
//!
//! This layer must not depend on Diesel, Axum, Tokio runtime wiring, RPC
//! client types, or Postgres models.

pub mod chain;
pub mod checkpoint;
pub mod event;
pub mod job;
pub mod reorg;
pub mod source;

use std::fmt;

pub type DomainResult<T> = Result<T, DomainError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    EmptyValue(&'static str),
    InvalidAddress(String),
    InvalidHash {
        field: &'static str,
        value: String,
    },
    InvalidAmount(String),
    InvalidRange {
        from: chain::BlockNumber,
        to: chain::BlockNumber,
    },
    InvalidCheckpoint {
        processed: chain::BlockNumber,
        finalized: chain::BlockNumber,
    },
    InvalidEnumValue {
        field: &'static str,
        value: String,
    },
    InvalidLedgerEntry(&'static str),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyValue(field) => write!(f, "{field} cannot be empty"),
            Self::InvalidAddress(value) => write!(f, "invalid EVM address: {value}"),
            Self::InvalidHash { field, value } => write!(f, "invalid {field}: {value}"),
            Self::InvalidAmount(value) => write!(f, "invalid token amount: {value}"),
            Self::InvalidRange { from, to } => {
                write!(f, "invalid block range: {from}..={to}")
            }
            Self::InvalidCheckpoint {
                processed,
                finalized,
            } => write!(
                f,
                "processed block {processed} cannot be greater than finalized block {finalized}"
            ),
            Self::InvalidEnumValue { field, value } => {
                write!(f, "invalid {field}: {value}")
            }
            Self::InvalidLedgerEntry(message) => write!(f, "invalid ledger entry: {message}"),
        }
    }
}

impl std::error::Error for DomainError {}

pub(crate) fn require_non_empty(
    field: &'static str,
    value: impl Into<String>,
) -> DomainResult<String> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(DomainError::EmptyValue(field));
    }
    Ok(value)
}
