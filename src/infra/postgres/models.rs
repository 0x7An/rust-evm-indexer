use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde_json::Value;
use uuid::Uuid;

use super::schema::{
    chains, checkpoints, events, job_attempts, jobs, ledger_entries, reorg_events, sources,
    token_balances, transaction_receipts,
};

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = chains)]
pub struct ChainRow {
    pub id: i64,
    pub name: String,
    pub chain_id: i64,
    pub rpc_url: String,
    pub finality_confirmations: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = chains)]
pub struct NewChainRow {
    pub name: String,
    pub chain_id: i64,
    pub rpc_url: String,
    pub finality_confirmations: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = sources)]
pub struct SourceRow {
    pub id: Uuid,
    pub chain_id: i64,
    pub name: String,
    pub contract_address: String,
    pub token_standard: String,
    pub event_signatures: Value,
    pub start_block: i64,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = sources)]
pub struct NewSourceRow {
    pub id: Uuid,
    pub chain_id: i64,
    pub name: String,
    pub contract_address: String,
    pub token_standard: String,
    pub event_signatures: Value,
    pub start_block: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = checkpoints)]
pub struct CheckpointRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub processed_block: i64,
    pub processed_block_hash: String,
    pub finalized_block: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = checkpoints)]
pub struct NewCheckpointRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub processed_block: i64,
    pub processed_block_hash: String,
    pub finalized_block: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = events)]
pub struct EventRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: Option<i32>,
    pub log_index: i32,
    pub contract_address: String,
    pub event_name: String,
    pub topics: Value,
    pub data: String,
    pub args: Value,
    pub finalized: bool,
    pub orphaned: bool,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = events)]
pub struct NewEventRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: Option<i32>,
    pub log_index: i32,
    pub contract_address: String,
    pub event_name: String,
    pub topics: Value,
    pub data: String,
    pub args: Value,
    pub finalized: bool,
    pub orphaned: bool,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = ledger_entries)]
pub struct LedgerEntryRow {
    pub id: Uuid,
    pub event_id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
    pub movement_type: String,
    pub operator_address: Option<String>,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_id: String,
    pub amount: BigDecimal,
    pub batch_index: i32,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: Option<i32>,
    pub log_index: i32,
    pub orphaned: bool,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = ledger_entries)]
pub struct NewLedgerEntryRow {
    pub id: Uuid,
    pub event_id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
    pub movement_type: String,
    pub operator_address: Option<String>,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_id: String,
    pub amount: BigDecimal,
    pub batch_index: i32,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: Option<i32>,
    pub log_index: i32,
    pub orphaned: bool,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = token_balances)]
pub struct TokenBalanceRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
    pub holder_address: String,
    pub token_id: String,
    pub balance: BigDecimal,
    pub first_received_block: Option<i64>,
    pub last_moved_block: Option<i64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = token_balances)]
pub struct NewTokenBalanceRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
    pub holder_address: String,
    pub token_id: String,
    pub balance: BigDecimal,
    pub first_received_block: Option<i64>,
    pub last_moved_block: Option<i64>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = transaction_receipts)]
pub struct TransactionReceiptRow {
    pub id: Uuid,
    pub chain_id: i64,
    pub transaction_hash: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_index: Option<i32>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub contract_address: Option<String>,
    pub status: Option<i32>,
    pub gas_used: BigDecimal,
    pub cumulative_gas_used: BigDecimal,
    pub effective_gas_price: Option<BigDecimal>,
    pub transaction_type: Option<String>,
    pub raw_receipt: Value,
    pub inserted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = transaction_receipts)]
pub struct NewTransactionReceiptRow {
    pub id: Uuid,
    pub chain_id: i64,
    pub transaction_hash: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_index: Option<i32>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub contract_address: Option<String>,
    pub status: Option<i32>,
    pub gas_used: BigDecimal,
    pub cumulative_gas_used: BigDecimal,
    pub effective_gas_price: Option<BigDecimal>,
    pub transaction_type: Option<String>,
    pub raw_receipt: Value,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = jobs)]
pub struct JobRow {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub source_id: Option<Uuid>,
    pub chain_id: i64,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub idempotency_key: String,
    pub leased_by: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = jobs)]
pub struct NewJobRow {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub source_id: Option<Uuid>,
    pub chain_id: i64,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub idempotency_key: String,
    pub max_attempts: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = job_attempts)]
pub struct JobAttemptRow {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = job_attempts)]
pub struct NewJobAttemptRow {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = reorg_events)]
pub struct ReorgEventRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub from_block: i64,
    pub to_block: i64,
    pub expected_block_hash: Option<String>,
    pub actual_block_hash: Option<String>,
    pub detected_at: DateTime<Utc>,
    pub replay_job_id: Option<Uuid>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = reorg_events)]
pub struct NewReorgEventRow {
    pub id: Uuid,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub from_block: i64,
    pub to_block: i64,
    pub expected_block_hash: Option<String>,
    pub actual_block_hash: Option<String>,
    pub replay_job_id: Option<Uuid>,
}
