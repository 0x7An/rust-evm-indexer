use std::future::Future;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    application::evm::{DecodedLog, RpcLog, TokenStandard},
    domain::job::JobType,
};

pub trait ChainRpc {
    fn block_number(&self) -> impl Future<Output = Result<u64>> + Send;

    fn code_at(
        &self,
        contract_address: &str,
        block: u64,
    ) -> impl Future<Output = Result<String>> + Send;

    fn block_hash(&self, block: u64) -> impl Future<Output = Result<String>> + Send;

    fn block_timestamp(&self, block: u64) -> impl Future<Output = Result<DateTime<Utc>>> + Send;

    fn logs(
        &self,
        contract_address: &str,
        standard: TokenStandard,
        from_block: u64,
        to_block: u64,
    ) -> impl Future<Output = Result<Vec<RpcLog>>> + Send;

    fn transaction_receipt(
        &self,
        transaction_hash: &str,
    ) -> impl Future<Output = Result<TransactionReceipt>> + Send;
}

pub trait SourceDescriptor {
    fn source_id(&self) -> Uuid;
    fn chain_id(&self) -> i64;
    fn contract_address(&self) -> &str;
    fn token_standard(&self) -> &str;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRef {
    pub id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
}

impl SourceRef {
    pub fn from_source(source: &impl SourceDescriptor) -> Self {
        Self {
            id: source.source_id(),
            chain_id: source.chain_id(),
            contract_address: source.contract_address().to_string(),
            token_standard: source.token_standard().to_string(),
        }
    }
}

impl SourceDescriptor for SourceRef {
    fn source_id(&self) -> Uuid {
        self.id
    }

    fn chain_id(&self) -> i64 {
        self.chain_id
    }

    fn contract_address(&self) -> &str {
        &self.contract_address
    }

    fn token_standard(&self) -> &str {
        &self.token_standard
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub processed_block: i64,
    pub processed_block_hash: String,
    pub finalized_block: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexedBlockHash {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgEventInsert {
    pub source_id: Uuid,
    pub chain_id: i64,
    pub from_block: i64,
    pub to_block: i64,
    pub expected_block_hash: Option<String>,
    pub actual_block_hash: Option<String>,
    pub replay_job_id: Option<Uuid>,
    pub mismatches: Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistDecodedLogsOptions {
    pub restore_orphaned_conflicts: bool,
}

#[derive(Debug, Clone)]
pub struct ScanSummary {
    pub source_id: Uuid,
    pub events_seen: usize,
    pub events_persisted: usize,
    pub ledger_entries_persisted: usize,
    pub transaction_receipts_persisted: usize,
    pub holder_count: i64,
    pub minter_count: i64,
    pub top_holders: Vec<TokenBalanceSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalanceSnapshot {
    pub holder_address: String,
    pub token_id: String,
    pub balance: String,
    pub first_received_block: Option<i64>,
    pub last_moved_block: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionReceipt {
    pub transaction_hash: String,
    pub transaction_index: String,
    pub block_hash: String,
    pub block_number: String,
    pub from: String,
    pub to: Option<String>,
    pub contract_address: Option<String>,
    pub status: Option<String>,
    pub gas_used: String,
    pub cumulative_gas_used: String,
    pub effective_gas_price: Option<String>,
    pub transaction_type: Option<String>,
    pub raw: Value,
}

pub trait LedgerIngestRepository {
    fn persist_decoded_logs_with_options(
        &self,
        source: &impl SourceDescriptor,
        logs: &[(RpcLog, DecodedLog)],
        options: PersistDecodedLogsOptions,
    ) -> Result<ScanSummary>;

    fn persist_transaction_receipts(
        &self,
        chain_id: i64,
        receipts: &[TransactionReceipt],
    ) -> Result<usize>;
}

pub trait BackfillRepository {
    fn checkpoint_for_source(&self, source_id: Uuid) -> Result<Option<SourceCheckpoint>>;
    fn enqueue_range_job(&self, job: NewRangeJob) -> Result<EnqueueRangeJobResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRangeJob {
    pub job_type: JobType,
    pub source_id: Uuid,
    pub chain_id: i64,
    pub from_block: i64,
    pub to_block: i64,
    pub idempotency_key: String,
    pub max_attempts: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueRangeJobResult {
    Inserted,
    Existing,
}

pub trait ReorgRepository {
    fn indexed_block_hashes(
        &self,
        source_id: Uuid,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<IndexedBlockHash>>;

    fn checkpoint_for_source(&self, source_id: Uuid) -> Result<Option<SourceCheckpoint>>;

    fn record_reorg_event(&self, event: ReorgEventInsert) -> Result<()>;
}
