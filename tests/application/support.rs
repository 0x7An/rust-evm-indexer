use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use indexer_rs::application::{
    evm::{DecodedLog, RpcLog, TokenStandard},
    ports::{
        BackfillRepository, ChainRpc, EnqueueRangeJobResult, IndexedBlockHash,
        LedgerIngestRepository, NewRangeJob, PersistDecodedLogsOptions, ReorgEventInsert,
        ReorgRepository, ScanSummary, SourceCheckpoint, SourceRef, TransactionReceipt,
    },
};
use uuid::Uuid;

pub const CHAIN_ID: i64 = 1;
pub const CONTRACT: &str = "0x1000000000000000000000000000000000000000";

pub fn source(token_standard: TokenStandard) -> SourceRef {
    SourceRef {
        id: Uuid::from_u128(1),
        chain_id: CHAIN_ID,
        contract_address: CONTRACT.to_string(),
        token_standard: token_standard.as_str().to_string(),
    }
}

#[derive(Debug, Default)]
pub struct FakeBackfillRepository {
    checkpoint: Mutex<Option<SourceCheckpoint>>,
    jobs: Mutex<Vec<NewRangeJob>>,
    unique_ranges: Mutex<HashSet<(Uuid, String, i64, i64)>>,
}

impl FakeBackfillRepository {
    pub fn with_checkpoint(checkpoint: SourceCheckpoint) -> Self {
        Self {
            checkpoint: Mutex::new(Some(checkpoint)),
            jobs: Mutex::default(),
            unique_ranges: Mutex::default(),
        }
    }

    pub fn jobs(&self) -> Vec<NewRangeJob> {
        self.jobs.lock().expect("jobs lock").clone()
    }
}

impl BackfillRepository for FakeBackfillRepository {
    fn checkpoint_for_source(&self, _source_id: Uuid) -> Result<Option<SourceCheckpoint>> {
        Ok(self.checkpoint.lock().expect("checkpoint lock").clone())
    }

    fn enqueue_range_job(&self, job: NewRangeJob) -> Result<EnqueueRangeJobResult> {
        let key = (
            job.source_id,
            job.job_type.to_string(),
            job.from_block,
            job.to_block,
        );
        if !self.unique_ranges.lock().expect("range lock").insert(key) {
            return Ok(EnqueueRangeJobResult::Existing);
        }

        self.jobs.lock().expect("jobs lock").push(job);
        Ok(EnqueueRangeJobResult::Inserted)
    }
}

#[derive(Debug, Default)]
pub struct FakeIngestRepository {
    persisted: Mutex<Vec<(RpcLog, DecodedLog)>>,
    persist_options: Mutex<Vec<PersistDecodedLogsOptions>>,
    receipts: Mutex<Vec<TransactionReceipt>>,
}

impl FakeIngestRepository {
    pub fn persisted(&self) -> Vec<(RpcLog, DecodedLog)> {
        self.persisted.lock().expect("persisted lock").clone()
    }

    pub fn persist_options(&self) -> Vec<PersistDecodedLogsOptions> {
        self.persist_options
            .lock()
            .expect("persist options lock")
            .clone()
    }

    pub fn receipts(&self) -> Vec<TransactionReceipt> {
        self.receipts.lock().expect("receipts lock").clone()
    }
}

impl LedgerIngestRepository for FakeIngestRepository {
    fn persist_decoded_logs_with_options(
        &self,
        source: &impl indexer_rs::application::ports::SourceDescriptor,
        logs: &[(RpcLog, DecodedLog)],
        options: PersistDecodedLogsOptions,
    ) -> Result<ScanSummary> {
        self.persisted
            .lock()
            .expect("persisted lock")
            .extend(logs.iter().cloned());
        self.persist_options
            .lock()
            .expect("persist options lock")
            .push(options);

        Ok(ScanSummary {
            source_id: source.source_id(),
            events_seen: logs.len(),
            events_persisted: logs.len(),
            ledger_entries_persisted: logs.iter().map(|(_, decoded)| decoded.entries.len()).sum(),
            transaction_receipts_persisted: 0,
            holder_count: 1,
            minter_count: 1,
            top_holders: Vec::new(),
        })
    }

    fn persist_transaction_receipts(
        &self,
        _chain_id: i64,
        receipts: &[TransactionReceipt],
    ) -> Result<usize> {
        self.receipts
            .lock()
            .expect("receipts lock")
            .extend(receipts.iter().cloned());
        Ok(receipts.len())
    }
}

#[derive(Debug, Default)]
pub struct FakeReorgRepository {
    indexed: Mutex<Vec<IndexedBlockHash>>,
    checkpoint: Mutex<Option<SourceCheckpoint>>,
    reorg_events: Mutex<Vec<ReorgEventInsert>>,
}

impl FakeReorgRepository {
    pub fn with_indexed(indexed: Vec<IndexedBlockHash>) -> Self {
        Self {
            indexed: Mutex::new(indexed),
            checkpoint: Mutex::default(),
            reorg_events: Mutex::default(),
        }
    }

    pub fn with_checkpoint(self, checkpoint: SourceCheckpoint) -> Self {
        *self.checkpoint.lock().expect("checkpoint lock") = Some(checkpoint);
        self
    }

    pub fn reorg_events(&self) -> Vec<ReorgEventInsert> {
        self.reorg_events.lock().expect("reorg events lock").clone()
    }
}

impl ReorgRepository for FakeReorgRepository {
    fn indexed_block_hashes(
        &self,
        _source_id: Uuid,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<IndexedBlockHash>> {
        Ok(self
            .indexed
            .lock()
            .expect("indexed lock")
            .iter()
            .filter(|row| row.block_number >= from_block && row.block_number <= to_block)
            .cloned()
            .collect())
    }

    fn checkpoint_for_source(&self, _source_id: Uuid) -> Result<Option<SourceCheckpoint>> {
        Ok(self.checkpoint.lock().expect("checkpoint lock").clone())
    }

    fn record_reorg_event(&self, event: ReorgEventInsert) -> Result<()> {
        self.reorg_events
            .lock()
            .expect("reorg events lock")
            .push(event);
        Ok(())
    }
}

#[derive(Debug)]
pub struct FakeChainRpc {
    head: u64,
    logs: Mutex<HashMap<(String, u64, u64), Vec<RpcLog>>>,
    block_hashes: Mutex<HashMap<u64, String>>,
    receipts: Mutex<HashMap<String, TransactionReceipt>>,
}

impl Default for FakeChainRpc {
    fn default() -> Self {
        Self {
            head: 1_000,
            logs: Mutex::default(),
            block_hashes: Mutex::default(),
            receipts: Mutex::default(),
        }
    }
}

impl FakeChainRpc {
    pub fn with_head(mut self, head: u64) -> Self {
        self.head = head;
        self
    }

    pub fn add_logs(&self, standard: TokenStandard, from: u64, to: u64, logs: Vec<RpcLog>) {
        self.logs
            .lock()
            .expect("logs lock")
            .insert((standard.as_str().to_string(), from, to), logs);
    }

    pub fn add_block_hash(&self, block: u64, hash: String) {
        self.block_hashes
            .lock()
            .expect("block hashes lock")
            .insert(block, hash);
    }

    pub fn add_receipt(&self, receipt: TransactionReceipt) {
        self.receipts
            .lock()
            .expect("receipts lock")
            .insert(receipt.transaction_hash.clone(), receipt);
    }
}

impl ChainRpc for FakeChainRpc {
    async fn block_number(&self) -> Result<u64> {
        Ok(self.head)
    }

    async fn code_at(&self, _contract_address: &str, _block: u64) -> Result<String> {
        Ok("0x6000".to_string())
    }

    async fn block_hash(&self, block: u64) -> Result<String> {
        self.block_hashes
            .lock()
            .expect("block hashes lock")
            .get(&block)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing fake block hash for {block}"))
    }

    async fn block_timestamp(&self, block: u64) -> Result<DateTime<Utc>> {
        Utc.timestamp_opt(1_700_000_000 + block as i64, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid fake timestamp"))
    }

    async fn logs(
        &self,
        _contract_address: &str,
        standard: TokenStandard,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<RpcLog>> {
        Ok(self
            .logs
            .lock()
            .expect("logs lock")
            .get(&(standard.as_str().to_string(), from_block, to_block))
            .cloned()
            .unwrap_or_default())
    }

    async fn transaction_receipt(&self, transaction_hash: &str) -> Result<TransactionReceipt> {
        self.receipts
            .lock()
            .expect("receipts lock")
            .get(transaction_hash)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing fake receipt for {transaction_hash}"))
    }
}

pub fn erc721_transfer_log(block_number: u64, transaction_hash: &str, token_id: u64) -> RpcLog {
    RpcLog {
        address: CONTRACT.to_string(),
        topics: vec![
            indexer_rs::application::evm::event_topic("Transfer(address,address,uint256)"),
            topic_address_word("11"),
            topic_address_word("22"),
            format!("0x{token_id:064x}"),
        ],
        data: "0x".to_string(),
        block_number: format!("0x{block_number:x}"),
        transaction_hash: transaction_hash.to_ascii_lowercase(),
        transaction_index: Some("0x0".to_string()),
        log_index: "0x0".to_string(),
        block_hash: block_hash("aa"),
        block_timestamp: None,
    }
}

pub fn receipt(transaction_hash: &str, block_number: u64) -> TransactionReceipt {
    TransactionReceipt {
        transaction_hash: transaction_hash.to_ascii_lowercase(),
        transaction_index: "0x0".to_string(),
        block_hash: block_hash("aa"),
        block_number: format!("0x{block_number:x}"),
        from: "0x1111111111111111111111111111111111111111".to_string(),
        to: Some(CONTRACT.to_string()),
        contract_address: None,
        status: Some("0x1".to_string()),
        gas_used: "0x5208".to_string(),
        cumulative_gas_used: "0x5208".to_string(),
        effective_gas_price: Some("0x1".to_string()),
        transaction_type: Some("0x2".to_string()),
        raw: serde_json::json!({ "transactionHash": transaction_hash }),
    }
}

pub fn block_hash(byte: &str) -> String {
    format!("0x{}", byte.repeat(32))
}

fn topic_address_word(byte: &str) -> String {
    format!("0x{}{}", "00".repeat(12), byte.repeat(20))
}
