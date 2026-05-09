use std::collections::HashMap;

use anyhow::{Context, Result};
use bigdecimal::{
    BigDecimal, Zero,
    num_bigint::{BigInt, BigUint},
};
use chrono::{DateTime, Utc};
use diesel::{
    PgConnection, QueryableByName,
    prelude::*,
    sql_types::{BigInt as SqlBigInt, Nullable, Text as SqlText, Timestamptz, Uuid as SqlUuid},
    upsert::excluded,
};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    domain::job::{JobStatus, JobType},
    infra::evm::{
        decoder::{DecodedLog, RpcLog, TokenStandard, parse_hex_u64},
        rpc::RpcTransactionReceipt,
    },
};

use super::{
    connection::PgPool,
    models::{
        ChainRow, CheckpointRow, LedgerEntryRow, NewChainRow, NewCheckpointRow, NewEventRow,
        NewLedgerEntryRow, NewReorgEventRow, NewSourceRow, NewTokenBalanceRow,
        NewTransactionReceiptRow, ReorgEventRow, SourceRow, TokenBalanceRow, TransactionReceiptRow,
    },
    schema::{
        chains, checkpoints, events, jobs, ledger_entries, reorg_events, sources, token_balances,
        transaction_receipts,
    },
};

#[derive(Debug, Clone)]
pub struct ScanSummary {
    pub source_id: Uuid,
    pub events_seen: usize,
    pub events_persisted: usize,
    pub ledger_entries_persisted: usize,
    pub transaction_receipts_persisted: usize,
    pub holder_count: i64,
    pub minter_count: i64,
    pub top_holders: Vec<TokenBalanceRow>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContractSummary {
    pub source_id: Uuid,
    pub chain_id: i64,
    pub contract_address: String,
    pub token_standard: String,
    pub start_block: i64,
    pub enabled: bool,
    pub event_count: i64,
    pub ledger_entry_count: i64,
    pub holder_count: i64,
    pub minter_count: i64,
    pub first_indexed_block: Option<i64>,
    pub last_indexed_block: Option<i64>,
    pub checkpoint_processed_block: Option<i64>,
    pub checkpoint_finalized_block: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HolderBalance {
    pub holder_address: String,
    pub token_id: String,
    pub balance: String,
    pub first_received_block: Option<i64>,
    pub last_moved_block: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MinterSummary {
    pub minter_address: String,
    pub mint_count: i64,
    pub first_mint_block: i64,
    pub first_mint_timestamp: Option<DateTime<Utc>>,
    pub last_mint_block: i64,
    pub last_mint_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LedgerTransfer {
    pub movement_type: String,
    pub operator_address: Option<String>,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_id: String,
    pub amount: String,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub transaction_hash: String,
    pub transaction_index: Option<i32>,
    pub log_index: i32,
    pub batch_index: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMetadataUpdate {
    pub events_updated: usize,
    pub ledger_entries_updated: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanRangeSummary {
    pub events_orphaned: usize,
    pub ledger_entries_orphaned: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistDecodedLogsOptions {
    pub restore_orphaned_conflicts: bool,
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

#[derive(Debug, QueryableByName)]
struct MissingReceiptHashRow {
    #[diesel(sql_type = SqlText)]
    transaction_hash: String,
}

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = SqlBigInt)]
    count: i64,
}

#[derive(Debug, QueryableByName)]
struct MinterSummaryRow {
    #[diesel(sql_type = SqlText)]
    minter_address: String,
    #[diesel(sql_type = SqlBigInt)]
    mint_count: i64,
    #[diesel(sql_type = SqlBigInt)]
    first_mint_block: i64,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    first_mint_timestamp: Option<DateTime<Utc>>,
    #[diesel(sql_type = SqlBigInt)]
    last_mint_block: i64,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    last_mint_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LedgerCursor {
    pub block_number: i64,
    pub log_index: i32,
    pub batch_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerQuery {
    pub limit: i64,
    pub cursor: Option<LedgerCursor>,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub holder: Option<String>,
    pub token_id: Option<String>,
    pub movement_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LedgerPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<LedgerCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerSort {
    Asc,
    Desc,
}

#[derive(Clone)]
pub struct LedgerRepository {
    pool: PgPool,
}

impl LedgerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn ensure_chain(
        &self,
        name: &str,
        chain_id: i64,
        rpc_url: &str,
        finality_confirmations: i64,
    ) -> Result<ChainRow> {
        let mut conn = self.connection()?;
        diesel::insert_into(chains::table)
            .values(NewChainRow {
                name: name.to_string(),
                chain_id,
                rpc_url: rpc_url.to_string(),
                finality_confirmations,
            })
            .on_conflict(chains::chain_id)
            .do_update()
            .set((
                chains::name.eq(name),
                chains::rpc_url.eq(rpc_url),
                chains::finality_confirmations.eq(finality_confirmations),
            ))
            .get_result(&mut conn)
            .context("upsert chain")
    }

    pub fn chain_by_chain_id(&self, chain_id: i64) -> Result<Option<ChainRow>> {
        let mut conn = self.connection()?;
        chains::table
            .filter(chains::chain_id.eq(chain_id))
            .first::<ChainRow>(&mut conn)
            .optional()
            .context("load chain by chain_id")
    }

    pub fn ensure_source(
        &self,
        chain_id: i64,
        name: &str,
        contract_address: &str,
        standard: TokenStandard,
        start_block: i64,
    ) -> Result<SourceRow> {
        if standard.is_auto() {
            anyhow::bail!("source token standard must be detected before persistence");
        }

        let mut conn = self.connection()?;
        diesel::insert_into(sources::table)
            .values(NewSourceRow {
                id: Uuid::new_v4(),
                chain_id,
                name: name.to_string(),
                contract_address: contract_address.to_ascii_lowercase(),
                token_standard: standard.as_str().to_string(),
                event_signatures: match standard {
                    TokenStandard::Auto => {
                        unreachable!("auto token standard is rejected before source persistence")
                    }
                    TokenStandard::Erc20 | TokenStandard::Erc721 => {
                        json!(["Transfer(address,address,uint256)"])
                    }
                    TokenStandard::Erc1155 => {
                        json!([
                            "TransferSingle(address,address,address,uint256,uint256)",
                            "TransferBatch(address,address,address,uint256[],uint256[])"
                        ])
                    }
                },
                start_block,
                enabled: true,
            })
            .on_conflict((sources::chain_id, sources::contract_address))
            .do_update()
            .set((
                sources::name.eq(name),
                sources::token_standard.eq(standard.as_str()),
                sources::event_signatures.eq(excluded(sources::event_signatures)),
                sources::start_block.eq(start_block),
                sources::enabled.eq(true),
            ))
            .get_result(&mut conn)
            .context("upsert source")
    }

    pub fn persist_decoded_logs(
        &self,
        source: &SourceRow,
        logs: &[(RpcLog, DecodedLog)],
    ) -> Result<ScanSummary> {
        self.persist_decoded_logs_with_options(source, logs, PersistDecodedLogsOptions::default())
    }

    pub fn persist_decoded_logs_with_options(
        &self,
        source: &SourceRow,
        logs: &[(RpcLog, DecodedLog)],
        options: PersistDecodedLogsOptions,
    ) -> Result<ScanSummary> {
        let mut conn = self.connection()?;
        conn.transaction::<ScanSummary, anyhow::Error, _>(|conn| {
            self.lock_source_for_write_conn(conn, source)?;
            let mut events_persisted = 0;
            let mut ledger_entries_persisted = 0;

            for (log, decoded) in logs {
                let event_id = self.upsert_event(conn, source, log, decoded, options)?;
                events_persisted += 1;

                for entry in &decoded.entries {
                    let previous =
                        self.lock_existing_ledger_entry(conn, source, log, entry.batch_index)?;
                    if previous
                        .as_ref()
                        .is_some_and(|row| row.source_id != source.id)
                    {
                        anyhow::bail!(
                            "existing ledger entry for transaction {} log {} batch {} belongs to another source",
                            log.transaction_hash,
                            log.log_index,
                            entry.batch_index
                        );
                    }
                    let current =
                        self.upsert_ledger_entry(conn, source, log, event_id, entry, options)?;
                    self.apply_balance_delta(conn, source, previous.as_ref(), &current)?;
                    ledger_entries_persisted += 1;
                }
            }

            let holder_count = self.holder_count_conn(conn, source.id)?;
            let minter_count = self.minter_count_conn(conn, source.id)?;
            let top_holders = self.top_holders_conn(conn, source.id, 10)?;

            Ok(ScanSummary {
                source_id: source.id,
                events_seen: logs.len(),
                events_persisted,
                ledger_entries_persisted,
                transaction_receipts_persisted: 0,
                holder_count,
                minter_count,
                top_holders,
            })
        })
        .context("persist decoded logs")
    }

    pub fn persist_transaction_receipts(
        &self,
        chain_id: i64,
        receipts: &[RpcTransactionReceipt],
    ) -> Result<usize> {
        let mut conn = self.connection()?;
        conn.transaction::<usize, anyhow::Error, _>(|conn| {
            let mut persisted = 0;
            for receipt in receipts {
                self.upsert_transaction_receipt(conn, chain_id, receipt)?;
                persisted += 1;
            }

            Ok(persisted)
        })
        .context("persist transaction receipts")
    }

    pub fn transaction_hashes_missing_receipts(
        &self,
        source_id: Uuid,
        limit: i64,
    ) -> Result<Vec<String>> {
        let mut conn = self.connection()?;
        diesel::sql_query(
            "SELECT le.transaction_hash AS transaction_hash
             FROM ledger_entries le
             LEFT JOIN transaction_receipts tr
               ON tr.chain_id = le.chain_id
              AND tr.transaction_hash = le.transaction_hash
             WHERE le.source_id = $1
               AND le.orphaned = false
               AND tr.id IS NULL
             GROUP BY le.transaction_hash
             ORDER BY MIN(le.block_number), le.transaction_hash
             LIMIT $2",
        )
        .bind::<SqlUuid, _>(source_id)
        .bind::<SqlBigInt, _>(limit.clamp(1, 10_000))
        .load::<MissingReceiptHashRow>(&mut conn)
        .map(|rows| rows.into_iter().map(|row| row.transaction_hash).collect())
        .context("load transaction hashes missing receipts")
    }

    pub fn source_by_contract(
        &self,
        chain_id: i64,
        contract_address: &str,
    ) -> Result<Option<SourceRow>> {
        let mut conn = self.connection()?;
        self.source_by_contract_conn(&mut conn, chain_id, contract_address)
    }

    pub fn source_by_id(&self, source_id: Uuid) -> Result<Option<SourceRow>> {
        let mut conn = self.connection()?;
        sources::table
            .filter(sources::id.eq(source_id))
            .first(&mut conn)
            .optional()
            .context("load source by id")
    }

    pub fn checkpoint_for_source(&self, source_id: Uuid) -> Result<Option<CheckpointRow>> {
        let mut conn = self.connection()?;
        checkpoints::table
            .filter(checkpoints::source_id.eq(source_id))
            .first(&mut conn)
            .optional()
            .context("load checkpoint by source")
    }

    pub fn indexed_block_hashes(
        &self,
        source_id: Uuid,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<IndexedBlockHash>> {
        let mut conn = self.connection()?;
        events::table
            .filter(events::source_id.eq(source_id))
            .filter(events::orphaned.eq(false))
            .filter(events::block_number.ge(from_block))
            .filter(events::block_number.le(to_block))
            .select((events::block_number, events::block_hash))
            .distinct()
            .order((events::block_number.asc(), events::block_hash.asc()))
            .load::<(i64, String)>(&mut conn)
            .map(|rows| {
                rows.into_iter()
                    .map(|(block_number, block_hash)| IndexedBlockHash {
                        block_number,
                        block_hash,
                    })
                    .collect()
            })
            .context("load indexed block hashes")
    }

    pub fn record_reorg_event(&self, event: ReorgEventInsert) -> Result<ReorgEventRow> {
        let mut conn = self.connection()?;
        diesel::insert_into(reorg_events::table)
            .values(NewReorgEventRow {
                id: Uuid::new_v4(),
                source_id: event.source_id,
                chain_id: event.chain_id,
                from_block: event.from_block,
                to_block: event.to_block,
                expected_block_hash: event
                    .expected_block_hash
                    .map(|value| value.to_ascii_lowercase()),
                actual_block_hash: event
                    .actual_block_hash
                    .map(|value| value.to_ascii_lowercase()),
                replay_job_id: event.replay_job_id,
                mismatches: event.mismatches,
            })
            .get_result(&mut conn)
            .context("insert reorg event")
    }

    pub fn reorg_events_for_source(&self, source_id: Uuid) -> Result<Vec<ReorgEventRow>> {
        let mut conn = self.connection()?;
        reorg_events::table
            .filter(reorg_events::source_id.eq(source_id))
            .order((
                reorg_events::from_block.asc(),
                reorg_events::detected_at.asc(),
            ))
            .load(&mut conn)
            .context("load reorg events for source")
    }

    pub fn orphan_source_range(
        &self,
        source: &SourceRow,
        from_block: i64,
        to_block: i64,
    ) -> Result<OrphanRangeSummary> {
        if from_block < 0 || to_block < 0 {
            anyhow::bail!("orphan range cannot be negative");
        }
        if from_block > to_block {
            anyhow::bail!("from-block {from_block} cannot be greater than to-block {to_block}");
        }

        let mut conn = self.connection()?;
        conn.transaction::<OrphanRangeSummary, anyhow::Error, _>(|conn| {
            self.lock_source_for_write_conn(conn, source)?;
            let rows = ledger_entries::table
                .filter(ledger_entries::source_id.eq(source.id))
                .filter(ledger_entries::block_number.ge(from_block))
                .filter(ledger_entries::block_number.le(to_block))
                .filter(ledger_entries::orphaned.eq(false))
                .order((
                    ledger_entries::block_number.asc(),
                    ledger_entries::log_index.asc(),
                    ledger_entries::batch_index.asc(),
                ))
                .for_update()
                .load::<LedgerEntryRow>(conn)
                .context("lock ledger entries for orphaning")?;

            for row in &rows {
                let mut orphaned = row.clone();
                orphaned.orphaned = true;
                self.apply_balance_delta(conn, source, Some(row), &orphaned)?;
            }

            let ledger_entries_orphaned = diesel::update(
                ledger_entries::table
                    .filter(ledger_entries::source_id.eq(source.id))
                    .filter(ledger_entries::block_number.ge(from_block))
                    .filter(ledger_entries::block_number.le(to_block))
                    .filter(ledger_entries::orphaned.eq(false)),
            )
            .set(ledger_entries::orphaned.eq(true))
            .execute(conn)
            .context("mark ledger entries orphaned")?;

            let events_orphaned = diesel::update(
                events::table
                    .filter(events::source_id.eq(source.id))
                    .filter(events::block_number.ge(from_block))
                    .filter(events::block_number.le(to_block))
                    .filter(events::orphaned.eq(false)),
            )
            .set(events::orphaned.eq(true))
            .execute(conn)
            .context("mark events orphaned")?;

            Ok(OrphanRangeSummary {
                events_orphaned,
                ledger_entries_orphaned,
            })
        })
        .context("orphan source range")
    }

    pub fn event_blocks_missing_metadata(&self, source_id: Uuid, limit: i64) -> Result<Vec<i64>> {
        let mut conn = self.connection()?;
        events::table
            .filter(events::source_id.eq(source_id))
            .filter(
                events::block_timestamp
                    .is_null()
                    .or(events::transaction_index.is_null())
                    .or(events::topics.eq(json!([]))),
            )
            .select(events::block_number)
            .distinct()
            .order(events::block_number.asc())
            .limit(limit.clamp(1, 10_000))
            .load(&mut conn)
            .context("load event blocks missing metadata")
    }

    pub fn update_log_metadata(
        &self,
        source: &SourceRow,
        log: &RpcLog,
    ) -> Result<LogMetadataUpdate> {
        let mut conn = self.connection()?;
        conn.transaction::<LogMetadataUpdate, anyhow::Error, _>(|conn| {
            let transaction_hash = log.transaction_hash.to_ascii_lowercase();
            let log_index = parse_hex_u64(&log.log_index)? as i32;
            let block_number = parse_hex_u64(&log.block_number)? as i64;
            let block_timestamp = log.block_timestamp.to_owned();
            let block_hash = log.block_hash.to_ascii_lowercase();
            let transaction_index = parse_optional_hex_i32(log.transaction_index.as_deref())?;

            let events_updated = diesel::update(
                events::table
                    .filter(events::source_id.eq(source.id))
                    .filter(events::chain_id.eq(source.chain_id))
                    .filter(events::transaction_hash.eq(&transaction_hash))
                    .filter(events::log_index.eq(log_index)),
            )
            .set((
                events::block_number.eq(block_number),
                events::block_timestamp.eq(block_timestamp),
                events::block_hash.eq(&block_hash),
                events::transaction_index.eq(transaction_index),
                events::contract_address.eq(log.address.to_ascii_lowercase()),
                events::topics.eq(normalized_topics(log)),
                events::data.eq(log.data.to_ascii_lowercase()),
            ))
            .execute(conn)
            .context("update event log metadata")?;

            let ledger_entries_updated = diesel::update(
                ledger_entries::table
                    .filter(ledger_entries::source_id.eq(source.id))
                    .filter(ledger_entries::chain_id.eq(source.chain_id))
                    .filter(ledger_entries::transaction_hash.eq(transaction_hash))
                    .filter(ledger_entries::log_index.eq(log_index)),
            )
            .set((
                ledger_entries::block_number.eq(block_number),
                ledger_entries::block_timestamp.eq(block_timestamp),
                ledger_entries::block_hash.eq(block_hash),
                ledger_entries::transaction_index.eq(transaction_index),
            ))
            .execute(conn)
            .context("update ledger entry log metadata")?;

            Ok(LogMetadataUpdate {
                events_updated,
                ledger_entries_updated,
            })
        })
    }

    pub fn advance_checkpoint(
        &self,
        source_id: Uuid,
        processed_block: i64,
        processed_block_hash: &str,
        finalized_block: i64,
    ) -> Result<CheckpointRow> {
        if processed_block < 0 {
            anyhow::bail!("processed block cannot be negative");
        }
        if finalized_block < 0 {
            anyhow::bail!("finalized block cannot be negative");
        }
        if processed_block > finalized_block {
            anyhow::bail!(
                "processed block {processed_block} cannot be greater than finalized block {finalized_block}"
            );
        }

        let mut conn = self.connection()?;
        conn.transaction::<CheckpointRow, anyhow::Error, _>(|conn| {
            let existing = checkpoints::table
                .filter(checkpoints::source_id.eq(source_id))
                .for_update()
                .first::<CheckpointRow>(conn)
                .optional()
                .context("lock checkpoint")?;

            let Some(existing) = existing else {
                return diesel::insert_into(checkpoints::table)
                    .values(NewCheckpointRow {
                        id: Uuid::new_v4(),
                        source_id,
                        processed_block,
                        processed_block_hash: processed_block_hash.to_ascii_lowercase(),
                        finalized_block,
                    })
                    .get_result(conn)
                    .context("insert checkpoint");
            };

            let processed_block_hash = processed_block_hash.to_ascii_lowercase();
            if processed_block == existing.processed_block
                && processed_block_hash != existing.processed_block_hash
            {
                anyhow::bail!(
                    "checkpoint hash mismatch at processed block {processed_block}: stored {}, new {}",
                    existing.processed_block_hash,
                    processed_block_hash
                );
            }

            if processed_block < existing.processed_block
                && finalized_block <= existing.finalized_block
            {
                return Ok(existing);
            }

            let next_processed_block = existing.processed_block.max(processed_block);
            let next_finalized_block = existing.finalized_block.max(finalized_block);
            let next_processed_block_hash = if processed_block > existing.processed_block {
                processed_block_hash
            } else {
                existing.processed_block_hash
            };

            diesel::update(checkpoints::table.filter(checkpoints::id.eq(existing.id)))
                .set((
                    checkpoints::processed_block.eq(next_processed_block),
                    checkpoints::processed_block_hash.eq(next_processed_block_hash),
                    checkpoints::finalized_block.eq(next_finalized_block),
                    checkpoints::updated_at.eq(Utc::now()),
                ))
                .get_result(conn)
                .context("update checkpoint")
        })
    }

    pub fn next_contiguous_checkpoint_target(
        &self,
        source: &SourceRow,
        completed_range: Option<(i64, i64)>,
    ) -> Result<Option<i64>> {
        if let Some((from, to)) = completed_range {
            if from < 0 || to < 0 {
                anyhow::bail!("completed range cannot be negative");
            }
            if from > to {
                anyhow::bail!("completed range {from}..={to} is inverted");
            }
        }

        let mut conn = self.connection()?;
        conn.transaction::<Option<i64>, anyhow::Error, _>(|conn| {
            let checkpoint = checkpoints::table
                .filter(checkpoints::source_id.eq(source.id))
                .for_update()
                .first::<CheckpointRow>(conn)
                .optional()
                .context("lock checkpoint")?;
            let frontier = checkpoint
                .as_ref()
                .map(|row| row.processed_block)
                .unwrap_or(source.start_block - 1);

            let mut ranges = jobs::table
                .filter(jobs::source_id.eq(Some(source.id)))
                .filter(jobs::job_type.eq(JobType::IngestRange.to_string()))
                .filter(jobs::status.eq(JobStatus::Succeeded.to_string()))
                .filter(jobs::from_block.is_not_null())
                .filter(jobs::to_block.is_not_null())
                .filter(jobs::to_block.gt(Some(frontier)))
                .order((jobs::from_block.asc(), jobs::to_block.asc()))
                .select((jobs::from_block, jobs::to_block))
                .load::<(Option<i64>, Option<i64>)>(conn)
                .context("load succeeded ranges for checkpoint")?
                .into_iter()
                .filter_map(|(from, to)| Some((from?, to?)))
                .collect::<Vec<_>>();

            if let Some(range) = completed_range {
                ranges.push(range);
            }
            ranges.sort_unstable();

            let mut target = frontier;
            for (from, to) in ranges {
                if from <= target.saturating_add(1) && to > target {
                    target = to;
                }
            }

            Ok((target > frontier).then_some(target))
        })
    }

    pub fn contract_summary(
        &self,
        chain_id: i64,
        contract_address: &str,
    ) -> Result<Option<ContractSummary>> {
        let mut conn = self.connection()?;
        let Some(source) = self.source_by_contract_conn(&mut conn, chain_id, contract_address)?
        else {
            return Ok(None);
        };

        let event_count = events::table
            .filter(events::source_id.eq(source.id))
            .filter(events::orphaned.eq(false))
            .count()
            .get_result(&mut conn)
            .context("count source events")?;
        let ledger_entry_count = ledger_entries::table
            .filter(ledger_entries::source_id.eq(source.id))
            .filter(ledger_entries::orphaned.eq(false))
            .count()
            .get_result(&mut conn)
            .context("count ledger entries")?;
        let holder_count = self.holder_count_conn(&mut conn, source.id)?;
        let minter_count = self.minter_count_conn(&mut conn, source.id)?;
        let first_indexed_block = self.first_block_conn(&mut conn, source.id)?;
        let last_indexed_block = self.last_block_conn(&mut conn, source.id)?;
        let checkpoint = checkpoints::table
            .filter(checkpoints::source_id.eq(source.id))
            .first::<CheckpointRow>(&mut conn)
            .optional()
            .context("load checkpoint for summary")?;

        Ok(Some(ContractSummary {
            source_id: source.id,
            chain_id: source.chain_id,
            contract_address: source.contract_address,
            token_standard: source.token_standard,
            start_block: source.start_block,
            enabled: source.enabled,
            event_count,
            ledger_entry_count,
            holder_count,
            minter_count,
            first_indexed_block,
            last_indexed_block,
            checkpoint_processed_block: checkpoint.as_ref().map(|row| row.processed_block),
            checkpoint_finalized_block: checkpoint.as_ref().map(|row| row.finalized_block),
        }))
    }

    pub fn holders(
        &self,
        chain_id: i64,
        contract_address: &str,
        limit: i64,
    ) -> Result<Option<Vec<HolderBalance>>> {
        let mut conn = self.connection()?;
        let Some(source) = self.source_by_contract_conn(&mut conn, chain_id, contract_address)?
        else {
            return Ok(None);
        };

        let rows = self.top_holders_conn(&mut conn, source.id, clamp_limit(limit))?;
        Ok(Some(rows.into_iter().map(HolderBalance::from).collect()))
    }

    pub fn minters(
        &self,
        chain_id: i64,
        contract_address: &str,
        limit: i64,
    ) -> Result<Option<Vec<MinterSummary>>> {
        let mut conn = self.connection()?;
        let Some(source) = self.source_by_contract_conn(&mut conn, chain_id, contract_address)?
        else {
            return Ok(None);
        };

        let rows = self.top_minters_conn(&mut conn, source.id, clamp_limit(limit))?;
        Ok(Some(rows))
    }

    pub fn transfers(
        &self,
        chain_id: i64,
        contract_address: &str,
        limit: i64,
    ) -> Result<Option<Vec<LedgerTransfer>>> {
        let page = self.transfers_page(
            chain_id,
            contract_address,
            LedgerQuery {
                limit,
                cursor: None,
                from_block: None,
                to_block: None,
                holder: None,
                token_id: None,
                movement_type: None,
            },
        )?;

        Ok(page.map(|page| page.items))
    }

    pub fn transfers_page(
        &self,
        chain_id: i64,
        contract_address: &str,
        query: LedgerQuery,
    ) -> Result<Option<LedgerPage<LedgerTransfer>>> {
        let mut conn = self.connection()?;
        let Some(source) = self.source_by_contract_conn(&mut conn, chain_id, contract_address)?
        else {
            return Ok(None);
        };

        let page = self
            .ledger_page_conn(&mut conn, source.id, query, LedgerSort::Desc)
            .context("load ledger transfers")?;

        Ok(Some(page))
    }

    pub fn token_path(
        &self,
        chain_id: i64,
        contract_address: &str,
        token_id: &str,
        limit: i64,
    ) -> Result<Option<Vec<LedgerTransfer>>> {
        let page = self.token_path_page(
            chain_id,
            contract_address,
            token_id,
            LedgerQuery {
                limit,
                cursor: None,
                from_block: None,
                to_block: None,
                holder: None,
                token_id: None,
                movement_type: None,
            },
        )?;

        Ok(page.map(|page| page.items))
    }

    pub fn token_path_page(
        &self,
        chain_id: i64,
        contract_address: &str,
        token_id: &str,
        mut query: LedgerQuery,
    ) -> Result<Option<LedgerPage<LedgerTransfer>>> {
        let mut conn = self.connection()?;
        let Some(source) = self.source_by_contract_conn(&mut conn, chain_id, contract_address)?
        else {
            return Ok(None);
        };

        query.token_id = Some(token_id.to_string());
        let page = self
            .ledger_page_conn(&mut conn, source.id, query, LedgerSort::Asc)
            .context("load token path")?;

        Ok(Some(page))
    }

    fn upsert_event(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        log: &RpcLog,
        decoded: &DecodedLog,
        options: PersistDecodedLogsOptions,
    ) -> Result<Uuid> {
        let values = NewEventRow {
            id: Uuid::new_v4(),
            source_id: source.id,
            chain_id: source.chain_id,
            block_number: parse_hex_u64(&log.block_number)? as i64,
            block_timestamp: log.block_timestamp.to_owned(),
            block_hash: log.block_hash.to_ascii_lowercase(),
            transaction_hash: log.transaction_hash.to_ascii_lowercase(),
            transaction_index: parse_optional_hex_i32(log.transaction_index.as_deref())?,
            log_index: parse_hex_u64(&log.log_index)? as i32,
            contract_address: log.address.to_ascii_lowercase(),
            event_name: decoded.event_name.clone(),
            topics: normalized_topics(log),
            data: log.data.to_ascii_lowercase(),
            args: json!({
                "entries": decoded.entries.iter().map(|entry| {
                    json!({
                        "event_name": entry.event_name,
                        "token_standard": entry.token_standard.as_str(),
                        "operator": entry.operator,
                        "from": entry.from,
                        "to": entry.to,
                        "token_id": entry.token_id,
                        "amount": entry.amount,
                        "batch_index": entry.batch_index,
                    })
                }).collect::<Vec<_>>()
            }),
            finalized: true,
            orphaned: false,
        };

        let row = diesel::insert_into(events::table)
            .values(values)
            .on_conflict((
                events::chain_id,
                events::transaction_hash,
                events::log_index,
            ))
            .do_update()
            .set((
                events::block_timestamp.eq(excluded(events::block_timestamp)),
                events::block_hash.eq(excluded(events::block_hash)),
                events::transaction_index.eq(excluded(events::transaction_index)),
                events::contract_address.eq(excluded(events::contract_address)),
                events::event_name.eq(excluded(events::event_name)),
                events::topics.eq(excluded(events::topics)),
                events::data.eq(excluded(events::data)),
                events::args.eq(excluded(events::args)),
                events::finalized.eq(true),
            ))
            .get_result::<super::models::EventRow>(conn)
            .context("upsert event")?;

        if options.restore_orphaned_conflicts && row.orphaned {
            diesel::update(events::table.filter(events::id.eq(row.id)))
                .set(events::orphaned.eq(false))
                .execute(conn)
                .context("restore replayed event")?;
        }

        Ok(row.id)
    }

    fn lock_existing_ledger_entry(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        log: &RpcLog,
        batch_index: i32,
    ) -> Result<Option<LedgerEntryRow>> {
        ledger_entries::table
            .filter(ledger_entries::chain_id.eq(source.chain_id))
            .filter(ledger_entries::transaction_hash.eq(log.transaction_hash.to_ascii_lowercase()))
            .filter(ledger_entries::log_index.eq(parse_hex_u64(&log.log_index)? as i32))
            .filter(ledger_entries::batch_index.eq(batch_index))
            .for_update()
            .first::<LedgerEntryRow>(conn)
            .optional()
            .context("lock existing ledger entry")
    }

    fn upsert_ledger_entry(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        log: &RpcLog,
        event_id: Uuid,
        entry: &crate::infra::evm::decoder::DecodedLedgerEntry,
        options: PersistDecodedLogsOptions,
    ) -> Result<LedgerEntryRow> {
        let amount = entry
            .amount
            .parse::<BigDecimal>()
            .with_context(|| format!("parse amount {}", entry.amount))?;
        let movement_type = movement_type(&entry.from, &entry.to);

        let values = NewLedgerEntryRow {
            id: Uuid::new_v4(),
            event_id,
            source_id: source.id,
            chain_id: source.chain_id,
            contract_address: log.address.to_ascii_lowercase(),
            token_standard: entry.token_standard.as_str().to_string(),
            movement_type: movement_type.to_string(),
            operator_address: entry
                .operator
                .as_ref()
                .map(|value| value.to_ascii_lowercase()),
            from_address: non_zero_address(&entry.from),
            to_address: non_zero_address(&entry.to),
            token_id: entry.token_id.clone(),
            amount,
            batch_index: entry.batch_index,
            block_number: parse_hex_u64(&log.block_number)? as i64,
            block_timestamp: log.block_timestamp.to_owned(),
            block_hash: log.block_hash.to_ascii_lowercase(),
            transaction_hash: log.transaction_hash.to_ascii_lowercase(),
            transaction_index: parse_optional_hex_i32(log.transaction_index.as_deref())?,
            log_index: parse_hex_u64(&log.log_index)? as i32,
            orphaned: false,
        };

        let mut row = diesel::insert_into(ledger_entries::table)
            .values(values)
            .on_conflict((
                ledger_entries::chain_id,
                ledger_entries::transaction_hash,
                ledger_entries::log_index,
                ledger_entries::batch_index,
            ))
            .do_update()
            .set((
                ledger_entries::amount.eq(excluded(ledger_entries::amount)),
                ledger_entries::block_timestamp.eq(excluded(ledger_entries::block_timestamp)),
                ledger_entries::block_hash.eq(excluded(ledger_entries::block_hash)),
                ledger_entries::transaction_index.eq(excluded(ledger_entries::transaction_index)),
            ))
            .get_result::<LedgerEntryRow>(conn)
            .context("upsert ledger entry")?;

        if options.restore_orphaned_conflicts && row.orphaned {
            row = diesel::update(ledger_entries::table.filter(ledger_entries::id.eq(row.id)))
                .set(ledger_entries::orphaned.eq(false))
                .get_result::<LedgerEntryRow>(conn)
                .context("restore replayed ledger entry")?;
        }

        Ok(row)
    }

    fn upsert_transaction_receipt(
        &self,
        conn: &mut PgConnection,
        chain_id: i64,
        receipt: &RpcTransactionReceipt,
    ) -> Result<Uuid> {
        let row = diesel::insert_into(transaction_receipts::table)
            .values(NewTransactionReceiptRow {
                id: Uuid::new_v4(),
                chain_id,
                transaction_hash: receipt.transaction_hash.to_ascii_lowercase(),
                block_number: parse_hex_u64(&receipt.block_number)? as i64,
                block_hash: receipt.block_hash.to_ascii_lowercase(),
                transaction_index: Some(parse_hex_i32(&receipt.transaction_index)?),
                from_address: receipt.from.to_ascii_lowercase(),
                to_address: receipt.to.as_ref().map(|value| value.to_ascii_lowercase()),
                contract_address: receipt
                    .contract_address
                    .as_ref()
                    .map(|value| value.to_ascii_lowercase()),
                status: parse_optional_hex_i32(receipt.status.as_deref())?,
                gas_used: parse_hex_big_decimal(&receipt.gas_used)?,
                cumulative_gas_used: parse_hex_big_decimal(&receipt.cumulative_gas_used)?,
                effective_gas_price: parse_optional_hex_big_decimal(
                    receipt.effective_gas_price.as_deref(),
                )?,
                transaction_type: receipt
                    .transaction_type
                    .as_ref()
                    .map(|value| value.to_ascii_lowercase()),
                raw_receipt: receipt.raw.clone(),
            })
            .on_conflict((
                transaction_receipts::chain_id,
                transaction_receipts::transaction_hash,
            ))
            .do_update()
            .set((
                transaction_receipts::block_number.eq(excluded(transaction_receipts::block_number)),
                transaction_receipts::block_hash.eq(excluded(transaction_receipts::block_hash)),
                transaction_receipts::transaction_index
                    .eq(excluded(transaction_receipts::transaction_index)),
                transaction_receipts::from_address.eq(excluded(transaction_receipts::from_address)),
                transaction_receipts::to_address.eq(excluded(transaction_receipts::to_address)),
                transaction_receipts::contract_address
                    .eq(excluded(transaction_receipts::contract_address)),
                transaction_receipts::status.eq(excluded(transaction_receipts::status)),
                transaction_receipts::gas_used.eq(excluded(transaction_receipts::gas_used)),
                transaction_receipts::cumulative_gas_used
                    .eq(excluded(transaction_receipts::cumulative_gas_used)),
                transaction_receipts::effective_gas_price
                    .eq(excluded(transaction_receipts::effective_gas_price)),
                transaction_receipts::transaction_type
                    .eq(excluded(transaction_receipts::transaction_type)),
                transaction_receipts::raw_receipt.eq(excluded(transaction_receipts::raw_receipt)),
                transaction_receipts::updated_at.eq(Utc::now()),
            ))
            .get_result::<TransactionReceiptRow>(conn)
            .context("upsert transaction receipt")?;

        Ok(row.id)
    }

    fn apply_balance_delta(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        previous: Option<&LedgerEntryRow>,
        current: &LedgerEntryRow,
    ) -> Result<()> {
        let mut deltas = HashMap::<(String, String), BalanceDelta>::new();
        if let Some(previous) = previous
            && !previous.orphaned
        {
            collect_balance_delta(&mut deltas, previous, true);
        }
        if !current.orphaned {
            collect_balance_delta(&mut deltas, current, false);
        }

        let mut deltas = deltas.into_iter().collect::<Vec<_>>();
        deltas.sort_by(
            |((left_holder, left_token), _), ((right_holder, right_token), _)| {
                left_holder
                    .cmp(right_holder)
                    .then_with(|| left_token.cmp(right_token))
            },
        );

        for ((holder_address, token_id), delta) in deltas {
            self.apply_holder_balance_delta(conn, source, holder_address, token_id, delta)?;
        }

        Ok(())
    }

    fn lock_source_for_write_conn(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
    ) -> Result<()> {
        sources::table
            .filter(sources::id.eq(source.id))
            .select(sources::id)
            .for_update()
            .first::<Uuid>(conn)
            .context("lock source ledger write")
            .map(|_| ())
    }

    fn apply_holder_balance_delta(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        holder_address: String,
        token_id: String,
        delta: BalanceDelta,
    ) -> Result<()> {
        let existing = token_balances::table
            .filter(token_balances::source_id.eq(source.id))
            .filter(token_balances::holder_address.eq(&holder_address))
            .filter(token_balances::token_id.eq(&token_id))
            .for_update()
            .first::<TokenBalanceRow>(conn)
            .optional()
            .context("lock token balance")?;

        let current_balance = existing
            .as_ref()
            .map(|row| row.balance.clone())
            .unwrap_or_else(BigDecimal::zero);
        let next_balance = current_balance + delta.balance_delta;
        if next_balance < BigDecimal::zero() {
            anyhow::bail!(
                "balance for holder {holder_address} token_id={token_id} would become negative"
            );
        }

        let first_received_block = min_optional_block(
            existing.as_ref().and_then(|row| row.first_received_block),
            delta.first_received_block,
        );
        let last_moved_block = max_optional_block(
            existing.as_ref().and_then(|row| row.last_moved_block),
            delta.last_moved_block,
        );

        if let Some(existing) = existing {
            diesel::update(token_balances::table.filter(token_balances::id.eq(existing.id)))
                .set((
                    token_balances::balance.eq(next_balance),
                    token_balances::first_received_block.eq(first_received_block),
                    token_balances::last_moved_block.eq(last_moved_block),
                    token_balances::updated_at.eq(Utc::now()),
                ))
                .execute(conn)
                .context("update token balance")?;
            return Ok(());
        }

        if next_balance.is_zero() {
            return Ok(());
        }

        diesel::insert_into(token_balances::table)
            .values(NewTokenBalanceRow {
                id: Uuid::new_v4(),
                source_id: source.id,
                chain_id: source.chain_id,
                contract_address: source.contract_address.clone(),
                token_standard: source.token_standard.clone(),
                holder_address,
                token_id,
                balance: next_balance,
                first_received_block,
                last_moved_block,
            })
            .execute(conn)
            .context("insert token balance")?;

        Ok(())
    }

    fn holder_count_conn(&self, conn: &mut PgConnection, source_id: Uuid) -> Result<i64> {
        token_balances::table
            .filter(token_balances::source_id.eq(source_id))
            .filter(token_balances::balance.gt(BigDecimal::zero()))
            .count()
            .get_result(conn)
            .context("count holders")
    }

    fn minter_count_conn(&self, conn: &mut PgConnection, source_id: Uuid) -> Result<i64> {
        diesel::sql_query(
            "SELECT COUNT(DISTINCT to_address) AS count
             FROM ledger_entries
             WHERE source_id = $1
               AND movement_type = 'mint'
               AND orphaned = false
               AND to_address IS NOT NULL",
        )
        .bind::<SqlUuid, _>(source_id)
        .get_result::<CountRow>(conn)
        .map(|row| row.count)
        .context("count distinct minters")
    }

    fn top_minters_conn(
        &self,
        conn: &mut PgConnection,
        source_id: Uuid,
        limit: i64,
    ) -> Result<Vec<MinterSummary>> {
        diesel::sql_query(
            "WITH minter_blocks AS (
                SELECT
                    to_address AS minter_address,
                    COUNT(*)::bigint AS mint_count,
                    MIN(block_number) AS first_mint_block,
                    MAX(block_number) AS last_mint_block
                FROM ledger_entries
                WHERE source_id = $1
                  AND movement_type = 'mint'
                  AND orphaned = false
                  AND to_address IS NOT NULL
                GROUP BY to_address
                ORDER BY COUNT(*) DESC, MIN(block_number) ASC, to_address ASC
                LIMIT $2
             )
             SELECT
                mb.minter_address AS minter_address,
                mb.mint_count AS mint_count,
                mb.first_mint_block AS first_mint_block,
                first_entry.block_timestamp AS first_mint_timestamp,
                mb.last_mint_block AS last_mint_block,
                last_entry.block_timestamp AS last_mint_timestamp
             FROM minter_blocks mb
             LEFT JOIN LATERAL (
                SELECT block_timestamp
                FROM ledger_entries
                WHERE source_id = $1
                  AND movement_type = 'mint'
                  AND orphaned = false
                  AND to_address = mb.minter_address
                  AND block_number = mb.first_mint_block
                ORDER BY log_index ASC, batch_index ASC
                LIMIT 1
             ) first_entry ON true
             LEFT JOIN LATERAL (
                SELECT block_timestamp
                FROM ledger_entries
                WHERE source_id = $1
                  AND movement_type = 'mint'
                  AND orphaned = false
                  AND to_address = mb.minter_address
                  AND block_number = mb.last_mint_block
                ORDER BY log_index DESC, batch_index DESC
                LIMIT 1
             ) last_entry ON true
             ORDER BY mb.mint_count DESC, mb.first_mint_block ASC, mb.minter_address ASC",
        )
        .bind::<SqlUuid, _>(source_id)
        .bind::<SqlBigInt, _>(limit.clamp(1, 100))
        .load::<MinterSummaryRow>(conn)
        .map(|rows| {
            rows.into_iter()
                .map(|row| MinterSummary {
                    minter_address: row.minter_address,
                    mint_count: row.mint_count,
                    first_mint_block: row.first_mint_block,
                    first_mint_timestamp: row.first_mint_timestamp,
                    last_mint_block: row.last_mint_block,
                    last_mint_timestamp: row.last_mint_timestamp,
                })
                .collect()
        })
        .context("load top minters")
    }

    fn top_holders_conn(
        &self,
        conn: &mut PgConnection,
        source_id: Uuid,
        limit: i64,
    ) -> Result<Vec<TokenBalanceRow>> {
        token_balances::table
            .filter(token_balances::source_id.eq(source_id))
            .filter(token_balances::balance.gt(BigDecimal::zero()))
            .order(token_balances::balance.desc())
            .limit(limit)
            .load(conn)
            .context("load top holders")
    }

    fn source_by_contract_conn(
        &self,
        conn: &mut PgConnection,
        chain_id: i64,
        contract_address: &str,
    ) -> Result<Option<SourceRow>> {
        sources::table
            .filter(sources::chain_id.eq(chain_id))
            .filter(sources::contract_address.eq(contract_address.to_ascii_lowercase()))
            .first(conn)
            .optional()
            .context("load source by contract")
    }

    fn first_block_conn(&self, conn: &mut PgConnection, source_id: Uuid) -> Result<Option<i64>> {
        ledger_entries::table
            .filter(ledger_entries::source_id.eq(source_id))
            .filter(ledger_entries::orphaned.eq(false))
            .select(ledger_entries::block_number)
            .order(ledger_entries::block_number.asc())
            .first(conn)
            .optional()
            .context("load first indexed block")
    }

    fn last_block_conn(&self, conn: &mut PgConnection, source_id: Uuid) -> Result<Option<i64>> {
        ledger_entries::table
            .filter(ledger_entries::source_id.eq(source_id))
            .filter(ledger_entries::orphaned.eq(false))
            .select(ledger_entries::block_number)
            .order(ledger_entries::block_number.desc())
            .first(conn)
            .optional()
            .context("load last indexed block")
    }

    fn ledger_page_conn(
        &self,
        conn: &mut PgConnection,
        source_id: Uuid,
        query: LedgerQuery,
        sort: LedgerSort,
    ) -> Result<LedgerPage<LedgerTransfer>> {
        let mut db_query = ledger_entries::table
            .filter(ledger_entries::source_id.eq(source_id))
            .filter(ledger_entries::orphaned.eq(false))
            .into_boxed();

        if let Some(from_block) = query.from_block {
            db_query = db_query.filter(ledger_entries::block_number.ge(from_block));
        }
        if let Some(to_block) = query.to_block {
            db_query = db_query.filter(ledger_entries::block_number.le(to_block));
        }
        if let Some(holder) = query.holder {
            let holder = holder.to_ascii_lowercase();
            db_query = db_query.filter(
                ledger_entries::from_address
                    .eq(Some(holder.clone()))
                    .or(ledger_entries::to_address.eq(Some(holder))),
            );
        }
        if let Some(token_id) = query.token_id {
            db_query = db_query.filter(ledger_entries::token_id.eq(token_id));
        }
        if let Some(movement_type) = query.movement_type {
            db_query = db_query
                .filter(ledger_entries::movement_type.eq(movement_type.to_ascii_lowercase()));
        }
        if let Some(cursor) = query.cursor {
            db_query = match sort {
                LedgerSort::Asc => db_query.filter(
                    ledger_entries::block_number
                        .gt(cursor.block_number)
                        .or(ledger_entries::block_number
                            .eq(cursor.block_number)
                            .and(ledger_entries::log_index.gt(cursor.log_index)))
                        .or(ledger_entries::block_number
                            .eq(cursor.block_number)
                            .and(ledger_entries::log_index.eq(cursor.log_index))
                            .and(ledger_entries::batch_index.gt(cursor.batch_index))),
                ),
                LedgerSort::Desc => db_query.filter(
                    ledger_entries::block_number
                        .lt(cursor.block_number)
                        .or(ledger_entries::block_number
                            .eq(cursor.block_number)
                            .and(ledger_entries::log_index.lt(cursor.log_index)))
                        .or(ledger_entries::block_number
                            .eq(cursor.block_number)
                            .and(ledger_entries::log_index.eq(cursor.log_index))
                            .and(ledger_entries::batch_index.lt(cursor.batch_index))),
                ),
            };
        }

        db_query = match sort {
            LedgerSort::Asc => db_query.order((
                ledger_entries::block_number.asc(),
                ledger_entries::log_index.asc(),
                ledger_entries::batch_index.asc(),
            )),
            LedgerSort::Desc => db_query.order((
                ledger_entries::block_number.desc(),
                ledger_entries::log_index.desc(),
                ledger_entries::batch_index.desc(),
            )),
        };

        let limit = clamp_limit(query.limit);
        let mut rows = db_query
            .limit(limit + 1)
            .load::<LedgerEntryRow>(conn)
            .context("load ledger page")?;
        let next_cursor = if rows.len() > limit as usize {
            rows.truncate(limit as usize);
            rows.last().map(LedgerCursor::from)
        } else {
            None
        };
        let items = rows.into_iter().map(LedgerTransfer::from).collect();

        Ok(LedgerPage { items, next_cursor })
    }

    fn connection(
        &self,
    ) -> Result<diesel::r2d2::PooledConnection<diesel::r2d2::ConnectionManager<PgConnection>>> {
        self.pool.get().context("get postgres connection")
    }
}

#[derive(Debug, Clone)]
struct BalanceDelta {
    balance_delta: BigDecimal,
    first_received_block: Option<i64>,
    last_moved_block: Option<i64>,
}

impl Default for BalanceDelta {
    fn default() -> Self {
        Self {
            balance_delta: BigDecimal::zero(),
            first_received_block: None,
            last_moved_block: None,
        }
    }
}

fn collect_balance_delta(
    deltas: &mut HashMap<(String, String), BalanceDelta>,
    row: &LedgerEntryRow,
    reverse: bool,
) {
    let amount = if reverse {
        -row.amount.clone()
    } else {
        row.amount.clone()
    };

    if let Some(from) = &row.from_address {
        let delta = deltas
            .entry((from.clone(), row.token_id.clone()))
            .or_default();
        delta.balance_delta -= amount.clone();
        if !reverse {
            delta.last_moved_block =
                max_optional_block(delta.last_moved_block, Some(row.block_number));
        }
    }

    if let Some(to) = &row.to_address {
        let delta = deltas
            .entry((to.clone(), row.token_id.clone()))
            .or_default();
        delta.balance_delta += amount;
        if !reverse {
            delta.first_received_block =
                min_optional_block(delta.first_received_block, Some(row.block_number));
            delta.last_moved_block =
                max_optional_block(delta.last_moved_block, Some(row.block_number));
        }
    }
}

fn min_optional_block(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate),
        (None, None) => None,
    }
}

fn max_optional_block(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate),
        (None, None) => None,
    }
}

impl From<TokenBalanceRow> for HolderBalance {
    fn from(row: TokenBalanceRow) -> Self {
        Self {
            holder_address: row.holder_address,
            token_id: row.token_id,
            balance: row.balance.to_string(),
            first_received_block: row.first_received_block,
            last_moved_block: row.last_moved_block,
        }
    }
}

impl From<LedgerEntryRow> for LedgerTransfer {
    fn from(row: LedgerEntryRow) -> Self {
        Self {
            movement_type: row.movement_type,
            operator_address: row.operator_address,
            from_address: row.from_address,
            to_address: row.to_address,
            token_id: row.token_id,
            amount: row.amount.to_string(),
            block_number: row.block_number,
            block_timestamp: row.block_timestamp,
            transaction_hash: row.transaction_hash,
            transaction_index: row.transaction_index,
            log_index: row.log_index,
            batch_index: row.batch_index,
        }
    }
}

impl From<&LedgerEntryRow> for LedgerCursor {
    fn from(row: &LedgerEntryRow) -> Self {
        Self {
            block_number: row.block_number,
            log_index: row.log_index,
            batch_index: row.batch_index,
        }
    }
}

fn clamp_limit(limit: i64) -> i64 {
    limit.clamp(1, 100)
}

fn non_zero_address(value: &str) -> Option<String> {
    let value = value.to_ascii_lowercase();
    (value != "0x0000000000000000000000000000000000000000").then_some(value)
}

fn movement_type(from: &str, to: &str) -> &'static str {
    match (
        non_zero_address(from).is_some(),
        non_zero_address(to).is_some(),
    ) {
        (false, true) => "mint",
        (true, false) => "burn",
        (true, true) => "transfer",
        (false, false) => "transfer",
    }
}

fn parse_optional_hex_i32(value: Option<&str>) -> Result<Option<i32>> {
    value.map(parse_hex_i32).transpose()
}

fn parse_hex_i32(value: &str) -> Result<i32> {
    let parsed = parse_hex_u64(value)?;
    i32::try_from(parsed).with_context(|| format!("hex value {value} exceeds i32"))
}

fn parse_optional_hex_big_decimal(value: Option<&str>) -> Result<Option<BigDecimal>> {
    value.map(parse_hex_big_decimal).transpose()
}

fn parse_hex_big_decimal(value: &str) -> Result<BigDecimal> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .with_context(|| format!("hex value must start with 0x: {value}"))?;
    if digits.is_empty() {
        anyhow::bail!("hex value {value} is empty");
    }

    let parsed = BigUint::parse_bytes(digits.as_bytes(), 16)
        .with_context(|| format!("parse hex numeric: {value}"))?;
    Ok(BigDecimal::from(BigInt::from(parsed)))
}

fn normalized_topics(log: &RpcLog) -> serde_json::Value {
    json!(
        log.topics
            .iter()
            .map(|topic| topic.to_ascii_lowercase())
            .collect::<Vec<_>>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_big_decimal_requires_hex_prefix() {
        let err = parse_hex_big_decimal("5208").unwrap_err();
        assert!(err.to_string().contains("must start with 0x"));
    }
}
