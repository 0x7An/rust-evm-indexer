use std::collections::HashMap;

use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, Zero};
use chrono::{DateTime, Utc};
use diesel::{
    PgConnection, QueryableByName,
    prelude::*,
    sql_types::{BigInt, Text as SqlText, Uuid as SqlUuid},
    upsert::excluded,
};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    domain::job::JobStatus,
    infra::evm::{
        decoder::{DecodedLog, RpcLog, TokenStandard, parse_hex_u64},
        rpc::RpcTransactionReceipt,
    },
};

use super::{
    connection::PgPool,
    models::{
        ChainRow, CheckpointRow, LedgerEntryRow, NewChainRow, NewCheckpointRow, NewEventRow,
        NewLedgerEntryRow, NewSourceRow, NewTokenBalanceRow, NewTransactionReceiptRow, SourceRow,
        TokenBalanceRow, TransactionReceiptRow,
    },
    schema::{
        chains, checkpoints, events, jobs, ledger_entries, sources, token_balances,
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

#[derive(Debug, QueryableByName)]
struct MissingReceiptHashRow {
    #[diesel(sql_type = SqlText)]
    transaction_hash: String,
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

    pub fn ensure_source(
        &self,
        chain_id: i64,
        name: &str,
        contract_address: &str,
        standard: TokenStandard,
        start_block: i64,
    ) -> Result<SourceRow> {
        let mut conn = self.connection()?;
        diesel::insert_into(sources::table)
            .values(NewSourceRow {
                id: Uuid::new_v4(),
                chain_id,
                name: name.to_string(),
                contract_address: contract_address.to_ascii_lowercase(),
                token_standard: standard.as_str().to_string(),
                event_signatures: match standard {
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
        let mut conn = self.connection()?;
        conn.transaction::<ScanSummary, anyhow::Error, _>(|conn| {
            let mut events_persisted = 0;
            let mut ledger_entries_persisted = 0;

            for (log, decoded) in logs {
                let event_id = self.upsert_event(conn, source, log, decoded)?;
                events_persisted += 1;

                for entry in &decoded.entries {
                    self.upsert_ledger_entry(conn, source, log, event_id, entry)?;
                    ledger_entries_persisted += 1;
                }
            }

            self.rebuild_balances(conn, source)?;
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
        .bind::<BigInt, _>(limit.clamp(1, 10_000))
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

            if processed_block < existing.processed_block
                && finalized_block <= existing.finalized_block
            {
                return Ok(existing);
            }

            let next_processed_block = existing.processed_block.max(processed_block);
            let next_finalized_block = existing.finalized_block.max(finalized_block);
            let next_processed_block_hash = if processed_block >= existing.processed_block {
                processed_block_hash.to_ascii_lowercase()
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
                .filter(jobs::status.eq(JobStatus::Succeeded.to_string()))
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

        let rows = ledger_entries::table
            .filter(ledger_entries::source_id.eq(source.id))
            .filter(ledger_entries::movement_type.eq("mint"))
            .filter(ledger_entries::orphaned.eq(false))
            .order((
                ledger_entries::block_number.asc(),
                ledger_entries::log_index.asc(),
                ledger_entries::batch_index.asc(),
            ))
            .load::<LedgerEntryRow>(&mut conn)
            .context("load minter ledger entries")?;

        let mut minters: HashMap<String, MinterAccumulator> = HashMap::new();
        for row in rows {
            let Some(address) = row.to_address else {
                continue;
            };
            let entry = minters.entry(address).or_insert_with(|| MinterAccumulator {
                mint_count: 0,
                first_mint_block: row.block_number,
                first_mint_timestamp: row.block_timestamp,
                last_mint_block: row.block_number,
                last_mint_timestamp: row.block_timestamp,
            });
            entry.mint_count += 1;
            if row.block_number < entry.first_mint_block {
                entry.first_mint_block = row.block_number;
                entry.first_mint_timestamp = row.block_timestamp;
            }
            if row.block_number > entry.last_mint_block {
                entry.last_mint_block = row.block_number;
                entry.last_mint_timestamp = row.block_timestamp;
            }
        }

        let mut summaries = minters
            .into_iter()
            .map(|(minter_address, value)| MinterSummary {
                minter_address,
                mint_count: value.mint_count,
                first_mint_block: value.first_mint_block,
                first_mint_timestamp: value.first_mint_timestamp,
                last_mint_block: value.last_mint_block,
                last_mint_timestamp: value.last_mint_timestamp,
            })
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            right
                .mint_count
                .cmp(&left.mint_count)
                .then_with(|| left.first_mint_block.cmp(&right.first_mint_block))
                .then_with(|| left.minter_address.cmp(&right.minter_address))
        });
        summaries.truncate(clamp_limit(limit) as usize);

        Ok(Some(summaries))
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
    ) -> Result<Uuid> {
        let row = diesel::insert_into(events::table)
            .values(NewEventRow {
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
            })
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
                events::orphaned.eq(false),
            ))
            .get_result::<super::models::EventRow>(conn)
            .context("upsert event")?;

        Ok(row.id)
    }

    fn upsert_ledger_entry(
        &self,
        conn: &mut PgConnection,
        source: &SourceRow,
        log: &RpcLog,
        event_id: Uuid,
        entry: &crate::infra::evm::decoder::DecodedLedgerEntry,
    ) -> Result<Uuid> {
        let amount = entry
            .amount
            .parse::<BigDecimal>()
            .with_context(|| format!("parse amount {}", entry.amount))?;
        let movement_type = movement_type(&entry.from, &entry.to);

        let row = diesel::insert_into(ledger_entries::table)
            .values(NewLedgerEntryRow {
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
            })
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
                ledger_entries::orphaned.eq(false),
            ))
            .get_result::<LedgerEntryRow>(conn)
            .context("upsert ledger entry")?;

        Ok(row.id)
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

    fn rebuild_balances(&self, conn: &mut PgConnection, source: &SourceRow) -> Result<()> {
        let rows = ledger_entries::table
            .filter(ledger_entries::source_id.eq(source.id))
            .filter(ledger_entries::orphaned.eq(false))
            .order((
                ledger_entries::block_number.asc(),
                ledger_entries::log_index.asc(),
                ledger_entries::batch_index.asc(),
            ))
            .load::<LedgerEntryRow>(conn)
            .context("load ledger entries for balance rebuild")?;

        diesel::delete(token_balances::table.filter(token_balances::source_id.eq(source.id)))
            .execute(conn)
            .context("delete previous token balances")?;

        let mut balances: HashMap<(String, String), BalanceAccumulator> = HashMap::new();

        for row in rows {
            if let Some(from) = row.from_address {
                let key = (from, row.token_id.clone());
                let entry = balances.entry(key).or_default();
                entry.balance -= row.amount.clone();
                entry.last_moved_block = Some(row.block_number);
            }

            if let Some(to) = row.to_address {
                let key = (to, row.token_id.clone());
                let entry = balances.entry(key).or_default();
                entry.balance += row.amount.clone();
                entry.first_received_block.get_or_insert(row.block_number);
                entry.last_moved_block = Some(row.block_number);
            }
        }

        let rows = balances
            .into_iter()
            .filter(|(_, value)| value.balance > BigDecimal::zero())
            .map(|((holder_address, token_id), value)| NewTokenBalanceRow {
                id: Uuid::new_v4(),
                source_id: source.id,
                chain_id: source.chain_id,
                contract_address: source.contract_address.clone(),
                token_standard: source.token_standard.clone(),
                holder_address,
                token_id,
                balance: value.balance,
                first_received_block: value.first_received_block,
                last_moved_block: value.last_moved_block,
            })
            .collect::<Vec<_>>();

        for chunk in rows.chunks(TOKEN_BALANCE_INSERT_CHUNK_SIZE) {
            diesel::insert_into(token_balances::table)
                .values(chunk)
                .execute(conn)
                .context("insert rebuilt token balances")?;
        }

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
        let minters = ledger_entries::table
            .filter(ledger_entries::source_id.eq(source_id))
            .filter(ledger_entries::movement_type.eq("mint"))
            .filter(ledger_entries::orphaned.eq(false))
            .select(ledger_entries::to_address)
            .distinct()
            .load::<Option<String>>(conn)
            .context("load distinct minters")?;

        Ok(minters.into_iter().flatten().count() as i64)
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
struct BalanceAccumulator {
    balance: BigDecimal,
    first_received_block: Option<i64>,
    last_moved_block: Option<i64>,
}

impl Default for BalanceAccumulator {
    fn default() -> Self {
        Self {
            balance: BigDecimal::zero(),
            first_received_block: None,
            last_moved_block: None,
        }
    }
}

#[derive(Debug, Clone)]
struct MinterAccumulator {
    mint_count: i64,
    first_mint_block: i64,
    first_mint_timestamp: Option<DateTime<Utc>>,
    last_mint_block: i64,
    last_mint_timestamp: Option<DateTime<Utc>>,
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
    Ok(BigDecimal::from(parse_hex_u64(value)?))
}

fn normalized_topics(log: &RpcLog) -> serde_json::Value {
    json!(
        log.topics
            .iter()
            .map(|topic| topic.to_ascii_lowercase())
            .collect::<Vec<_>>()
    )
}

const TOKEN_BALANCE_INSERT_CHUNK_SIZE: usize = 1_000;
