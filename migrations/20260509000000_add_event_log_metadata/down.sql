DROP INDEX idx_ledger_entries_source_block_timestamp;
DROP INDEX idx_events_source_block_timestamp;

ALTER TABLE ledger_entries
    DROP CONSTRAINT ledger_entries_transaction_index_check,
    DROP COLUMN transaction_index,
    DROP COLUMN block_timestamp;

ALTER TABLE events
    DROP CONSTRAINT events_transaction_index_check,
    DROP COLUMN data,
    DROP COLUMN topics,
    DROP COLUMN transaction_index,
    DROP COLUMN block_timestamp;
