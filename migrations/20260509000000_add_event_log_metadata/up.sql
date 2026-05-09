ALTER TABLE events
    ADD COLUMN block_timestamp TIMESTAMPTZ,
    ADD COLUMN transaction_index INTEGER,
    ADD COLUMN topics JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN data TEXT NOT NULL DEFAULT '0x',
    ADD CONSTRAINT events_transaction_index_check CHECK (
        transaction_index IS NULL OR transaction_index >= 0
    );

ALTER TABLE ledger_entries
    ADD COLUMN block_timestamp TIMESTAMPTZ,
    ADD COLUMN transaction_index INTEGER,
    ADD CONSTRAINT ledger_entries_transaction_index_check CHECK (
        transaction_index IS NULL OR transaction_index >= 0
    );

CREATE INDEX idx_events_source_block_timestamp
    ON events(source_id, block_timestamp);

CREATE INDEX idx_ledger_entries_source_block_timestamp
    ON ledger_entries(source_id, block_timestamp);
