ALTER TABLE reorg_events
    ADD COLUMN mismatches JSONB NOT NULL DEFAULT '[]'::jsonb;
