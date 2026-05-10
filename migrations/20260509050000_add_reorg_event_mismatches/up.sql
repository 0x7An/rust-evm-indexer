ALTER TABLE reorg_events
    ADD COLUMN IF NOT EXISTS mismatches JSONB NOT NULL DEFAULT '[]'::jsonb;
