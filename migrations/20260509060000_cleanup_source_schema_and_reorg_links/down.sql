ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_token_standard_check;
ALTER TABLE sources ALTER COLUMN token_standard SET DEFAULT 'auto';
ALTER TABLE sources
    ADD CONSTRAINT sources_token_standard_check
    CHECK (token_standard IN ('auto', 'erc20', 'erc721', 'erc1155'));

ALTER TABLE sources
    ADD COLUMN IF NOT EXISTS event_signatures JSONB NOT NULL DEFAULT '[]'::jsonb;
