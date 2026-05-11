ALTER TABLE sources DROP COLUMN IF EXISTS event_signatures;
ALTER TABLE sources ALTER COLUMN token_standard DROP DEFAULT;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sources WHERE token_standard = 'auto') THEN
        RAISE EXCEPTION 'cannot migrate: sources with token_standard = ''auto'' exist; resolve token standards before applying this migration';
    END IF;
END $$;

ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_token_standard_check;
ALTER TABLE sources
    ADD CONSTRAINT sources_token_standard_check
    CHECK (token_standard IN ('erc20', 'erc721', 'erc1155'));
