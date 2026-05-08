CREATE TABLE chains (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    chain_id BIGINT NOT NULL UNIQUE,
    rpc_url TEXT NOT NULL,
    finality_confirmations BIGINT NOT NULL DEFAULT 12,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sources (
    id UUID PRIMARY KEY,
    chain_id BIGINT NOT NULL REFERENCES chains(chain_id),
    name TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    token_standard TEXT NOT NULL DEFAULT 'auto',
    event_signatures JSONB NOT NULL,
    start_block BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(chain_id, contract_address),
    CHECK (token_standard IN ('auto', 'erc20', 'erc721', 'erc1155')),
    CHECK (start_block >= 0)
);

CREATE TABLE checkpoints (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL REFERENCES sources(id),
    processed_block BIGINT NOT NULL,
    processed_block_hash TEXT NOT NULL,
    finalized_block BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id),
    CHECK (processed_block >= 0),
    CHECK (finalized_block >= 0),
    CHECK (processed_block <= finalized_block)
);

CREATE TABLE events (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL REFERENCES sources(id),
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    contract_address TEXT NOT NULL,
    event_name TEXT NOT NULL,
    args JSONB NOT NULL,
    finalized BOOLEAN NOT NULL DEFAULT false,
    orphaned BOOLEAN NOT NULL DEFAULT false,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(chain_id, transaction_hash, log_index),
    CHECK (block_number >= 0),
    CHECK (log_index >= 0)
);

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    source_id UUID NOT NULL REFERENCES sources(id),
    chain_id BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    token_standard TEXT NOT NULL,
    movement_type TEXT NOT NULL,
    operator_address TEXT,
    from_address TEXT,
    to_address TEXT,
    token_id TEXT NOT NULL DEFAULT '',
    amount NUMERIC(78,0) NOT NULL,
    batch_index INTEGER NOT NULL DEFAULT 0,
    block_number BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    orphaned BOOLEAN NOT NULL DEFAULT false,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(chain_id, transaction_hash, log_index, batch_index),
    CHECK (token_standard IN ('erc20', 'erc721', 'erc1155')),
    CHECK (movement_type IN ('mint', 'transfer', 'burn')),
    CHECK (amount >= 0),
    CHECK (batch_index >= 0),
    CHECK (block_number >= 0),
    CHECK (log_index >= 0),
    CHECK (from_address IS NOT NULL OR to_address IS NOT NULL)
);

CREATE TABLE token_balances (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL REFERENCES sources(id),
    chain_id BIGINT NOT NULL,
    contract_address TEXT NOT NULL,
    token_standard TEXT NOT NULL,
    holder_address TEXT NOT NULL,
    token_id TEXT NOT NULL DEFAULT '',
    balance NUMERIC(78,0) NOT NULL,
    first_received_block BIGINT,
    last_moved_block BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, holder_address, token_id),
    CHECK (token_standard IN ('erc20', 'erc721', 'erc1155')),
    CHECK (balance >= 0),
    CHECK (first_received_block IS NULL OR first_received_block >= 0),
    CHECK (last_moved_block IS NULL OR last_moved_block >= 0)
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    source_id UUID REFERENCES sources(id),
    chain_id BIGINT NOT NULL,
    from_block BIGINT,
    to_block BIGINT,
    idempotency_key TEXT NOT NULL UNIQUE,
    leased_by TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    error_class TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (job_type IN ('INGEST_RANGE', 'BACKFILL_RANGE', 'REPLAY_RANGE', 'VERIFY_REORG', 'REPAIR_CHECKPOINT')),
    CHECK (status IN ('queued', 'leased', 'running', 'succeeded', 'failed', 'dead_lettered', 'cancelled')),
    CHECK (from_block IS NULL OR from_block >= 0),
    CHECK (to_block IS NULL OR to_block >= 0),
    CHECK (from_block IS NULL OR to_block IS NULL OR from_block <= to_block),
    CHECK (attempts >= 0),
    CHECK (max_attempts > 0)
);

CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error_class TEXT,
    error_message TEXT,
    CHECK (attempt_number > 0),
    CHECK (status IN ('leased', 'running', 'succeeded', 'failed', 'dead_lettered', 'cancelled'))
);

CREATE TABLE reorg_events (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL REFERENCES sources(id),
    chain_id BIGINT NOT NULL,
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    expected_block_hash TEXT,
    actual_block_hash TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replay_job_id UUID REFERENCES jobs(id),
    CHECK (from_block >= 0),
    CHECK (to_block >= 0),
    CHECK (from_block <= to_block)
);

CREATE INDEX idx_sources_chain_enabled ON sources(chain_id, enabled);
CREATE UNIQUE INDEX idx_checkpoints_source_id ON checkpoints(source_id);

CREATE INDEX idx_events_source_block ON events(source_id, block_number);
CREATE INDEX idx_events_chain_block ON events(chain_id, block_number);
CREATE INDEX idx_events_contract_block ON events(contract_address, block_number);
CREATE INDEX idx_events_orphaned_finalized_block ON events(orphaned, finalized, block_number);

CREATE INDEX idx_ledger_entries_source_block ON ledger_entries(source_id, block_number);
CREATE INDEX idx_ledger_entries_source_token_block ON ledger_entries(source_id, token_id, block_number);
CREATE INDEX idx_ledger_entries_source_from_block ON ledger_entries(source_id, from_address, block_number);
CREATE INDEX idx_ledger_entries_source_to_block ON ledger_entries(source_id, to_address, block_number);
CREATE INDEX idx_ledger_entries_source_movement_block ON ledger_entries(source_id, movement_type, block_number);

CREATE INDEX idx_token_balances_source_holder ON token_balances(source_id, holder_address);
CREATE INDEX idx_token_balances_source_token ON token_balances(source_id, token_id);
CREATE INDEX idx_token_balances_current_holders ON token_balances(source_id, balance) WHERE balance > 0;

CREATE INDEX idx_jobs_status_lease_created ON jobs(status, lease_expires_at, created_at);
CREATE INDEX idx_jobs_source_status ON jobs(source_id, status);
CREATE INDEX idx_job_attempts_job_attempt ON job_attempts(job_id, attempt_number);
CREATE INDEX idx_reorg_events_source_detected ON reorg_events(source_id, detected_at);
