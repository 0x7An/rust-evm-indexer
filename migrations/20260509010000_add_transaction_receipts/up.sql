CREATE TABLE transaction_receipts (
    id UUID PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    transaction_index INTEGER,
    from_address TEXT NOT NULL,
    to_address TEXT,
    contract_address TEXT,
    status INTEGER,
    gas_used NUMERIC(78,0) NOT NULL,
    cumulative_gas_used NUMERIC(78,0) NOT NULL,
    effective_gas_price NUMERIC(78,0),
    transaction_type TEXT,
    raw_receipt JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(chain_id, transaction_hash),
    CHECK (block_number >= 0),
    CHECK (transaction_index IS NULL OR transaction_index >= 0),
    CHECK (status IS NULL OR status IN (0, 1)),
    CHECK (gas_used >= 0),
    CHECK (cumulative_gas_used >= 0),
    CHECK (effective_gas_price IS NULL OR effective_gas_price >= 0)
);

CREATE INDEX idx_transaction_receipts_chain_block
    ON transaction_receipts(chain_id, block_number);

CREATE INDEX idx_transaction_receipts_chain_from
    ON transaction_receipts(chain_id, from_address);
