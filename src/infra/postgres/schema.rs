// @generated automatically by Diesel CLI.

diesel::table! {
    chains (id) {
        id -> Int8,
        name -> Text,
        chain_id -> Int8,
        rpc_url -> Text,
        finality_confirmations -> Int8,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    checkpoints (id) {
        id -> Uuid,
        source_id -> Uuid,
        processed_block -> Int8,
        processed_block_hash -> Text,
        finalized_block -> Int8,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    events (id) {
        id -> Uuid,
        source_id -> Uuid,
        chain_id -> Int8,
        block_number -> Int8,
        block_timestamp -> Nullable<Timestamptz>,
        block_hash -> Text,
        transaction_hash -> Text,
        transaction_index -> Nullable<Int4>,
        log_index -> Int4,
        contract_address -> Text,
        event_name -> Text,
        topics -> Jsonb,
        data -> Text,
        args -> Jsonb,
        finalized -> Bool,
        orphaned -> Bool,
        inserted_at -> Timestamptz,
    }
}

diesel::table! {
    job_attempts (id) {
        id -> Uuid,
        job_id -> Uuid,
        attempt_number -> Int4,
        worker_id -> Text,
        started_at -> Timestamptz,
        finished_at -> Nullable<Timestamptz>,
        status -> Text,
        error_class -> Nullable<Text>,
        error_message -> Nullable<Text>,
    }
}

diesel::table! {
    jobs (id) {
        id -> Uuid,
        job_type -> Text,
        status -> Text,
        source_id -> Nullable<Uuid>,
        chain_id -> Int8,
        from_block -> Nullable<Int8>,
        to_block -> Nullable<Int8>,
        idempotency_key -> Text,
        leased_by -> Nullable<Text>,
        lease_expires_at -> Nullable<Timestamptz>,
        attempts -> Int4,
        max_attempts -> Int4,
        error_class -> Nullable<Text>,
        error_message -> Nullable<Text>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    ledger_entries (id) {
        id -> Uuid,
        event_id -> Uuid,
        source_id -> Uuid,
        chain_id -> Int8,
        contract_address -> Text,
        token_standard -> Text,
        movement_type -> Text,
        operator_address -> Nullable<Text>,
        from_address -> Nullable<Text>,
        to_address -> Nullable<Text>,
        token_id -> Text,
        amount -> Numeric,
        batch_index -> Int4,
        block_number -> Int8,
        block_timestamp -> Nullable<Timestamptz>,
        block_hash -> Text,
        transaction_hash -> Text,
        transaction_index -> Nullable<Int4>,
        log_index -> Int4,
        orphaned -> Bool,
        inserted_at -> Timestamptz,
    }
}

diesel::table! {
    reorg_events (id) {
        id -> Uuid,
        source_id -> Uuid,
        chain_id -> Int8,
        from_block -> Int8,
        to_block -> Int8,
        expected_block_hash -> Nullable<Text>,
        actual_block_hash -> Nullable<Text>,
        detected_at -> Timestamptz,
        replay_job_id -> Nullable<Uuid>,
    }
}

diesel::table! {
    sources (id) {
        id -> Uuid,
        chain_id -> Int8,
        name -> Text,
        contract_address -> Text,
        token_standard -> Text,
        event_signatures -> Jsonb,
        start_block -> Int8,
        enabled -> Bool,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    token_balances (id) {
        id -> Uuid,
        source_id -> Uuid,
        chain_id -> Int8,
        contract_address -> Text,
        token_standard -> Text,
        holder_address -> Text,
        token_id -> Text,
        balance -> Numeric,
        first_received_block -> Nullable<Int8>,
        last_moved_block -> Nullable<Int8>,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    transaction_receipts (id) {
        id -> Uuid,
        chain_id -> Int8,
        transaction_hash -> Text,
        block_number -> Int8,
        block_hash -> Text,
        transaction_index -> Nullable<Int4>,
        from_address -> Text,
        to_address -> Nullable<Text>,
        contract_address -> Nullable<Text>,
        status -> Nullable<Int4>,
        gas_used -> Numeric,
        cumulative_gas_used -> Numeric,
        effective_gas_price -> Nullable<Numeric>,
        transaction_type -> Nullable<Text>,
        raw_receipt -> Jsonb,
        inserted_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::joinable!(checkpoints -> sources (source_id));
diesel::joinable!(events -> sources (source_id));
diesel::joinable!(job_attempts -> jobs (job_id));
diesel::joinable!(jobs -> sources (source_id));
diesel::joinable!(ledger_entries -> events (event_id));
diesel::joinable!(ledger_entries -> sources (source_id));
diesel::joinable!(reorg_events -> jobs (replay_job_id));
diesel::joinable!(reorg_events -> sources (source_id));
diesel::joinable!(token_balances -> sources (source_id));

diesel::allow_tables_to_appear_in_same_query!(
    chains,
    checkpoints,
    events,
    job_attempts,
    jobs,
    ledger_entries,
    reorg_events,
    sources,
    token_balances,
    transaction_receipts,
);
