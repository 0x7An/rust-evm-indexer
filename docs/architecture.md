# Architecture

`indexer-rs` is a Rust EVM token ledger indexer for ERC-20, ERC-721, and
ERC-1155 contracts. It indexes finalized contract history into Postgres so an
application can query minters, holders, balances, transfers, provenance paths,
transaction receipts, checkpoints, and reorg repair state.

## Runtime Shape

The project has three operator-facing entry points:

- CLI commands for scans, backfill planning, replay planning, repair, status,
  and health checks.
- A worker process that leases durable Postgres jobs and executes ingestion or
  replay ranges.
- An Axum API that serves read models and process-local Prometheus metrics.

The layering is:

```txt
api / cli / worker
        |
        v
application ports and use cases
        |
        v
domain invariants
        ^
        |
infra adapters: Postgres, EVM RPC, telemetry
```

`application` depends on ports. `infra` implements those ports with Diesel,
Postgres, EVM JSON-RPC, Prometheus, and tracing.

## Postgres Model

The central tables are:

- `chains`: chain identity, redacted RPC metadata, and finality settings.
- `sources`: one indexed contract per chain, with the detected concrete token
  standard.
- `jobs`: durable work queue for `INGEST_RANGE`, `REPLAY_RANGE`,
  `VERIFY_REORG`, and `REPAIR_CHECKPOINT` job records.
- `job_attempts`: lease and retry audit trail for worker execution.
- `events`: decoded source logs with raw topics/data and block metadata.
- `ledger_entries`: normalized mint, burn, and transfer rows.
- `token_balances`: materialized current holder balances per source/token.
- `transaction_receipts`: optional transaction sender/status/gas provenance.
- `checkpoints`: contiguous source progress against finalized block history.
- `reorg_events`: detected block hash mismatches and replay linkage.

## Job Lifecycle

Backfill planning splits a requested finalized block range into deterministic
`INGEST_RANGE` jobs. Jobs use idempotency keys plus a source/type/range unique
constraint so repeated planning does not duplicate work.

The worker:

1. Leases an eligible queued job.
2. Marks the attempt as running.
3. Fetches logs from the configured chain RPC.
4. Decodes supported token events.
5. Persists events, ledger rows, holder balances, and optional receipts.
6. Marks the job succeeded or failed.
7. Advances checkpoints only across contiguous succeeded ingest ranges.

Interrupted jobs are returned to the queue. Failed jobs retry until
`max_attempts`, then become dead-lettered.

## Reorg And Replay

Normal ingestion targets finalized ranges, but the project still keeps block
hashes for verification. `verify-reorg` compares indexed block hashes and
checkpoint hashes with canonical RPC block hashes. Mismatches are recorded as
contiguous ranges with per-block hash details.

Replay is explicit. After reviewing a mismatch, an operator enqueues a
`REPLAY_RANGE` job. Replay marks existing rows in the range as orphaned, reverses
their holder balance effects, then ingests the canonical logs for the same
range. Replay jobs preserve audit history and do not advance the normal ingest
checkpoint.

## Observability

The API and worker expose process-local Prometheus metrics at `/metrics`.
Prometheus should scrape every running process.

Structured logs use `tracing`:

- `RUST_LOG` controls filtering and defaults to `info`.
- `LOG_FORMAT=json` emits JSON Lines.
- pretty text is used by default for local development.

Important spans include `job_lease`, `rpc_fetch`, `event_decode`, `db_insert`,
and `checkpoint_update`. Job failure logs include job id, job type, error class,
attempt counts, and the retry decision.
