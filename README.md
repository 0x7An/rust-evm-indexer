# indexer-rs

A Rust EVM token ledger indexer for application-owned blockchain data infrastructure.

Add a token contract and `indexer-rs` builds a durable ledger registry for it: transfers, mints, burns, current holders, balances, and NFT / ERC-1155 provenance paths. It targets ERC-20, ERC-721, and ERC-1155 contracts on a single EVM chain, ingests only finalized blocks, and persists everything in Postgres.

It is intentionally not a generic ABI indexer, a mapping runtime, a Graph Node clone, or a decentralized indexing marketplace.

## Features

**Indexing**

- ERC-20, ERC-721, and ERC-1155 standard transfer decoding (`Transfer`, `TransferSingle`, `TransferBatch`).
- Conservative finalization: never advances past `head - finality_confirmations`.
- Idempotent event and ledger inserts keyed by `(chain, tx hash, log index, batch index)`.
- Incremental holder balance materialization, not full rebuilds.
- Token provenance paths queryable by `(contract, token_id)`.

**Durable jobs**

- Postgres-backed job queue with leases, retries, dead-lettering, and per-attempt audit rows.
- `INGEST_RANGE` and `REPLAY_RANGE` job types; `FOR UPDATE SKIP LOCKED` leasing.
- One-shot, drain-until-idle, and daemon worker modes.
- Schema-enforced uniqueness of `(source_id, job_type, from_block, to_block)`.

**Reorg & replay**

- `verify-reorg` compares stored block hashes against canonical chain hashes.
- Detected mismatches are coalesced into `reorg_events` rows with per-block hash detail.
- `enqueue-replay` orphans the affected range, reverses balance effects, and re-ingests canonical logs. Replay jobs do not advance the normal checkpoint.

**Operability**

- `doctor` preflight: DB, migrations, RPC, chain head, source config, checkpoint, job backlog, dead letters.
- Prometheus metrics exposed by the API and by each worker process (separate `--metrics-bind`).
- Structured `tracing` with JSON output and per-request request IDs in API error envelopes.
- Repair commands for legacy event metadata and missing transaction receipts.

**Architecture**

- Clean hexagonal layering. `application/` defines ports (`ChainRpc`, `LedgerIngestRepository`, `BackfillRepository`, `ReorgRepository`). `infra/` implements them with Diesel/Postgres and an EVM JSON-RPC client.
- Application-layer tests run against fakes — no Postgres or live RPC required.

## Non-Goals

- arbitrary ABI indexing
- custom user-defined mappings
- WASM execution
- GraphQL query engine
- Graph Node dependency
- staking, allocations, TAP/RAV, POI disputes, or query-fee markets
- IPFS manifests
- multi-chain orchestration
- Solana
- dashboard UI

## Quickstart

Requires Rust (stable), Docker, and an EVM JSON-RPC endpoint.

```sh
# 1. Start Postgres.
docker compose up -d postgres

# 2. Apply migrations.
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs \
  diesel migration run

# 3. Configure RPC and DB credentials.
cp .env.example .env
$EDITOR .env

# 4. Scan a contract slice end-to-end.
cargo run -- scan-contract \
  --contract 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d \
  --standard erc721 \
  --from-block 12287507 \
  --to-block 12288507

# 5. Start the read API.
cargo run -- serve --bind 127.0.0.1:3000

# Get the ledger summary.
curl http://127.0.0.1:3000/chains/1/contracts/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/summary
```

For copy-pasteable end-to-end flows including Polygon ERC-1155 and replay/reorg paths, see [docs/demo.md](docs/demo.md).

## CLI

| Command | Purpose |
|---|---|
| `scan-contract` | Synchronous index of a finalized range. Useful for ad-hoc slices and demos. |
| `enqueue-contract` | Insert a single durable `INGEST_RANGE` job for the selected range. |
| `backfill-contract` | Plan a multi-job backfill, splitting a range into deterministic chunks. |
| `enqueue-replay` | Insert a `REPLAY_RANGE` job over an already-indexed range; links matching `reorg_events`. |
| `verify-reorg` | Compare stored block hashes to canonical chain hashes, record mismatches. |
| `backfill-event-metadata` | Repair `block_timestamp`/`topics`/`data` on legacy event rows. |
| `backfill-transaction-receipts` | Fetch and persist `eth_getTransactionReceipt` for ledger rows missing receipts. |
| `worker run-once` / `worker run` | Lease and execute durable jobs. `run` polls until idle or forever; `--metrics-bind` exposes worker-local metrics. |
| `jobs status` | Group jobs by status, optionally filtered by chain, source, or type. |
| `doctor` | Operator preflight against a chain + contract. Non-zero exit on failure. |
| `serve` | Run the Axum read API. |

Use `--standard auto` on `scan-contract`, `enqueue-contract`, and `backfill-contract` to detect the standard from log shape. Detection probes the range in small chunks and persists the resolved standard.

The default `eth_getLogs` chunk size is 10 blocks to fit conservative RPC plans. Increase with `--chunk-size` when your provider allows wider queries.

## HTTP API

The read API runs on whatever address you pass to `serve` (default `127.0.0.1:3000`).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness check. |
| `GET` | `/metrics` | Prometheus exposition for the API process. |
| `GET` | `/chains/:chain_id/contracts/:address/summary` | Counts, checkpoint, lag for an indexed source. |
| `GET` | `/chains/:chain_id/contracts/:address/holders` | Current holders with balance > 0. |
| `GET` | `/chains/:chain_id/contracts/:address/minters` | Distinct minter addresses with mint counts. |
| `GET` | `/chains/:chain_id/contracts/:address/transfers` | Cursor-paginated ledger transfers; filters: `from_block`, `to_block`, `token_id`, `movement_type`. |
| `GET` | `/chains/:chain_id/contracts/:address/tokens/:token_id/path` | Chronological provenance for a single NFT or ERC-1155 token id. |

Pagination uses opaque `cursor=...` values returned alongside `items` as `next_cursor`. Errors carry a stable shape with a `request_id` for log correlation:

```json
{ "error": "from_block cannot be greater than to_block", "request_id": "..." }
```

## Observability

Prometheus metrics follow the spec's twelve series with conforming labels:

```
indexer_head_block{chain_id}
indexer_finalized_block{chain_id}
indexer_processed_block{chain_id, source_id}
indexer_source_lag_blocks{chain_id, source_id}
indexer_job_backlog{job_type, status}
indexer_failed_jobs_total{job_type, error_class}
indexer_dead_letter_jobs_total{job_type, error_class}
indexer_events_processed_total{chain_id, source_id, event_name}
indexer_rpc_errors_total{chain_id, error_class}
indexer_db_write_duration_ms{operation}
indexer_reorgs_detected_total{chain_id, source_id}
indexer_worker_lease_failures_total{worker_id}
```

Each running process (API, every worker) should be scraped independently. The API exposes `/metrics` on its bind address; workers expose `/metrics` on `--metrics-bind`.

Structured logs use `tracing`:

```sh
RUST_LOG=info LOG_FORMAT=json cargo run -- worker run --metrics-bind 127.0.0.1:9101
```

`LOG_FORMAT=json` emits JSON Lines for production log pipelines; omit it for human-readable local output. Job execution and RPC calls emit spans carrying `job_id`, `source_id`, `chain_id`, and (in the API) `request_id`.

## Architecture

```
api / cli / worker
        |
        v
application (ports + use cases)
        |
        v
domain (invariants)
        ^
        |
infra (Postgres, EVM RPC, telemetry)
```

- `domain` holds pure business types and invariants. No Diesel, no RPC, no Tokio.
- `application` owns use cases and traits. Functions are generic over `impl ChainRpc`, `impl LedgerIngestRepository`, etc. — testable against fakes.
- `infra` implements the application ports with Diesel/Postgres and an EVM JSON-RPC client.
- `api`, `cli`, `worker` are thin adapters that compose the layers above.

More detail in [docs/architecture.md](docs/architecture.md).

## Development

```sh
cargo fmt
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
```

The repository ships fakes for the application ports, so the focused suites run without infrastructure:

```sh
cargo test --test application --test metrics
```

The full suite needs Postgres. Start it via Docker Compose and run migrations first:

```sh
docker compose up -d postgres
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs diesel migration run
cargo test
```

CI runs the same `fmt`, `check`, `test`, `clippy` gates against a Postgres service on every push and PR. Supply-chain checks are configured in `deny.toml`:

```sh
cargo audit
cargo deny check
```

## Security

See [SECURITY.md](SECURITY.md). Do not commit `.env` files or real RPC provider keys. `.env` is gitignored; `.env.example` is the template.

## License

MIT. See [LICENSE](LICENSE).
