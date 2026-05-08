# indexer-rs

`indexer-rs` is a Rust EVM token ledger indexer for application-owned blockchain data infrastructure.

The goal is to add a token contract and build a durable ledger registry for it:

- transfers
- mints
- burns
- current holders
- balances
- NFT and ERC-1155 provenance paths

V1 targets standard token ledger events for ERC-20, ERC-721, and ERC-1155 contracts. It is not a generic ABI indexer, mapping runtime, Graph Node clone, or decentralized indexing marketplace.

## Scope

V1 focuses on a single-chain, Postgres-backed indexer with conservative finalized-block ingestion.

Planned stack:

- Rust
- Tokio
- Diesel
- Postgres
- Axum
- EVM RPC adapter behind an application port
- Durable Postgres jobs with leases
- Replay/backfill
- Reorg verification
- Prometheus metrics
- CLI doctor/status commands

## Non-Goals

This project does not aim to support the following in V1:

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

## Architecture

The codebase is organized around clean architecture boundaries:

```txt
api / cli / worker
        |
        v
application
        |
        v
domain

infra implements application ports
```

Layer responsibilities:

- `domain`: pure business types and invariants.
- `application`: use cases and ports.
- `infra`: Diesel/Postgres, EVM RPC, telemetry, and config implementations.
- `api`: thin Axum adapter.
- `worker`: thin job execution adapter.
- `cli`: thin operator adapter.
- `config`: typed runtime configuration.

Infrastructure types must not leak into `domain` or `application`.

## Development Checkpoints

This repository is intended to grow through small, reviewable checkpoints:

1. Repo skeleton and README intent
2. Domain model and tests
3. Diesel schema and migrations
4. Job leasing
5. Token event decoder with fixtures
6. Ingest worker
7. Ledger queries and API
8. Replay and backfill
9. Observability and doctor CLI

Each checkpoint should keep the project buildable, include focused validation, and produce a clear commit/PR boundary.

## Current Status

Checkpoint 4 is complete. The project currently contains the public Rust skeleton, pure domain model, initial Diesel/Postgres schema, local database setup, and durable job leasing repository tests. Real RPC, API, worker execution, and runtime migration commands will be introduced in later checkpoints.

## Development

```sh
cargo fmt
cargo check
cargo test
```

`cargo test` includes Postgres-backed job repository integration tests. Start
the local database and run migrations before the full test suite:

```sh
docker compose up -d postgres
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs diesel migration run
cargo test
```

## Local Database

Start Postgres with Docker Compose:

```sh
docker compose up -d postgres
```

Run migrations against the local database:

```sh
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs diesel migration run
```

The local credentials are for development only. Use real secrets outside local
development.
