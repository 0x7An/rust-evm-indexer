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
5. Live token event scanner
6. Ingest worker
7. Ledger queries and API
8. Replay and backfill
9. Observability and doctor CLI

Each checkpoint should keep the project buildable, include focused validation, and produce a clear commit/PR boundary.

## Current Status

Checkpoint 5 is complete. The project currently contains the public Rust skeleton, pure domain model, initial Diesel/Postgres schema, local database setup, durable job leasing repository tests, and a live `scan-contract` CLI that fetches ERC-20, ERC-721, or ERC-1155 transfer logs from an EVM RPC endpoint and persists a ledger slice. API, worker execution, replay, and observability will be introduced in later checkpoints.

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

## Live Contract Scan

Set `EVM_RPC_URL` locally instead of committing provider credentials:

```sh
export DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs
export EVM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/your-api-key
```

Scan an Ethereum ERC-721 contract:

```sh
cargo run -- scan-contract \
  --contract 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d \
  --standard erc721 \
  --lookback 5000 \
  --chunk-size 10
```

Scan a Polygon ERC-1155 contract:

```sh
export EVM_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/your-api-key

cargo run -- scan-contract \
  --chain-name polygon-mainnet \
  --chain-id 137 \
  --finality-confirmations 256 \
  --contract 0x2953399124f0cbb46d2cbacd8a89cf0599974963 \
  --standard erc1155 \
  --lookback 5000 \
  --chunk-size 10
```

For repeatable checks, use explicit finalized block ranges:

```sh
cargo run -- scan-contract \
  --chain-name polygon-mainnet \
  --chain-id 137 \
  --finality-confirmations 256 \
  --contract 0x2953399124f0cbb46d2cbacd8a89cf0599974963 \
  --standard erc1155 \
  --from-block 0x528e895 \
  --to-block 0x528e895 \
  --chunk-size 10
```

Ethereum ERC-721 repeatable example:

```sh
cargo run -- scan-contract \
  --contract 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d \
  --standard erc721 \
  --from-block 19700000 \
  --to-block 19701000 \
  --chunk-size 10
```

The default chunk size is 10 blocks so the command works with RPC providers that tightly limit `eth_getLogs` ranges. Increase `--chunk-size` when your provider plan allows wider log queries. The default `latest` end block resolves to `head - finality_confirmations`, and the command verifies that the contract has bytecode on the selected chain before printing decoded log counts, persisted ledger entries, minters, and current holders for the indexed block slice.
