# Demo Commands

These commands are intended for a local portfolio demo. They assume Postgres is
running and `.env` contains `DATABASE_URL` plus a valid Ethereum RPC URL.

## Setup

```sh
docker compose up -d postgres
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer_rs diesel migration run
```

## Small BAYC Smoke Test

Bored Ape Yacht Club is an ERC-721 contract on Ethereum mainnet:

```txt
0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
```

Start from the initial transfer history when testing holder balances. A random
mid-history slice can contain an outgoing transfer whose earlier incoming
transfer is outside the indexed range; the balance materializer correctly
rejects that as a negative balance.

```sh
cargo run -- scan-contract \
  --contract 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d \
  --standard erc721 \
  --from-block 12287507 \
  --to-block 12287607 \
  --chunk-size 10 \
  --include-transaction-receipts
```

## Durable Backfill Path

```sh
cargo run -- backfill-contract \
  --chain-name ethereum-mainnet \
  --chain-id 1 \
  --finality-confirmations 64 \
  --contract 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d \
  --standard erc721 \
  --from-block 12287507 \
  --to-block 12288507 \
  --range-size 100
```

```sh
RUST_LOG=info LOG_FORMAT=json cargo run -- worker run \
  --worker-id bayc-worker \
  --chain-id 1 \
  --lease-seconds 60 \
  --chunk-size 10 \
  --include-transaction-receipts \
  --metrics-bind 127.0.0.1:9101 \
  --stop-when-idle
```

## API And Metrics

```sh
cargo run -- serve --bind 127.0.0.1:3000
```

```sh
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/metrics
curl http://127.0.0.1:9101/metrics
```

```sh
curl http://127.0.0.1:3000/chains/1/contracts/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/summary
curl "http://127.0.0.1:3000/chains/1/contracts/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/holders?limit=25"
curl "http://127.0.0.1:3000/chains/1/contracts/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/transfers?limit=25"
```

## Request ID Error Envelope

```sh
curl -i \
  -H "x-request-id: portfolio-demo" \
  http://127.0.0.1:3000/chains/1/contracts/not-an-address/summary
```

Expected shape:

```json
{
  "error": {
    "class": "BadRequest",
    "message": "invalid EVM contract address",
    "request_id": "portfolio-demo"
  }
}
```
