use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use indexer_rs::{
    api,
    infra::{
        evm::{
            decoder::{TokenStandard, decode_log},
            rpc::EvmRpcClient,
        },
        postgres::{connection::build_pool, repositories::PostgresRepositories},
    },
};

#[derive(Debug, Parser)]
#[command(name = "indexer")]
#[command(about = "EVM token ledger indexer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Fetch standard token logs for a contract and persist a ledger slice.
    ScanContract {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long, env = "EVM_RPC_URL")]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Chain display name to persist with the source.
        #[arg(long, default_value = "ethereum-mainnet")]
        chain_name: String,

        /// EVM chain id to persist with the source.
        #[arg(long, default_value_t = 1)]
        chain_id: i64,

        /// Finality confirmations to persist with the chain metadata.
        #[arg(long, default_value_t = 64)]
        finality_confirmations: i64,

        /// Contract address to scan.
        #[arg(long)]
        contract: String,

        /// Token standard: erc20, erc721, or erc1155.
        #[arg(long)]
        standard: String,

        /// Inclusive start block. Defaults to to-block minus lookback.
        #[arg(long)]
        from_block: Option<String>,

        /// Inclusive end block. Use a decimal block, hex block, or finalized latest.
        #[arg(long, default_value = "latest")]
        to_block: String,

        /// Number of recent blocks to scan when from-block is omitted.
        #[arg(long, default_value_t = 5_000)]
        lookback: u64,

        /// Maximum block span per eth_getLogs call.
        #[arg(long, default_value_t = 10)]
        chunk_size: u64,
    },

    /// Run the HTTP read API for indexed ledger data.
    Serve {
        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Socket address to bind.
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ScanContract {
            rpc_url,
            database_url,
            chain_name,
            chain_id,
            finality_confirmations,
            contract,
            standard,
            from_block,
            to_block,
            lookback,
            chunk_size,
        } => {
            let rpc_url = rpc_url
                .or_else(|| std::env::var("ETH_RPC_URL").ok())
                .context("missing RPC URL; set --rpc-url, EVM_RPC_URL, or ETH_RPC_URL")?;

            scan_contract(
                rpc_url,
                database_url,
                chain_name,
                chain_id,
                finality_confirmations,
                contract,
                standard,
                from_block,
                to_block,
                lookback,
                chunk_size,
            )
            .await
        }
        Commands::Serve { database_url, bind } => serve_api(database_url, bind).await,
    }
}

async fn serve_api(database_url: String, bind: SocketAddr) -> Result<()> {
    let pool = build_pool(&database_url).context("build postgres pool")?;
    let app = api::router(pool);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind API listener on {bind}"))?;

    println!("API listening on http://{bind}");
    axum::serve(listener, app).await.context("serve HTTP API")
}

async fn scan_contract(
    rpc_url: String,
    database_url: String,
    chain_name: String,
    chain_id: i64,
    finality_confirmations: i64,
    contract: String,
    standard: String,
    from_block: Option<String>,
    to_block: String,
    lookback: u64,
    chunk_size: u64,
) -> Result<()> {
    let standard = standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {standard}"))?;
    let contract = normalize_address(&contract)?;
    if chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if finality_confirmations < 0 {
        bail!("finality-confirmations cannot be negative");
    }

    let rpc = EvmRpcClient::new(&rpc_url);

    let head = rpc.block_number().await.context("fetch head block")?;
    let finalized_head = head.saturating_sub(finality_confirmations as u64);
    let to = parse_block_arg(&to_block, finalized_head)?;
    let from = match from_block {
        Some(value) => parse_block_arg(&value, finalized_head)?,
        None => to.saturating_sub(lookback),
    };

    if from > to {
        bail!("from-block {from} cannot be greater than to-block {to}");
    }

    let code = rpc
        .code_at(&contract, to)
        .await
        .with_context(|| format!("fetch contract code at block {to}"))?;
    if code == "0x" {
        bail!("no contract code at {contract} on {chain_name} ({chain_id}) at block {to}");
    }

    println!(
        "Scanning {contract} on {chain_name} ({chain_id}) as {} over blocks {from}..={to} in {}-block chunks",
        standard.as_str(),
        chunk_size
    );

    let logs = fetch_logs_in_chunks(&rpc, &contract, standard, from, to, chunk_size).await?;

    let mut decoded = Vec::new();
    for log in logs {
        if let Some(decoded_log) = decode_log(&log, standard).context("decode log")? {
            decoded.push((log, decoded_log));
        }
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    repositories
        .ledger()
        .ensure_chain(
            &chain_name,
            chain_id,
            &redact_rpc_url(&rpc_url),
            finality_confirmations,
        )
        .context("ensure chain")?;
    let source = repositories
        .ledger()
        .ensure_source(
            chain_id,
            &format!("{}-{contract}", standard.as_str()),
            &contract,
            standard,
            from as i64,
        )
        .context("ensure source")?;
    let summary = repositories
        .ledger()
        .persist_decoded_logs(&source, &decoded)
        .context("persist ledger")?;

    println!("RPC logs decoded: {}", summary.events_seen);
    println!("Events persisted: {}", summary.events_persisted);
    println!(
        "Ledger entries persisted: {}",
        summary.ledger_entries_persisted
    );
    println!("Current holders in indexed slice: {}", summary.holder_count);
    println!("Minters in indexed slice: {}", summary.minter_count);

    if summary.top_holders.is_empty() {
        println!("No holders found in this block range.");
    } else {
        println!("Top holders in indexed slice:");
        for holder in summary.top_holders {
            println!(
                "- {} token_id={} balance={}",
                holder.holder_address, holder.token_id, holder.balance
            );
        }
    }

    Ok(())
}

async fn fetch_logs_in_chunks(
    rpc: &EvmRpcClient,
    contract: &str,
    standard: TokenStandard,
    from: u64,
    to: u64,
    chunk_size: u64,
) -> Result<Vec<indexer_rs::infra::evm::decoder::RpcLog>> {
    let mut logs = Vec::new();
    let mut chunk_from = from;

    while chunk_from <= to {
        let chunk_to = chunk_from.saturating_add(chunk_size - 1).min(to);
        let chunk_logs = rpc
            .logs(contract, standard, chunk_from, chunk_to)
            .await
            .with_context(|| format!("fetch contract logs {chunk_from}..={chunk_to}"))?;
        logs.extend(chunk_logs);

        if chunk_to == u64::MAX {
            break;
        }
        chunk_from = chunk_to + 1;
    }

    Ok(logs)
}

fn parse_block_arg(value: &str, latest: u64) -> Result<u64> {
    if value == "latest" {
        return Ok(latest);
    }

    if let Some(hex) = value.strip_prefix("0x") {
        return u64::from_str_radix(hex, 16).with_context(|| format!("parse hex block {value}"));
    }

    value
        .parse::<u64>()
        .with_context(|| format!("parse decimal block {value}"))
}

fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if normalized.len() != 42
        || !normalized.starts_with("0x")
        || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        bail!("invalid EVM address: {value}");
    }
    Ok(normalized)
}

fn redact_rpc_url(value: &str) -> String {
    for marker in ["/v2/", "/v3/"] {
        if let Some(index) = value.find(marker) {
            let end = index + marker.len();
            return format!("{}<redacted>", &value[..end]);
        }
    }

    "<configured>".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_provider_key_from_rpc_url() {
        assert_eq!(
            redact_rpc_url("https://eth-mainnet.g.alchemy.com/v2/example-key"),
            "https://eth-mainnet.g.alchemy.com/v2/<redacted>"
        );
    }
}
