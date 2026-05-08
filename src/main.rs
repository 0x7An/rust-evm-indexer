use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use chrono::Duration;
use clap::{Parser, Subcommand};
use indexer_rs::{
    api,
    application::{
        backfill::{BackfillPlan, plan_backfill_jobs},
        ingest::{ingest_source_range, normalize_address, redact_rpc_url, resolve_finalized_range},
    },
    domain::job::JobType,
    infra::{
        evm::{decoder::TokenStandard, rpc::EvmRpcClient},
        postgres::{
            connection::build_pool,
            job_repository::{EnqueueResult, NewJob},
            repositories::PostgresRepositories,
        },
    },
    worker::{IngestWorker, WorkerOutcome},
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
        #[arg(long, env = "EVM_RPC_URL", hide_env_values = true)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
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

    /// Enqueue a durable ingestion job for a contract range.
    EnqueueContract {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long, env = "EVM_RPC_URL", hide_env_values = true)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
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

        /// Maximum attempts before the job is dead-lettered.
        #[arg(long, default_value_t = 5)]
        max_attempts: i32,
    },

    /// Plan a contract backfill as deterministic durable ingestion jobs.
    BackfillContract {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long, env = "EVM_RPC_URL", hide_env_values = true)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
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

        /// Contract address to backfill.
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

        /// Number of recent blocks to plan when from-block is omitted.
        #[arg(long, default_value_t = 5_000)]
        lookback: u64,

        /// Maximum inclusive block span per durable ingestion job.
        #[arg(long, default_value_t = 100)]
        range_size: u64,

        /// Maximum attempts before each job is dead-lettered.
        #[arg(long, default_value_t = 5)]
        max_attempts: i32,
    },

    /// Run the HTTP read API for indexed ledger data.
    Serve {
        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// Socket address to bind.
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: SocketAddr,
    },

    /// Run ingestion workers.
    Worker {
        #[command(subcommand)]
        command: WorkerCommands,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerCommands {
    /// Lease and execute at most one queued ingestion job.
    RunOnce {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long, env = "EVM_RPC_URL", hide_env_values = true)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// Stable worker id recorded in job attempts.
        #[arg(long, default_value = "local-worker")]
        worker_id: String,

        /// Optional chain id to restrict leased jobs.
        #[arg(long)]
        chain_id: Option<i64>,

        /// Lease duration for the job.
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,

        /// Maximum block span per eth_getLogs call.
        #[arg(long, default_value_t = 10)]
        chunk_size: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
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
            let rpc_url = rpc_url_from_args(rpc_url)?;

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
        Commands::EnqueueContract {
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
            max_attempts,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url)?;

            enqueue_contract(
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
                max_attempts,
            )
            .await
        }
        Commands::BackfillContract {
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
            range_size,
            max_attempts,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url)?;

            backfill_contract(
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
                range_size,
                max_attempts,
            )
            .await
        }
        Commands::Serve { database_url, bind } => serve_api(database_url, bind).await,
        Commands::Worker { command } => match command {
            WorkerCommands::RunOnce {
                rpc_url,
                database_url,
                worker_id,
                chain_id,
                lease_seconds,
                chunk_size,
            } => {
                let rpc_url = rpc_url_from_args(rpc_url)?;
                worker_run_once(
                    rpc_url,
                    database_url,
                    worker_id,
                    chain_id,
                    lease_seconds,
                    chunk_size,
                )
                .await
            }
        },
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
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }

    let rpc = EvmRpcClient::new(&rpc_url);
    let range = resolve_finalized_range(
        &rpc,
        from_block.as_deref(),
        &to_block,
        lookback,
        finality_confirmations,
    )
    .await?;

    println!(
        "Scanning {contract} on {chain_name} ({chain_id}) as {} over blocks {from}..={to} in {chunk_size}-block chunks",
        standard.as_str(),
        from = range.from,
        to = range.to,
        chunk_size = chunk_size
    );

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
            range.from as i64,
        )
        .context("ensure source")?;
    let summary = ingest_source_range(
        &rpc,
        repositories.ledger(),
        &source,
        range.from,
        range.to,
        chunk_size,
    )
    .await?;

    print_scan_summary(&summary);

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

async fn enqueue_contract(
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
    max_attempts: i32,
) -> Result<()> {
    let standard = standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {standard}"))?;
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if max_attempts <= 0 {
        bail!("max-attempts must be greater than zero");
    }

    let rpc = EvmRpcClient::new(&rpc_url);
    let range = resolve_finalized_range(
        &rpc,
        from_block.as_deref(),
        &to_block,
        lookback,
        finality_confirmations,
    )
    .await?;
    let code = rpc
        .code_at(&contract, range.to)
        .await
        .with_context(|| format!("fetch contract code at block {}", range.to))?;
    if code == "0x" {
        bail!(
            "no contract code at {contract} on {chain_name} ({chain_id}) at block {}",
            range.to
        );
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
            range.from as i64,
        )
        .context("ensure source")?;

    let idempotency_key = format!("ingest:{}:{}:{}", source.id, range.from, range.to);
    let result = repositories
        .jobs()
        .enqueue(
            NewJob::new(JobType::IngestRange, chain_id, idempotency_key)
                .with_source(source.id)
                .with_range(range.from as i64, range.to as i64)
                .with_max_attempts(max_attempts),
        )
        .context("enqueue ingest job")?;

    match result {
        EnqueueResult::Inserted(job) => {
            println!(
                "Enqueued ingest job {} for {} blocks {}..={}",
                job.id, source.contract_address, range.from, range.to
            );
        }
        EnqueueResult::Existing(job) => {
            println!(
                "Ingest job already exists as {} for {} blocks {}..={}",
                job.id, source.contract_address, range.from, range.to
            );
        }
    }

    Ok(())
}

async fn backfill_contract(
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
    range_size: u64,
    max_attempts: i32,
) -> Result<()> {
    let standard = standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {standard}"))?;
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if range_size == 0 {
        bail!("range-size must be greater than zero");
    }
    if max_attempts <= 0 {
        bail!("max-attempts must be greater than zero");
    }

    let rpc = EvmRpcClient::new(&rpc_url);
    let range = resolve_finalized_range(
        &rpc,
        from_block.as_deref(),
        &to_block,
        lookback,
        finality_confirmations,
    )
    .await?;
    let code = rpc
        .code_at(&contract, range.to)
        .await
        .with_context(|| format!("fetch contract code at block {}", range.to))?;
    if code == "0x" {
        bail!(
            "no contract code at {contract} on {chain_name} ({chain_id}) at block {}",
            range.to
        );
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
            range.from as i64,
        )
        .context("ensure source")?;

    let plan = plan_backfill_jobs(
        &repositories,
        &source,
        range.from,
        range.to,
        range_size,
        max_attempts,
    )?;
    print_backfill_plan(&source.contract_address, &chain_name, chain_id, &plan);

    Ok(())
}

async fn worker_run_once(
    rpc_url: String,
    database_url: String,
    worker_id: String,
    chain_id: Option<i64>,
    lease_seconds: i64,
    chunk_size: u64,
) -> Result<()> {
    if lease_seconds <= 0 {
        bail!("lease-seconds must be greater than zero");
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let mut worker = IngestWorker::new(
        repositories,
        EvmRpcClient::new(rpc_url),
        worker_id,
        Duration::seconds(lease_seconds),
        chunk_size,
    );
    if let Some(chain_id) = chain_id {
        if chain_id <= 0 {
            bail!("chain-id must be greater than zero");
        }
        worker = worker.with_chain_id(chain_id);
    }

    match worker.run_once().await? {
        WorkerOutcome::NoJob => println!("No queued jobs available."),
        WorkerOutcome::Processed { job_id, summary } => {
            println!("Processed ingest job {job_id}.");
            print_scan_summary(&summary);
        }
        WorkerOutcome::Failed {
            job_id,
            status,
            error,
        } => {
            println!("Ingest job {job_id} failed with status {status}: {error}");
            if status == "dead_lettered" {
                bail!("ingest job {job_id} is dead-lettered");
            }
        }
    }

    Ok(())
}

fn print_backfill_plan(contract: &str, chain_name: &str, chain_id: i64, plan: &BackfillPlan) {
    println!(
        "Backfill plan for {contract} on {chain_name} ({chain_id}) requested blocks {from}..={to}.",
        from = plan.requested_from,
        to = plan.requested_to
    );

    match (plan.planned_from, plan.planned_to) {
        (Some(from), Some(to)) => println!(
            "Jobs cover blocks {from}..={to} in {jobs} ranges of up to {range_size} blocks.",
            jobs = plan.total_jobs(),
            range_size = plan.range_size
        ),
        _ => println!("No new jobs needed; the checkpoint already covers the requested range."),
    }

    println!("Inserted jobs: {}", plan.inserted_jobs);
    println!("Existing jobs: {}", plan.existing_jobs);
}

fn print_scan_summary(summary: &indexer_rs::infra::postgres::ledger_repository::ScanSummary) {
    println!("RPC logs decoded: {}", summary.events_seen);
    println!("Events persisted: {}", summary.events_persisted);
    println!(
        "Ledger entries persisted: {}",
        summary.ledger_entries_persisted
    );
    println!("Current holders in indexed slice: {}", summary.holder_count);
    println!("Minters in indexed slice: {}", summary.minter_count);
}

fn rpc_url_from_args(value: Option<String>) -> Result<String> {
    value
        .or_else(|| std::env::var("ETH_RPC_URL").ok())
        .context("missing RPC URL; set --rpc-url, EVM_RPC_URL, or ETH_RPC_URL")
}
