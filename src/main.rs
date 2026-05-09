use std::{collections::HashMap, net::SocketAddr, time::Duration as StdDuration};

use anyhow::{Context, Result, bail};
use chrono::Duration;
use clap::{Parser, Subcommand};
use indexer_rs::{
    api,
    application::{
        backfill::{BackfillPlan, plan_backfill_jobs},
        ingest::{
            IngestOptions, detect_token_standard, ingest_source_range, normalize_address,
            redact_rpc_url, resolve_finalized_range, validate_contract_code_at_boundaries,
        },
        reorg::verify_source_reorgs,
    },
    domain::job::{JobStatus, JobType},
    infra::{
        evm::{decoder::TokenStandard, rpc::EvmRpcClient},
        postgres::{
            connection::build_pool,
            job_repository::{EnqueueResult, JobStatusCount, NewJob},
            repositories::PostgresRepositories,
        },
    },
    worker::{IngestWorker, WorkerOutcome},
};

const AUTO_DETECT_CHUNK_SIZE: u64 = 2_000;

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
        #[arg(long)]
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

        /// Token standard: auto, erc20, erc721, or erc1155.
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

        /// Fetch and persist eth_getTransactionReceipt data for each unique transaction.
        #[arg(long)]
        include_transaction_receipts: bool,
    },

    /// Enqueue a durable ingestion job for a contract range.
    EnqueueContract {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
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

        /// Token standard: auto, erc20, erc721, or erc1155.
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
        #[arg(long)]
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

        /// Token standard: auto, erc20, erc721, or erc1155.
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

    /// Enqueue a durable replay job for an already indexed contract range.
    EnqueueReplay {
        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// EVM chain id for the indexed source.
        #[arg(long, default_value_t = 1)]
        chain_id: i64,

        /// Contract address to replay.
        #[arg(long)]
        contract: String,

        /// Inclusive replay start block.
        #[arg(long)]
        from_block: u64,

        /// Inclusive replay end block.
        #[arg(long)]
        to_block: u64,

        /// Maximum attempts before the replay job is dead-lettered.
        #[arg(long, default_value_t = 5)]
        max_attempts: i32,
    },

    /// Repair event block timestamps and raw log metadata for already indexed rows.
    BackfillEventMetadata {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// EVM chain id for the indexed source.
        #[arg(long, default_value_t = 1)]
        chain_id: i64,

        /// Contract address with indexed events to repair.
        #[arg(long)]
        contract: String,

        /// Maximum distinct event blocks to repair in one run.
        #[arg(long, default_value_t = 100)]
        limit_blocks: i64,
    },

    /// Fetch receipts for indexed ledger transactions that do not have one yet.
    BackfillTransactionReceipts {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// EVM chain id for the indexed source.
        #[arg(long, default_value_t = 1)]
        chain_id: i64,

        /// Contract address with indexed ledger rows to repair.
        #[arg(long)]
        contract: String,

        /// Maximum missing transaction receipts to fetch in one run.
        #[arg(long, default_value_t = 1_000)]
        limit: i64,

        /// Number of fetched receipts to persist per database transaction.
        #[arg(long, default_value_t = 100)]
        persist_batch_size: usize,
    },

    /// Verify indexed block hashes against canonical RPC block hashes.
    VerifyReorg {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
        rpc_url: Option<String>,

        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// EVM chain id for the indexed source.
        #[arg(long, default_value_t = 1)]
        chain_id: i64,

        /// Contract address with indexed rows to verify.
        #[arg(long)]
        contract: String,

        /// Inclusive start block to verify.
        #[arg(long)]
        from_block: u64,

        /// Inclusive end block to verify.
        #[arg(long)]
        to_block: u64,
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

    /// Inspect durable ingestion jobs.
    Jobs {
        #[command(subcommand)]
        command: JobCommands,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerCommands {
    /// Lease and execute at most one queued ingestion job.
    RunOnce {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
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

        /// Fetch and persist eth_getTransactionReceipt data for each unique transaction.
        #[arg(long)]
        include_transaction_receipts: bool,
    },

    /// Continuously lease and execute queued ingestion jobs.
    Run {
        /// EVM JSON-RPC URL. Prefer EVM_RPC_URL for local use.
        #[arg(long)]
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

        /// Lease duration for each job.
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,

        /// Maximum block span per eth_getLogs call.
        #[arg(long, default_value_t = 10)]
        chunk_size: u64,

        /// Stop after this many job attempts.
        #[arg(long)]
        max_jobs: Option<usize>,

        /// Stop once the queue is empty instead of polling forever.
        #[arg(long)]
        stop_when_idle: bool,

        /// Sleep duration between empty queue polls.
        #[arg(long, default_value_t = 1_000)]
        idle_sleep_ms: u64,

        /// Fetch and persist eth_getTransactionReceipt data for each unique transaction.
        #[arg(long)]
        include_transaction_receipts: bool,
    },
}

#[derive(Debug, Subcommand)]
enum JobCommands {
    /// Print durable job counts grouped by status.
    Status {
        /// Postgres database URL. Prefer DATABASE_URL for local use.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,

        /// Optional chain id filter.
        #[arg(long)]
        chain_id: Option<i64>,

        /// Optional contract source filter. Requires --chain-id.
        #[arg(long)]
        contract: Option<String>,

        /// Optional job type filter, such as INGEST_RANGE.
        #[arg(long)]
        job_type: Option<String>,
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
            include_transaction_receipts,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;

            scan_contract(ScanContractArgs {
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
                include_transaction_receipts,
            })
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
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;

            enqueue_contract(EnqueueContractArgs {
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
            })
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
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;

            backfill_contract(BackfillContractArgs {
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
            })
            .await
        }
        Commands::EnqueueReplay {
            database_url,
            chain_id,
            contract,
            from_block,
            to_block,
            max_attempts,
        } => enqueue_replay(
            database_url,
            chain_id,
            contract,
            from_block,
            to_block,
            max_attempts,
        ),
        Commands::BackfillEventMetadata {
            rpc_url,
            database_url,
            chain_id,
            contract,
            limit_blocks,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;

            backfill_event_metadata(rpc_url, database_url, chain_id, contract, limit_blocks).await
        }
        Commands::BackfillTransactionReceipts {
            rpc_url,
            database_url,
            chain_id,
            contract,
            limit,
            persist_batch_size,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;

            backfill_transaction_receipts(
                rpc_url,
                database_url,
                chain_id,
                contract,
                limit,
                persist_batch_size,
            )
            .await
        }
        Commands::VerifyReorg {
            rpc_url,
            database_url,
            chain_id,
            contract,
            from_block,
            to_block,
        } => {
            let rpc_url = rpc_url_from_args(rpc_url, Some(chain_id))?;
            verify_reorg(
                rpc_url,
                database_url,
                chain_id,
                contract,
                from_block,
                to_block,
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
                include_transaction_receipts,
            } => {
                let rpc_url = rpc_url_from_args(rpc_url, chain_id)?;
                worker_run_once(
                    rpc_url,
                    database_url,
                    worker_id,
                    chain_id,
                    lease_seconds,
                    chunk_size,
                    include_transaction_receipts,
                )
                .await
            }
            WorkerCommands::Run {
                rpc_url,
                database_url,
                worker_id,
                chain_id,
                lease_seconds,
                chunk_size,
                max_jobs,
                stop_when_idle,
                idle_sleep_ms,
                include_transaction_receipts,
            } => {
                let rpc_url = rpc_url_from_args(rpc_url, chain_id)?;
                worker_run(
                    rpc_url,
                    database_url,
                    worker_id,
                    chain_id,
                    lease_seconds,
                    chunk_size,
                    max_jobs,
                    stop_when_idle,
                    idle_sleep_ms,
                    include_transaction_receipts,
                )
                .await
            }
        },
        Commands::Jobs { command } => match command {
            JobCommands::Status {
                database_url,
                chain_id,
                contract,
                job_type,
            } => jobs_status(database_url, chain_id, contract, job_type),
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

struct ScanContractArgs {
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
    include_transaction_receipts: bool,
}

async fn resolve_requested_standard(
    rpc: &EvmRpcClient,
    contract: &str,
    requested: TokenStandard,
    from: u64,
    to: u64,
    chunk_size: u64,
) -> Result<TokenStandard> {
    if !requested.is_auto() {
        return Ok(requested);
    }

    let detected = detect_token_standard(rpc, contract, from, to, chunk_size, true)
        .await
        .context("auto-detect token standard")?;
    println!(
        "Detected token standard for {contract} over blocks {from}..={to}: {}",
        detected.as_str()
    );
    Ok(detected)
}

async fn scan_contract(args: ScanContractArgs) -> Result<()> {
    let ScanContractArgs {
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
        include_transaction_receipts,
    } = args;

    let requested_standard = standard
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
    let chain_label = format!("{chain_name} ({chain_id})");
    validate_contract_code_at_boundaries(&rpc, &contract, &chain_label, range.from, range.to)
        .await?;
    let standard = resolve_requested_standard(
        &rpc,
        &contract,
        requested_standard,
        range.from,
        range.to,
        chunk_size,
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
        IngestOptions {
            include_transaction_receipts,
            progress: true,
        },
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

struct EnqueueContractArgs {
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
}

async fn enqueue_contract(args: EnqueueContractArgs) -> Result<()> {
    let EnqueueContractArgs {
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
    } = args;

    let requested_standard = standard
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
    let chain_label = format!("{chain_name} ({chain_id})");
    validate_contract_code_at_boundaries(&rpc, &contract, &chain_label, range.from, range.to)
        .await?;
    let standard = resolve_requested_standard(
        &rpc,
        &contract,
        requested_standard,
        range.from,
        range.to,
        AUTO_DETECT_CHUNK_SIZE,
    )
    .await?;

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

struct BackfillContractArgs {
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
}

async fn backfill_contract(args: BackfillContractArgs) -> Result<()> {
    let BackfillContractArgs {
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
    } = args;

    let requested_standard = standard
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
    let chain_label = format!("{chain_name} ({chain_id})");
    validate_contract_code_at_boundaries(&rpc, &contract, &chain_label, range.from, range.to)
        .await?;
    let standard = resolve_requested_standard(
        &rpc,
        &contract,
        requested_standard,
        range.from,
        range.to,
        range_size,
    )
    .await?;

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

fn enqueue_replay(
    database_url: String,
    chain_id: i64,
    contract: String,
    from_block: u64,
    to_block: u64,
    max_attempts: i32,
) -> Result<()> {
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if from_block > to_block {
        bail!("from-block {from_block} cannot be greater than to-block {to_block}");
    }
    if max_attempts <= 0 {
        bail!("max-attempts must be greater than zero");
    }
    let from_i64 =
        i64::try_from(from_block).context("from-block exceeds postgres bigint storage")?;
    let to_i64 = i64::try_from(to_block).context("to-block exceeds postgres bigint storage")?;

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let source = repositories
        .ledger()
        .source_by_contract(chain_id, &contract)
        .context("load source by contract")?
        .context("contract source not found")?;
    let idempotency_key = format!("replay:{}:{}:{}", source.id, from_block, to_block);
    let result = repositories
        .jobs()
        .enqueue(
            NewJob::new(JobType::ReplayRange, chain_id, idempotency_key)
                .with_source(source.id)
                .with_range(from_i64, to_i64)
                .with_max_attempts(max_attempts),
        )
        .context("enqueue replay job")?;

    match result {
        EnqueueResult::Inserted(job) => {
            println!(
                "Enqueued replay job {} for {} blocks {}..={}",
                job.id, source.contract_address, from_block, to_block
            );
        }
        EnqueueResult::Existing(job) => {
            println!(
                "Replay job already exists as {} for {} blocks {}..={}",
                job.id, source.contract_address, from_block, to_block
            );
        }
    }

    Ok(())
}

async fn backfill_event_metadata(
    rpc_url: String,
    database_url: String,
    chain_id: i64,
    contract: String,
    limit_blocks: i64,
) -> Result<()> {
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if limit_blocks <= 0 {
        bail!("limit-blocks must be greater than zero");
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let source = repositories
        .ledger()
        .source_by_contract(chain_id, &contract)
        .context("load source by contract")?
        .context("contract source not found")?;
    let standard = source
        .token_standard
        .parse::<TokenStandard>()
        .with_context(|| format!("parse token standard {}", source.token_standard))?;
    let blocks = repositories
        .ledger()
        .event_blocks_missing_metadata(source.id, limit_blocks)
        .context("load blocks missing metadata")?;

    if blocks.is_empty() {
        println!("No indexed event rows are missing block/log metadata for {contract}.");
        return Ok(());
    }

    let rpc = EvmRpcClient::new(&rpc_url);
    let mut rpc_logs_seen = 0usize;
    let mut events_updated = 0usize;
    let mut ledger_entries_updated = 0usize;

    for block in &blocks {
        let block = u64::try_from(*block).context("indexed block number cannot be negative")?;
        let timestamp = rpc
            .block_timestamp(block)
            .await
            .with_context(|| format!("fetch block timestamp for block {block}"))?;
        let mut logs = rpc
            .logs(&source.contract_address, standard, block, block)
            .await
            .with_context(|| format!("fetch contract logs for block {block}"))?;

        rpc_logs_seen += logs.len();
        for log in &mut logs {
            log.block_timestamp = Some(timestamp.to_owned());
            let update = repositories
                .ledger()
                .update_log_metadata(&source, log)
                .context("update indexed log metadata")?;
            events_updated += update.events_updated;
            ledger_entries_updated += update.ledger_entries_updated;
        }
    }

    println!(
        "Backfilled event metadata for {contract} on chain {chain_id} across {blocks} blocks.",
        blocks = blocks.len()
    );
    println!("RPC logs inspected: {rpc_logs_seen}");
    println!("Event rows updated: {events_updated}");
    println!("Ledger rows updated: {ledger_entries_updated}");

    Ok(())
}

async fn backfill_transaction_receipts(
    rpc_url: String,
    database_url: String,
    chain_id: i64,
    contract: String,
    limit: i64,
    persist_batch_size: usize,
) -> Result<()> {
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if limit <= 0 {
        bail!("limit must be greater than zero");
    }
    if persist_batch_size == 0 {
        bail!("persist-batch-size must be greater than zero");
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let source = repositories
        .ledger()
        .source_by_contract(chain_id, &contract)
        .context("load source by contract")?
        .context("contract source not found")?;
    let transaction_hashes = repositories
        .ledger()
        .transaction_hashes_missing_receipts(source.id, limit)
        .context("load missing transaction receipt hashes")?;

    if transaction_hashes.is_empty() {
        println!("No indexed ledger transactions are missing receipts for {contract}.");
        return Ok(());
    }

    println!(
        "Backfilling transaction receipts for {contract} on chain {chain_id}: {} missing transactions selected.",
        transaction_hashes.len()
    );

    let rpc = EvmRpcClient::new(&rpc_url);
    let mut fetched = 0usize;
    let mut persisted = 0usize;
    let mut batch = Vec::with_capacity(persist_batch_size.min(transaction_hashes.len()));
    let total = transaction_hashes.len();

    for transaction_hash in transaction_hashes {
        batch.push(
            rpc.transaction_receipt(&transaction_hash)
                .await
                .with_context(|| format!("fetch transaction receipt {transaction_hash}"))?,
        );
        fetched += 1;

        if fetched == 1 || fetched == total || fetched.is_multiple_of(100) {
            println!("Fetched transaction receipt {fetched}/{total}.");
        }

        if batch.len() >= persist_batch_size {
            persisted += repositories
                .ledger()
                .persist_transaction_receipts(chain_id, &batch)
                .context("persist transaction receipt batch")?;
            println!("Persisted transaction receipts: {persisted}/{total}.");
            batch.clear();
        }
    }

    if !batch.is_empty() {
        persisted += repositories
            .ledger()
            .persist_transaction_receipts(chain_id, &batch)
            .context("persist final transaction receipt batch")?;
        println!("Persisted transaction receipts: {persisted}/{total}.");
    }

    println!("Transaction receipt backfill complete.");
    println!("Receipts fetched: {fetched}");
    println!("Receipts persisted: {persisted}");

    Ok(())
}

async fn verify_reorg(
    rpc_url: String,
    database_url: String,
    chain_id: i64,
    contract: String,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let contract = normalize_address(&contract)?;
    if chain_id <= 0 {
        bail!("chain-id must be greater than zero");
    }
    if from_block > to_block {
        bail!("from-block {from_block} cannot be greater than to-block {to_block}");
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let source = repositories
        .ledger()
        .source_by_contract(chain_id, &contract)
        .context("load source by contract")?
        .context("contract source not found")?;
    let rpc = EvmRpcClient::new(&rpc_url);
    let verification =
        verify_source_reorgs(&rpc, repositories.ledger(), &source, from_block, to_block).await?;

    println!(
        "Verified {} indexed/checkpointed block hashes for {contract} on chain {chain_id} over blocks {from_block}..={to_block}.",
        verification.checked_blocks
    );
    if verification.mismatches.is_empty() {
        println!("No reorg mismatches detected.");
        return Ok(());
    }

    println!(
        "Detected {} reorg mismatch(es) and persisted them to reorg_events:",
        verification.mismatches.len()
    );
    for mismatch in verification.mismatches {
        println!(
            "- block {} expected {} actual {}",
            mismatch.block_number, mismatch.expected_block_hash, mismatch.actual_block_hash
        );
    }

    Ok(())
}

async fn worker_run_once(
    rpc_url: String,
    database_url: String,
    worker_id: String,
    chain_id: Option<i64>,
    lease_seconds: i64,
    chunk_size: u64,
    include_transaction_receipts: bool,
) -> Result<()> {
    if lease_seconds <= 0 {
        bail!("lease-seconds must be greater than zero");
    }

    let worker = build_worker(
        rpc_url,
        database_url,
        worker_id,
        chain_id,
        lease_seconds,
        chunk_size,
        include_transaction_receipts,
    )?;

    let outcome = worker.run_once_until_shutdown(shutdown_signal()).await?;
    print_worker_outcome(&outcome);
    if outcome.is_terminal_failure()
        && let WorkerOutcome::Failed { job_id, .. } = outcome
    {
        bail!("ingest job {job_id} is dead-lettered");
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn worker_run(
    rpc_url: String,
    database_url: String,
    worker_id: String,
    chain_id: Option<i64>,
    lease_seconds: i64,
    chunk_size: u64,
    max_jobs: Option<usize>,
    stop_when_idle: bool,
    idle_sleep_ms: u64,
    include_transaction_receipts: bool,
) -> Result<()> {
    if max_jobs == Some(0) {
        bail!("max-jobs must be greater than zero when provided");
    }

    let worker = build_worker(
        rpc_url,
        database_url,
        worker_id,
        chain_id,
        lease_seconds,
        chunk_size,
        include_transaction_receipts,
    )?;
    let mut attempted_jobs = 0usize;
    let mut printed_idle = false;

    loop {
        if max_jobs.is_some_and(|max_jobs| attempted_jobs >= max_jobs) {
            println!("Reached max job attempts: {attempted_jobs}");
            return Ok(());
        }

        let outcome = worker.run_once_until_shutdown(shutdown_signal()).await?;
        match &outcome {
            WorkerOutcome::NoJob => {
                if stop_when_idle {
                    println!(
                        "No queued jobs available. Worker stopped after {attempted_jobs} job attempts."
                    );
                    return Ok(());
                }
                if !printed_idle {
                    println!("No queued jobs available. Polling every {idle_sleep_ms}ms.");
                    printed_idle = true;
                }
                tokio::select! {
                    _ = tokio::time::sleep(StdDuration::from_millis(idle_sleep_ms)) => {}
                    _ = shutdown_signal() => {
                        println!("Worker shutdown requested while idle.");
                        return Ok(());
                    }
                }
            }
            WorkerOutcome::Processed { .. } | WorkerOutcome::Failed { .. } => {
                attempted_jobs += 1;
                printed_idle = false;
                print_worker_outcome(&outcome);
            }
            WorkerOutcome::Interrupted { .. } => {
                print_worker_outcome(&outcome);
                return Ok(());
            }
        }
    }
}

fn build_worker(
    rpc_url: String,
    database_url: String,
    worker_id: String,
    chain_id: Option<i64>,
    lease_seconds: i64,
    chunk_size: u64,
    include_transaction_receipts: bool,
) -> Result<IngestWorker> {
    if lease_seconds <= 0 {
        bail!("lease-seconds must be greater than zero");
    }
    if chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let mut worker = IngestWorker::new(
        repositories,
        EvmRpcClient::new(rpc_url),
        worker_id,
        Duration::seconds(lease_seconds),
        chunk_size,
    )
    .with_transaction_receipts(include_transaction_receipts)
    .with_progress(true);
    if let Some(chain_id) = chain_id {
        if chain_id <= 0 {
            bail!("chain-id must be greater than zero");
        }
        worker = worker.with_chain_id(chain_id);
    }

    Ok(worker)
}

fn print_worker_outcome(outcome: &WorkerOutcome) {
    match outcome {
        WorkerOutcome::NoJob => println!("No queued jobs available."),
        WorkerOutcome::Processed { job_id, summary } => {
            println!("Processed range job {job_id}.");
            print_scan_summary(summary);
        }
        WorkerOutcome::Failed {
            job_id,
            status,
            error,
        } => {
            println!("Ingest job {job_id} failed with status {status}: {error}");
        }
        WorkerOutcome::Interrupted { job_id, status } => {
            println!("Worker interrupted. Job {job_id} released with status {status}.");
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("Failed to listen for shutdown signal: {error}");
    }
}

fn jobs_status(
    database_url: String,
    chain_id: Option<i64>,
    contract: Option<String>,
    job_type: Option<String>,
) -> Result<()> {
    let pool = build_pool(&database_url).context("build postgres pool")?;
    let repositories = PostgresRepositories::new(pool);
    let contract = contract
        .map(|value| normalize_address(&value))
        .transpose()
        .context("normalize contract address")?;
    let source_id = match contract {
        Some(contract) => {
            let chain_id = chain_id.context("--chain-id is required when --contract is used")?;
            let source = repositories
                .ledger()
                .source_by_contract(chain_id, &contract)
                .context("load source by contract")?
                .context("contract source not found")?;
            Some(source.id)
        }
        None => None,
    };
    let job_type = job_type
        .map(|value| {
            value
                .parse::<JobType>()
                .with_context(|| format!("parse job type {value}"))
        })
        .transpose()?;

    let counts = repositories
        .jobs()
        .status_counts(chain_id, source_id, job_type)
        .context("load job status counts")?;
    print_job_status_counts(&counts);

    Ok(())
}

fn print_job_status_counts(counts: &[JobStatusCount]) {
    let counts_by_status = counts
        .iter()
        .map(|row| (row.status.as_str(), row.count))
        .collect::<HashMap<_, _>>();
    let statuses = [
        JobStatus::Queued,
        JobStatus::Leased,
        JobStatus::Running,
        JobStatus::Succeeded,
        JobStatus::Failed,
        JobStatus::DeadLettered,
        JobStatus::Cancelled,
    ];
    let mut total = 0;

    println!("Job status:");
    for status in statuses {
        let count = counts_by_status.get(status.as_str()).copied().unwrap_or(0);
        total += count;
        println!("- {}: {}", status.as_str(), count);
    }
    println!("Total jobs: {total}");
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
    println!(
        "Transaction receipts persisted: {}",
        summary.transaction_receipts_persisted
    );
    println!("Current holders in indexed slice: {}", summary.holder_count);
    println!("Minters in indexed slice: {}", summary.minter_count);
}

fn rpc_url_from_args(value: Option<String>, chain_id: Option<i64>) -> Result<String> {
    let value = match value {
        Some(value) => value,
        None => rpc_url_from_env(chain_id)?,
    };
    validate_rpc_url_chain_hint(&value, chain_id)?;

    Ok(value)
}

fn rpc_url_from_env(chain_id: Option<i64>) -> Result<String> {
    if let Some(env_name) = chain_rpc_env_name(chain_id)
        && let Ok(value) = std::env::var(env_name)
    {
        return Ok(value);
    }

    std::env::var("EVM_RPC_URL")
        .or_else(|_| std::env::var("ETH_RPC_URL"))
        .with_context(|| {
            let chain_env = chain_rpc_env_name(chain_id)
                .map(|name| format!("{name}, "))
                .unwrap_or_default();
            format!("missing RPC URL; set --rpc-url, {chain_env}EVM_RPC_URL, or ETH_RPC_URL")
        })
}

fn chain_rpc_env_name(chain_id: Option<i64>) -> Option<&'static str> {
    match chain_id {
        Some(1) => Some("ETH_MAINNET_RPC_URL"),
        Some(137) => Some("POLYGON_MAINNET_RPC_URL"),
        _ => None,
    }
}

fn validate_rpc_url_chain_hint(value: &str, chain_id: Option<i64>) -> Result<()> {
    let lower = value.to_ascii_lowercase();
    match chain_id {
        Some(1) if lower.contains("polygon") => {
            bail!("selected RPC URL looks like Polygon, but --chain-id is 1")
        }
        Some(137) if lower.contains("eth-mainnet") || lower.contains("ethereum") => {
            bail!("selected RPC URL looks like Ethereum mainnet, but --chain-id is 137")
        }
        _ => Ok(()),
    }
}
