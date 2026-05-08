//! Thin Axum API adapter for querying indexed ledger slices.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::infra::postgres::{
    connection::PgPool,
    ledger_repository::{ContractSummary, HolderBalance, LedgerTransfer, MinterSummary},
    repositories::PostgresRepositories,
};

#[derive(Clone)]
struct ApiState {
    repositories: PostgresRepositories,
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/chains/{chain_id}/contracts/{contract_address}/summary",
            get(contract_summary),
        )
        .route(
            "/chains/{chain_id}/contracts/{contract_address}/holders",
            get(contract_holders),
        )
        .route(
            "/chains/{chain_id}/contracts/{contract_address}/minters",
            get(contract_minters),
        )
        .route(
            "/chains/{chain_id}/contracts/{contract_address}/transfers",
            get(contract_transfers),
        )
        .route(
            "/chains/{chain_id}/contracts/{contract_address}/tokens/{token_id}/path",
            get(token_path),
        )
        .with_state(ApiState {
            repositories: PostgresRepositories::new(pool),
        })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn contract_summary(
    State(state): State<ApiState>,
    Path((chain_id, contract_address)): Path<(i64, String)>,
) -> ApiResult<Json<ContractSummary>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let repo = state.repositories.ledger().clone();
    let summary = blocking(move || repo.contract_summary(chain_id, &contract_address)).await?;
    let summary = summary.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(summary))
}

async fn contract_holders(
    State(state): State<ApiState>,
    Path((chain_id, contract_address)): Path<(i64, String)>,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<ItemsResponse<HolderBalance>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let limit = query.limit_or_default(50)?;
    let repo = state.repositories.ledger().clone();
    let holders = blocking(move || repo.holders(chain_id, &contract_address, limit)).await?;
    let holders = holders.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(ItemsResponse { items: holders }))
}

async fn contract_minters(
    State(state): State<ApiState>,
    Path((chain_id, contract_address)): Path<(i64, String)>,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<ItemsResponse<MinterSummary>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let limit = query.limit_or_default(50)?;
    let repo = state.repositories.ledger().clone();
    let minters = blocking(move || repo.minters(chain_id, &contract_address, limit)).await?;
    let minters = minters.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(ItemsResponse { items: minters }))
}

async fn contract_transfers(
    State(state): State<ApiState>,
    Path((chain_id, contract_address)): Path<(i64, String)>,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<ItemsResponse<LedgerTransfer>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let limit = query.limit_or_default(50)?;
    let repo = state.repositories.ledger().clone();
    let transfers = blocking(move || repo.transfers(chain_id, &contract_address, limit)).await?;
    let transfers = transfers.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(ItemsResponse { items: transfers }))
}

async fn token_path(
    State(state): State<ApiState>,
    Path((chain_id, contract_address, token_id)): Path<(i64, String, String)>,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<ItemsResponse<LedgerTransfer>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let limit = query.limit_or_default(100)?;
    let repo = state.repositories.ledger().clone();
    let path =
        blocking(move || repo.token_path(chain_id, &contract_address, &token_id, limit)).await?;
    let path = path.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(ItemsResponse { items: path }))
}

async fn blocking<T>(operation: impl FnOnce() -> anyhow::Result<T> + Send + 'static) -> ApiResult<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| ApiError::internal(format!("blocking task failed: {error}")))?
        .map_err(ApiError::from)
}

fn normalize_contract_address(value: &str) -> ApiResult<String> {
    let normalized = value.to_ascii_lowercase();
    if normalized.len() != 42
        || !normalized.starts_with("0x")
        || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        return Err(ApiError::bad_request("invalid EVM contract address"));
    }

    Ok(normalized)
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

impl LimitQuery {
    fn limit_or_default(&self, default: i64) -> ApiResult<i64> {
        let limit = self.limit.unwrap_or(default);
        if !(1..=100).contains(&limit) {
            return Err(ApiError::bad_request("limit must be between 1 and 100"));
        }

        Ok(limit)
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ItemsResponse<T> {
    items: Vec<T>,
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(anyhow::Error),
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::Internal(anyhow::anyhow!(message.into()))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
