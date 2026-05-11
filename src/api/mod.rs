//! Thin Axum API adapter for querying indexed ledger slices.

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::Instrument;
use uuid::Uuid;

use crate::infra::postgres::{
    connection::PgPool,
    ledger_repository::{
        ContractSummary, HolderBalance, LedgerCursor, LedgerQuery, LedgerTransfer, MinterSummary,
    },
    repositories::PostgresRepositories,
};
use crate::infra::telemetry::metrics;

const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    static CURRENT_REQUEST_ID: String;
}

#[derive(Clone)]
struct ApiState {
    repositories: PostgresRepositories,
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics::prometheus_response))
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
        .layer(middleware::from_fn(request_id_middleware))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn request_id_middleware(request: Request<Body>, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let method = request.method().clone();
    let uri = request.uri().clone();
    let span = tracing::info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        uri = %uri,
    );
    let mut response = CURRENT_REQUEST_ID
        .scope(request_id.clone(), async move {
            next.run(request).instrument(span).await
        })
        .await;

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }

    response
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
    Query(query): Query<LedgerPageQuery>,
) -> ApiResult<Json<PageResponse<LedgerTransfer>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let query = query.to_ledger_query(50)?;
    let repo = state.repositories.ledger().clone();
    let page = blocking(move || repo.transfers_page(chain_id, &contract_address, query)).await?;
    let page = page.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(PageResponse::from(page)))
}

async fn token_path(
    State(state): State<ApiState>,
    Path((chain_id, contract_address, token_id)): Path<(i64, String, String)>,
    Query(query): Query<LedgerPageQuery>,
) -> ApiResult<Json<PageResponse<LedgerTransfer>>> {
    let contract_address = normalize_contract_address(&contract_address)?;
    let query = query.to_ledger_query(100)?;
    let repo = state.repositories.ledger().clone();
    let page =
        blocking(move || repo.token_path_page(chain_id, &contract_address, &token_id, query))
            .await?;
    let page = page.ok_or_else(|| ApiError::not_found("contract source not found"))?;

    Ok(Json(PageResponse::from(page)))
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
    normalize_evm_address(value, "contract")
}

fn normalize_holder_address(value: &str) -> ApiResult<String> {
    normalize_evm_address(value, "holder")
}

fn normalize_evm_address(value: &str, field: &str) -> ApiResult<String> {
    let normalized = value.to_ascii_lowercase();
    if normalized.len() != 42
        || !normalized.starts_with("0x")
        || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        return Err(ApiError::bad_request(format!(
            "invalid EVM {field} address"
        )));
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

#[derive(Debug, Deserialize)]
struct LedgerPageQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    from_block: Option<i64>,
    to_block: Option<i64>,
    holder: Option<String>,
    token_id: Option<String>,
    movement_type: Option<String>,
}

impl LedgerPageQuery {
    fn to_ledger_query(&self, default_limit: i64) -> ApiResult<LedgerQuery> {
        let limit = LimitQuery { limit: self.limit }.limit_or_default(default_limit)?;
        if let Some(from_block) = self.from_block
            && from_block < 0
        {
            return Err(ApiError::bad_request("from_block cannot be negative"));
        }
        if let Some(to_block) = self.to_block
            && to_block < 0
        {
            return Err(ApiError::bad_request("to_block cannot be negative"));
        }
        if let (Some(from_block), Some(to_block)) = (self.from_block, self.to_block)
            && from_block > to_block
        {
            return Err(ApiError::bad_request(
                "from_block cannot be greater than to_block",
            ));
        }

        Ok(LedgerQuery {
            limit,
            cursor: self.cursor.as_deref().map(parse_cursor).transpose()?,
            from_block: self.from_block,
            to_block: self.to_block,
            holder: self
                .holder
                .as_deref()
                .map(normalize_holder_address)
                .transpose()?,
            token_id: self.token_id.clone(),
            movement_type: self
                .movement_type
                .as_deref()
                .map(normalize_movement_type)
                .transpose()?,
        })
    }
}

fn parse_cursor(value: &str) -> ApiResult<LedgerCursor> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ApiError::bad_request("invalid cursor"));
    }

    let block_number = parts[0]
        .parse::<i64>()
        .map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let log_index = parts[1]
        .parse::<i32>()
        .map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let batch_index = parts[2]
        .parse::<i32>()
        .map_err(|_| ApiError::bad_request("invalid cursor"))?;

    if block_number < 0 || log_index < 0 || batch_index < 0 {
        return Err(ApiError::bad_request("invalid cursor"));
    }

    Ok(LedgerCursor {
        block_number,
        log_index,
        batch_index,
    })
}

fn encode_cursor(cursor: &LedgerCursor) -> String {
    format!(
        "{}:{}:{}",
        cursor.block_number, cursor.log_index, cursor.batch_index
    )
}

fn normalize_movement_type(value: &str) -> ApiResult<String> {
    let value = value.to_ascii_lowercase();
    if !matches!(value.as_str(), "mint" | "transfer" | "burn") {
        return Err(ApiError::bad_request(
            "movement_type must be mint, transfer, or burn",
        ));
    }

    Ok(value)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ItemsResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct PageResponse<T> {
    items: Vec<T>,
    next_cursor: Option<String>,
}

impl<T> From<crate::infra::postgres::ledger_repository::LedgerPage<T>> for PageResponse<T> {
    fn from(page: crate::infra::postgres::ledger_repository::LedgerPage<T>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor.as_ref().map(encode_cursor),
        }
    }
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
        let request_id = CURRENT_REQUEST_ID
            .try_with(Clone::clone)
            .unwrap_or_else(|_| "unknown".to_string());
        let (status, class, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "BadRequest", message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, "NotFound", message),
            Self::Internal(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal",
                error.to_string(),
            ),
        };

        (
            status,
            Json(json!({
                "error": {
                    "class": class,
                    "message": message,
                    "request_id": request_id,
                }
            })),
        )
            .into_response()
    }
}
