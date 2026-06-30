//! HTTP route handlers and router construction.

use crate::config::ServerConfig;
use crate::error::{Result, ShopError, payment_required_response, uuid_v4_str};
use crate::orchestrator::{self};
use crate::security::{generate_challenge, verify_challenge};
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, Sse, sse},
};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the Axum router for the Shop API.
pub fn build_router(state: AppState, sc: &ServerConfig) -> axum::Router {
    let prefix = sc.prefix.trim_end_matches('/').to_string();

    axum::Router::new()
        .route(
            &format!("{prefix}/challenge"),
            axum::routing::get(challenge_handler),
        )
        .route(
            &format!("{prefix}/spaces"),
            axum::routing::post(create_space_handler).get(list_spaces_handler),
        )
        .route(
            &format!("{prefix}/spaces/{{sid}}"),
            axum::routing::get(get_space_handler),
        )
        .route(
            &format!("{prefix}/rates"),
            axum::routing::get(rates_handler),
        )
        .route(
            &format!("{prefix}/upload"),
            axum::routing::post(presigned_upload_handler),
        )
        .route(
            &format!("{prefix}/tasks"),
            axum::routing::get(list_tasks_handler),
        )
        .route(
            &format!("{prefix}/tasks/{{kind}}"),
            axum::routing::get(list_tasks_by_kind_handler).post(submit_task_handler),
        )
        .route(
            &format!("{prefix}/tasks/{{kind}}/{{tid}}"),
            axum::routing::get(get_task_handler),
        )
        .route(
            &format!("{prefix}/tasks/{{kind}}/{{tid}}/events"),
            axum::routing::get(task_events_handler),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

/// Extract client IP from headers or socket address for rate limiting.
fn client_ip_key(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn set_request_id(headers: &mut HeaderMap) -> String {
    let id = uuid_v4_str();
    headers.insert(
        "x-request-id",
        axum::http::HeaderValue::from_str(&id)
            .unwrap_or(axum::http::HeaderValue::from_static("unknown")),
    );
    id
}

// ---------------------------------------------------------------------------
// GET /challenge
// ---------------------------------------------------------------------------

async fn challenge_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let challenge = generate_challenge(&state.config.shop.challenge);
    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(challenge)))
}

// ---------------------------------------------------------------------------
// POST /spaces
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateSpaceRequest {
    #[serde(default)]
    metadata: serde_json::Value,
}

async fn create_space_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateSpaceRequest>,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let space = state.create_space(body.metadata).await?;
    let mut resp_headers = HeaderMap::new();
    let request_id = set_request_id(&mut resp_headers);

    Ok((
        StatusCode::CREATED,
        resp_headers,
        Json(serde_json::json!({
            "sid": space.sid,
            "created_at": space.created_at,
            "metadata": space.metadata,
            "request_id": request_id,
        })),
    ))
}

// ---------------------------------------------------------------------------
// GET /spaces
// ---------------------------------------------------------------------------

async fn list_spaces_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let spaces = state.list_spaces().await?;
    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(serde_json::json!({ "spaces": spaces }))))
}

// ---------------------------------------------------------------------------
// GET /spaces/:sid
// ---------------------------------------------------------------------------

async fn get_space_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    // Validate sid format
    if sid.len() != 24 || !sid.chars().all(|c| c.is_ascii_digit()) {
        return Err(ShopError::BadRequest("invalid sid format".into()));
    }

    let space = state
        .get_space(&sid)
        .await?
        .ok_or_else(|| ShopError::NotFound(format!("space {sid} not found")))?;

    let deposits = state.get_deposits(&sid).await?;
    let balances = state.get_balances(&sid).await?;

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((
        resp_headers,
        Json(serde_json::json!({
            "sid": space.sid,
            "created_at": space.created_at,
            "metadata": space.metadata,
            "deposits": deposits,
            "balances": balances,
        })),
    ))
}

// ---------------------------------------------------------------------------
// GET /rates
// ---------------------------------------------------------------------------

async fn rates_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let rates_cfg = &state.config.shop.rates;

    // If proxy_url is configured, fetch from there
    if let Some(proxy_url) = &rates_cfg.proxy_url {
        let client = reqwest::Client::new();
        let resp = client
            .get(proxy_url)
            .send()
            .await
            .map_err(ShopError::Http)?;
        let proxy_rates: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ShopError::Internal(format!("failed to parse proxy rates: {e}")))?;

        let mut resp_headers = HeaderMap::new();
        set_request_id(&mut resp_headers);
        return Ok((resp_headers, Json(proxy_rates)));
    }

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((
        resp_headers,
        Json(serde_json::json!({
            "rates": rates_cfg.static_rates,
            "source": "static",
        })),
    ))
}

// ---------------------------------------------------------------------------
// POST /upload
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PresignedUploadRequest {
    key: String,
    #[serde(default)]
    content_type: String,
    #[serde(default)]
    max_size_bytes: Option<u64>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

async fn presigned_upload_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PresignedUploadRequest>,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let storage_cfg = &state.config.shop.storage;

    if storage_cfg.endpoint.is_empty() {
        return Err(ShopError::BadRequest("storage not configured".into()));
    }

    if body.key.is_empty() {
        return Err(ShopError::BadRequest("key is required".into()));
    }

    let max_size = body
        .max_size_bytes
        .unwrap_or(state.config.server_config().body_limit_bytes as u64);

    let presigned = crate::storage::build_presigned_post(
        storage_cfg,
        &body.key,
        &body.content_type,
        max_size,
        &body.metadata,
    )?;

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(presigned)))
}

// ---------------------------------------------------------------------------
// GET /tasks  (list task kinds)
// ---------------------------------------------------------------------------

async fn list_tasks_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let kinds: Vec<serde_json::Value> = state
        .config
        .shop
        .kinds
        .iter()
        .map(|k| {
            serde_json::json!({
                "slug": k.slug,
                "name": k.name,
                "description": k.description,
                "price_cents": k.price_cents,
            })
        })
        .collect();

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(serde_json::json!({ "kinds": kinds }))))
}

// ---------------------------------------------------------------------------
// GET /tasks/:kind  (list jobs for a kind + sid)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ListTasksQuery {
    sid: String,
}

async fn list_tasks_by_kind_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(kind): Path<String>,
    Query(query): Query<ListTasksQuery>,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    // Validate sid
    if query.sid.len() != 24 || !query.sid.chars().all(|c| c.is_ascii_digit()) {
        return Err(ShopError::BadRequest("invalid sid format".into()));
    }

    let jobs = state.get_jobs(&query.sid, Some(&kind)).await?;

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(serde_json::json!({ "jobs": jobs }))))
}

// ---------------------------------------------------------------------------
// POST /tasks/:kind  (submit a new task)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SubmitTaskRequest {
    sid: String,
    input: serde_json::Value,
    /// Idempotency key — optional client-chosen key.
    #[serde(default)]
    idempotency_key: Option<String>,
    /// Challenge verification fields.
    #[serde(default)]
    challenge_salt: Option<String>,
    #[serde(default)]
    challenge_expires: Option<String>,
    #[serde(default)]
    challenge_solution: Option<String>,
}

#[derive(Debug, Serialize)]
struct SubmitTaskResponse {
    tid: String,
    kind: String,
    status: String,
    price_cents: i64,
    created_at: String,
}

async fn submit_task_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(kind): Path<String>,
    Json(body): Json<SubmitTaskRequest>,
) -> Result<Response> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    // Validate sid
    if body.sid.len() != 24 || !body.sid.chars().all(|c| c.is_ascii_digit()) {
        return Err(ShopError::BadRequest("invalid sid format".into()));
    }

    // Verify challenge if configured with non-zero cost
    let challenge_cfg = &state.config.shop.challenge;
    if challenge_cfg.cost > 0 {
        let salt = body
            .challenge_salt
            .as_deref()
            .ok_or_else(|| ShopError::BadRequest("challenge_salt required".into()))?;
        let expires = body
            .challenge_expires
            .as_deref()
            .ok_or_else(|| ShopError::BadRequest("challenge_expires required".into()))?;
        let solution = body
            .challenge_solution
            .as_deref()
            .ok_or_else(|| ShopError::BadRequest("challenge_solution required".into()))?;

        let valid = verify_challenge(challenge_cfg, salt, expires, solution)?;
        if !valid {
            return Err(ShopError::Forbidden("challenge verification failed".into()));
        }
    }

    // Find the kind config
    let kind_cfg = state
        .config
        .shop
        .kinds
        .iter()
        .find(|k| k.slug == kind)
        .ok_or_else(|| ShopError::NotFound(format!("task kind '{kind}' not found")))?;

    // Idempotency check
    let idempotency_key = body
        .idempotency_key
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    if let Some(cached) = state.check_idempotency(&idempotency_key).await? {
        let mut resp_headers = HeaderMap::new();
        set_request_id(&mut resp_headers);
        return Ok((
            StatusCode::OK,
            resp_headers,
            Json(serde_json::json!({"cached": true, "response": cached})),
        )
            .into_response());
    }

    // Check balance if task has a price
    let price_cents = kind_cfg.price_cents as i64;
    if price_cents > 0 {
        let balances = state.get_balances(&body.sid).await?;
        let has_funds = balances.iter().any(|b| {
            b.asset == "USDC" && b.amount.parse::<f64>().unwrap_or(0.0) as i64 >= price_cents
        });
        if !has_funds {
            let request_id = uuid_v4_str();
            let response = payment_required_response(
                &format!(
                    "insufficient balance for kind '{kind}': requires {price_cents} USDC cents"
                ),
                &request_id,
            );
            return Ok(response);
        }
    }

    // Create the job
    let job = state
        .create_job(&body.sid, &kind, &body.input, price_cents)
        .await?;

    let response = SubmitTaskResponse {
        tid: job.tid.clone(),
        kind: kind.clone(),
        status: "pending".to_string(),
        price_cents,
        created_at: job.created_at.clone(),
    };

    // Store idempotency
    let response_json = serde_json::to_value(&response)?;
    state
        .store_idempotency(&idempotency_key, &body.sid, &kind, &response_json)
        .await?;

    // Spawn task execution
    let state_clone = state.clone();
    let bus_clone = state.bus.clone();
    let kind_cfg_clone = kind_cfg.clone();
    let input_clone = body.input.clone();
    let job_clone = job.clone();

    tokio::spawn(async move {
        orchestrator::run_task(
            state_clone,
            bus_clone,
            job_clone,
            kind_cfg_clone,
            input_clone,
        )
        .await;
    });

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    let response_json = serde_json::to_value(&response)?;
    Ok((StatusCode::CREATED, resp_headers, Json(response_json)).into_response())
}

// ---------------------------------------------------------------------------
// GET /tasks/:kind/:tid
// ---------------------------------------------------------------------------

async fn get_task_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((kind, tid)): Path<(String, String)>,
) -> Result<impl IntoResponse> {
    let key = client_ip_key(&headers);
    if !state.rate_limiter.check(&key).await {
        return Err(ShopError::TooManyRequests("rate limit exceeded".into()));
    }

    let job = state
        .get_job(&tid)
        .await?
        .ok_or_else(|| ShopError::NotFound(format!("task {tid} not found")))?;

    if job.kind != kind {
        return Err(ShopError::NotFound(format!(
            "task {tid} does not match kind {kind}"
        )));
    }

    let mut resp_headers = HeaderMap::new();
    set_request_id(&mut resp_headers);

    Ok((resp_headers, Json(job)))
}

// ---------------------------------------------------------------------------
// GET /tasks/:kind/:tid/events  (SSE)
// ---------------------------------------------------------------------------

async fn task_events_handler(
    State(state): State<AppState>,
    _headers: HeaderMap,
    Path((kind, tid)): Path<(String, String)>,
) -> Result<Sse<impl Stream<Item = std::result::Result<sse::Event, Infallible>>>> {
    // Verify the job exists and belongs to the given kind
    let job = state
        .get_job(&tid)
        .await?
        .ok_or_else(|| ShopError::NotFound(format!("task {tid} not found")))?;

    if job.kind != kind {
        return Err(ShopError::NotFound(format!(
            "task {tid} does not match kind {kind}"
        )));
    }

    // Send stored events first, then subscribe to live stream
    let stored_events = state.get_task_events(&tid).await?;

    let bus = state.bus.clone();
    let mut rx = bus.subscribe(&tid).await;

    let stream = async_stream::stream! {
        // Replay stored events
        for evt in stored_events {
            let data = serde_json::to_string(&evt).unwrap_or_default();
            yield Ok(sse::Event::default()
                .event("task_event")
                .data(data));
        }

        // Stream live events
        loop {
            match rx.recv().await {
                Ok(evt) => {
                    let data = serde_json::to_string(&evt).unwrap_or_default();
                    yield Ok(sse::Event::default()
                        .event("task_event")
                        .data(data));

                    // If the event indicates completion, close the stream
                    if evt.status == "completed" || evt.status == "failed" {
                        yield Ok(sse::Event::default()
                            .event("done")
                            .data("stream complete"));
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(sse::Event::default()
                        .event("warning")
                        .data(format!("lagged by {n} events")));
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(
        sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_config() -> AppConfig {
        let raw = r#"
version = 1

[shop.challenge]
secret = "test-secret-api-test-123456"
ttl_secs = 600
cost = 0

[shop.rates]
static_rates = { ETH = 3000.0, USDC = 1.0 }

[shop.rate_limit]
capacity = 1000
window_secs = 60

[[shop.kinds]]
slug = "img.edit"
name = "Image Edit"
description = "Edit an image"
price_cents = 0
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        cfg
    }

    async fn test_state() -> AppState {
        let cfg = test_config();
        let db_path =
            std::path::PathBuf::from(format!("/tmp/shop-test-{}.db", uuid::Uuid::new_v4()));
        AppState::new(cfg, db_path).await.unwrap()
    }

    #[tokio::test]
    async fn get_challenge_returns_valid_challenge() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state, &sc);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{}/challenge", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["algorithm"].is_string());
        assert!(json["challenge"].is_string());
        assert!(json["salt"].is_string());
        assert!(json["expires"].is_string());
    }

    #[tokio::test]
    async fn create_and_get_space() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state.clone(), &sc);

        // Create space
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/spaces", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"metadata":{"name":"test"}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sid = json["sid"].as_str().unwrap().to_string();
        assert_eq!(sid.len(), 24);
        assert!(sid.chars().all(|c| c.is_ascii_digit()));

        // Get the space
        let app2 = build_router(state.clone(), &sc);
        let response = app2
            .oneshot(
                Request::builder()
                    .uri(format!("{}/spaces/{sid}", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["sid"], sid);
        assert_eq!(json["metadata"]["name"], "test");
    }

    #[tokio::test]
    async fn get_space_invalid_sid_returns_400() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state, &sc);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{}/spaces/not-24-digits", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_rates_returns_static_rates() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state, &sc);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{}/rates", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rates"]["ETH"], 3000.0);
        assert_eq!(json["rates"]["USDC"], 1.0);
        assert_eq!(json["source"], "static");
    }

    #[tokio::test]
    async fn list_tasks_returns_kinds() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state, &sc);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{}/tasks", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kinds"][0]["slug"], "img.edit");
    }

    #[tokio::test]
    async fn submit_task_creates_job() {
        let state = test_state().await;
        let sc = state.config.server_config();

        // First create a space to get a valid sid
        let app_create = build_router(state.clone(), &sc);
        let response = app_create
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/spaces", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sid = json["sid"].as_str().unwrap().to_string();

        // Submit a task
        let app_task = build_router(state.clone(), &sc);
        let task_body = serde_json::json!({
            "sid": sid,
            "input": {"image_url": "https://example.com/img.png"},
        });
        let response = app_task
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/tasks/img.edit", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&task_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["tid"].is_string());
        assert_eq!(json["kind"], "img.edit");
        assert_eq!(json["status"], "pending");
    }

    #[tokio::test]
    async fn submit_task_with_idempotency_returns_cached() {
        let state = test_state().await;
        let sc = state.config.server_config();

        // Create space
        let app = build_router(state.clone(), &sc);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/spaces", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sid = json["sid"].as_str().unwrap().to_string();

        // First submission
        let idemp_key = "test-idemp-key-001";
        let task_body = serde_json::json!({
            "sid": sid,
            "input": {},
            "idempotency_key": idemp_key,
        });

        let app1 = build_router(state.clone(), &sc);
        let response = app1
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/tasks/img.edit", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&task_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tid1 = json1["tid"].as_str().unwrap().to_string();

        // Second submission with same idempotency key — should return cached
        let app2 = build_router(state.clone(), &sc);
        let response = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/tasks/img.edit", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&task_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json2["cached"].as_bool().unwrap_or(false));
        assert_eq!(json2["response"]["tid"], tid1);
    }

    #[tokio::test]
    async fn get_unknown_space_returns_404() {
        let state = test_state().await;
        let sc = state.config.server_config();
        let app = build_router(state, &sc);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("{}/spaces/123456789012345678901234", sc.prefix))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_task_kind_returns_404() {
        let state = test_state().await;
        let sc = state.config.server_config();

        // Create space first
        let app = build_router(state.clone(), &sc);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/spaces", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sid = json["sid"].as_str().unwrap().to_string();

        let app2 = build_router(state.clone(), &sc);
        let task_body = serde_json::json!({"sid": sid, "input": {}});
        let response = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{}/tasks/nonexistent", sc.prefix))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&task_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
