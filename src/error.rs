use axum::Json;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Structured JSON error response with code, message, and request_id.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorPayload,
}

#[derive(Debug, Serialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    pub request_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ShopError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("payment required: {0}")]
    PaymentRequired(String),

    #[error("too many requests: {0}")]
    TooManyRequests(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, ShopError>;

impl ShopError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Config(_) | Self::Internal(_) | Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest(_) | Self::Toml(_) | Self::Json(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,
            Self::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Sqlite(_) | Self::Http(_) => StatusCode::BAD_GATEWAY,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "config_error",
            Self::BadRequest(_) => "bad_request",
            Self::NotFound(_) => "not_found",
            Self::Forbidden(_) => "forbidden",
            Self::PaymentRequired(_) => "payment_required",
            Self::TooManyRequests(_) => "too_many_requests",
            Self::Conflict(_) => "conflict",
            Self::Internal(_) => "internal_error",
            Self::Io(_) => "io_error",
            Self::Toml(_) => "toml_error",
            Self::Json(_) => "json_error",
            Self::Sqlite(_) => "sqlite_error",
            Self::Http(_) => "http_error",
        }
    }
}

impl IntoResponse for ShopError {
    fn into_response(self) -> axum::response::Response {
        let request_id = uuid_v4_str();
        let status = self.status();
        let body = ErrorBody {
            error: ErrorPayload {
                code: self.code().to_string(),
                message: self.to_string(),
                request_id,
            },
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-request-id",
            HeaderValue::from_str(&body.error.request_id)
                .unwrap_or(HeaderValue::from_static("unknown")),
        );
        (status, headers, Json(body)).into_response()
    }
}

/// Generate a v4 UUID string for request tracing.
pub fn uuid_v4_str() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Produce a 402 Payment Required response with x402 headers.
pub fn payment_required_response(message: &str, request_id: &str) -> Response {
    let body = ErrorBody {
        error: ErrorPayload {
            code: "payment_required".to_string(),
            message: format!("payment required: {message}"),
            request_id: request_id.to_string(),
        },
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-request-id",
        HeaderValue::from_str(request_id).unwrap_or(HeaderValue::from_static("unknown")),
    );
    headers.insert("x402-version", HeaderValue::from_static("1"));
    (StatusCode::PAYMENT_REQUIRED, headers, Json(body)).into_response()
}
