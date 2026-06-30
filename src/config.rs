use crate::error::{Result, ShopError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Top-level AppConfig — parses the merged universal Config.toml
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    /// Shared configuration schema version. Current schema is version = 1.
    pub version: u32,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub stores: BTreeMap<String, StoreConfig>,
    #[serde(default)]
    pub shop: ShopConfig,
    // ---- tolerate sibling package sections ----
    #[serde(default)]
    #[allow(dead_code)]
    ladon: Option<toml::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    pano: Option<toml::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    bria: Option<toml::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    oracles: Option<toml::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    artur: Option<toml::Value>,
}

impl AppConfig {
    /// Derive the final runtime/server configuration by merging shared sections
    /// with [shop]-specific overrides.
    pub fn server_config(&self) -> ServerConfig {
        ServerConfig {
            bind: self
                .shop
                .server
                .bind
                .clone()
                .or_else(|| self.http.bind.clone())
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            port: self.shop.server.port.or(self.http.port).unwrap_or(47296),
            prefix: self
                .shop
                .server
                .prefix
                .clone()
                .or_else(|| self.http.prefix.clone())
                .unwrap_or_else(|| "/v1".to_string()),
            body_limit_bytes: self
                .shop
                .server
                .body_limit_bytes
                .or(self.http.max_body_bytes)
                .or(self.runtime.max_payload_bytes)
                .unwrap_or(10 * 1024 * 1024),
            api_key: self
                .shop
                .server
                .api_key
                .clone()
                .or_else(|| self.http.api_key.clone()),
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(ShopError::Config(format!(
                "unsupported config version {}; expected version = 1",
                self.version
            )));
        }
        // Validate challenge config
        if self.shop.challenge.secret.trim().is_empty()
            || self.shop.challenge.secret == "change-me-to-a-random-secret"
        {
            return Err(ShopError::Config(
                "shop.challenge.secret must be set to a random secret string".to_string(),
            ));
        }
        // Validate storage is configured if upload endpoint is not disabled
        if !self.shop.storage.endpoint.trim().is_empty()
            && (self.shop.storage.access_key.trim().is_empty()
                || self.shop.storage.secret_key.trim().is_empty()
                || self.shop.storage.bucket.trim().is_empty())
        {
            return Err(ShopError::Config(
                "shop.storage requires access_key, secret_key, and bucket when endpoint is set"
                    .to_string(),
            ));
        }
        // Validate at least one kind is defined
        if self.shop.kinds.is_empty() {
            return Err(ShopError::Config(
                "at least one [[shop.kinds]] entry is required".to_string(),
            ));
        }
        // Validate kinds
        let mut kind_slugs = std::collections::BTreeSet::new();
        for kind in &self.shop.kinds {
            if kind.slug.trim().is_empty() {
                return Err(ShopError::Config("kind slug cannot be empty".to_string()));
            }
            if !kind_slugs.insert(kind.slug.clone()) {
                return Err(ShopError::Config(format!(
                    "duplicate kind slug '{}'",
                    kind.slug
                )));
            }
        }
        // Validate packages
        for (name, pkg) in &self.shop.packages {
            if name.trim().is_empty() {
                return Err(ShopError::Config(
                    "package name cannot be empty".to_string(),
                ));
            }
            if pkg.enabled && pkg.command.trim().is_empty() {
                return Err(ShopError::Config(format!(
                    "package '{name}' is enabled but command is empty"
                )));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared root sections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LogConfig {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub worker_threads: Option<usize>,
    #[serde(default)]
    pub shutdown_timeout_secs: Option<u64>,
    #[serde(default)]
    pub tmp_dir: Option<String>,
    #[serde(default)]
    pub max_payload_bytes: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct HttpConfig {
    #[serde(default)]
    pub bind: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub max_body_bytes: Option<usize>,
    #[serde(default)]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StoreConfig {
    pub driver: Option<String>,
    pub url: String,
    #[serde(default)]
    pub migrate: bool,
    #[serde(default)]
    pub connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_connections: Option<u32>,
}

// ---------------------------------------------------------------------------
// Derived server config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub prefix: String,
    pub body_limit_bytes: usize,
    pub api_key: Option<String>,
}

// ---------------------------------------------------------------------------
// [shop] namespace
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ShopConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub server: ShopServerConfig,
    #[serde(default)]
    pub challenge: ChallengeConfig,
    #[serde(default)]
    pub rates: RatesConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub idempotency: IdempotencyConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub kinds: Vec<KindConfig>,
    #[serde(default)]
    pub packages: BTreeMap<String, PackageConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ShopServerConfig {
    #[serde(default)]
    pub bind: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub body_limit_bytes: Option<usize>,
    #[serde(default)]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChallengeConfig {
    /// HMAC secret for signing challenges (ALTCHA-style).
    #[serde(default = "default_challenge_secret")]
    pub secret: String,
    /// Challenge expiry in seconds.
    #[serde(default = "default_challenge_ttl")]
    pub ttl_secs: u64,
    /// PoW cost/difficulty.
    #[serde(default = "default_challenge_cost")]
    pub cost: u32,
    /// Algorithm for the PoW challenge.
    #[serde(default = "default_challenge_algorithm")]
    pub algorithm: String,
}

impl Default for ChallengeConfig {
    fn default() -> Self {
        Self {
            secret: default_challenge_secret(),
            ttl_secs: default_challenge_ttl(),
            cost: default_challenge_cost(),
            algorithm: default_challenge_algorithm(),
        }
    }
}

fn default_challenge_secret() -> String {
    "change-me-to-a-random-secret".to_string()
}
fn default_challenge_ttl() -> u64 {
    600
}
fn default_challenge_cost() -> u32 {
    5000
}
fn default_challenge_algorithm() -> String {
    "PBKDF2/SHA-256".to_string()
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RatesConfig {
    /// Static rates as a JSON object (symbol -> price).
    #[serde(default)]
    pub static_rates: serde_json::Value,
    /// Optional HTTP endpoint to proxy rates from.
    #[serde(default)]
    pub proxy_url: Option<String>,
    #[serde(default = "default_rates_refresh_secs")]
    pub refresh_secs: u64,
}

fn default_rates_refresh_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StorageConfig {
    /// S3-compatible endpoint URL (e.g., http://127.0.0.1:9000).
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default)]
    pub bucket: String,
    /// Region string (default us-east-1).
    #[serde(default = "default_region")]
    pub region: String,
    /// Public base URL for uploaded objects (optional).
    #[serde(default)]
    pub public_base_url: Option<String>,
    /// Presigned URL expiry in seconds.
    #[serde(default = "default_presigned_expiry")]
    pub presigned_expiry_secs: u64,
}

fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_presigned_expiry() -> u64 {
    3600
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct IdempotencyConfig {
    /// How long idempotency keys are retained (seconds).
    #[serde(default = "default_idempotency_ttl")]
    pub ttl_secs: u64,
}

fn default_idempotency_ttl() -> u64 {
    86400
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RateLimitConfig {
    /// Requests per window.
    #[serde(default = "default_rate_limit_capacity")]
    pub capacity: u32,
    /// Window duration in seconds.
    #[serde(default = "default_rate_limit_window")]
    pub window_secs: u64,
}

fn default_rate_limit_capacity() -> u32 {
    60
}
fn default_rate_limit_window() -> u64 {
    60
}

// ---------------------------------------------------------------------------
// [[shop.kinds]]
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KindConfig {
    /// Unique slug for the kind (e.g., "img.edit").
    pub slug: String,
    /// Human-readable name.
    #[serde(default)]
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: String,
    /// Price in USD cents (0 = free).
    #[serde(default)]
    pub price_cents: u64,
    /// JSON schema for input validation.
    #[serde(default)]
    pub input_schema: serde_json::Value,
    /// Task execution steps.
    #[serde(default)]
    pub steps: Vec<TaskStepConfig>,
    /// Maximum parallelism for concurrent steps within a task.
    #[serde(default = "default_kind_concurrency")]
    pub concurrency: u32,
}

fn default_kind_concurrency() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStepConfig {
    /// Step id for dependency ordering.
    #[serde(default)]
    pub id: String,
    /// Command to execute.
    pub command: String,
    /// Arguments passed to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Working directory.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Inherit parent process environment.
    #[serde(default = "default_true")]
    pub inherit_env: bool,
    /// Acceptable exit codes.
    #[serde(default = "default_success_codes")]
    pub success_exit_codes: Vec<i32>,
    /// Timeout in milliseconds.
    #[serde(default = "default_step_timeout")]
    pub timeout_ms: u64,
    /// Step IDs that must complete before this step starts.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Whether to continue on error.
    #[serde(default)]
    pub continue_on_error: bool,
    /// Max stdout bytes.
    #[serde(default = "default_output_limit")]
    pub max_stdout_bytes: usize,
    /// Max stderr bytes.
    #[serde(default = "default_output_limit")]
    pub max_stderr_bytes: usize,
}

fn default_true() -> bool {
    true
}
fn default_success_codes() -> Vec<i32> {
    vec![0]
}
fn default_step_timeout() -> u64 {
    300_000
}
fn default_output_limit() -> usize {
    1024 * 1024
}

// ---------------------------------------------------------------------------
// [shop.packages.<name>]
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PackageConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_true")]
    pub inherit_env: bool,
    #[serde(default)]
    pub restart: bool,
    #[serde(default = "default_restart_delay")]
    pub restart_delay_secs: u64,
}

fn default_restart_delay() -> u64 {
    5
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

/// Load configuration from a file path or HTTP(S) URL.
pub async fn load_config(location: &str) -> Result<AppConfig> {
    let raw = if location.starts_with("http://") || location.starts_with("https://") {
        reqwest::get(location)
            .await?
            .error_for_status()?
            .text()
            .await?
    } else {
        let path = Path::new(location);
        tokio::fs::read_to_string(path).await?
    };
    let cfg: AppConfig = toml::from_str(&raw)?;
    cfg.validate()?;
    Ok(cfg)
}

/// Derive the SQLite database path from the [stores] section.
pub fn resolve_sqlite_path(stores: &BTreeMap<String, StoreConfig>) -> PathBuf {
    for store in stores.values() {
        let is_sqlite = store
            .driver
            .as_deref()
            .map(|d| d == "sqlite")
            .unwrap_or(false);
        if is_sqlite && !store.url.is_empty() {
            let path = store
                .url
                .strip_prefix("sqlite://")
                .or_else(|| store.url.strip_prefix("sqlite:"))
                .unwrap_or(&store.url);
            if !path.is_empty() {
                return PathBuf::from(path);
            }
        }
    }
    PathBuf::from("shop.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let raw = r#"
version = 1

[shop.challenge]
secret = "test-secret-1234567890"

[[shop.kinds]]
slug = "img.edit"
name = "Image Edit"
description = "Edit an image"
price_cents = 100
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.shop.kinds.len(), 1);
        assert_eq!(cfg.shop.kinds[0].slug, "img.edit");
        assert_eq!(cfg.server_config().port, 47296);
    }

    #[test]
    fn parse_full_config_with_siblings() {
        let raw = r#"
version = 1

[http]
bind = "0.0.0.0"
port = 48080
prefix = "/v1"

[log]
level = "info"

[stores.shop]
driver = "sqlite"
url = "sqlite://data/shop.db"

[shop.challenge]
secret = "super-secret-hmac-key"
ttl_secs = 300
cost = 10000

[shop.rates]
static_rates = { ETH = 3000.0, USDC = 1.0 }

[shop.storage]
endpoint = "http://127.0.0.1:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket = "shop-uploads"
region = "us-east-1"

[[shop.kinds]]
slug = "img.resize"
name = "Resize Image"
price_cents = 50

[[shop.kinds.steps]]
command = "convert"
args = ["-resize", "800x600"]

[shop.packages.ladon]
enabled = true
command = "ladon"

[ladon]
some_ladon_setting = true

[bria]
some_bria_setting = false
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        let server = cfg.server_config();
        assert_eq!(server.bind, "0.0.0.0");
        assert_eq!(server.port, 48080);
        assert_eq!(server.prefix, "/v1");
        assert_eq!(cfg.shop.kinds.len(), 1);
        assert_eq!(cfg.shop.challenge.cost, 10000);
        assert_eq!(cfg.shop.rates.static_rates["ETH"], 3000.0);
        assert!(cfg.shop.packages.contains_key("ladon"));
    }

    #[test]
    fn rejects_missing_challenge_secret() {
        let raw = r#"
version = 1

[[shop.kinds]]
slug = "test"
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("challenge.secret"));
    }

    #[test]
    fn rejects_duplicate_kind_slug() {
        let raw = r#"
version = 1

[shop.challenge]
secret = "test"

[[shop.kinds]]
slug = "img.edit"

[[shop.kinds]]
slug = "img.edit"
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn server_config_inherits_http_prefix() {
        let raw = r#"
version = 1

[http]
prefix = "/api/v2"

[shop.challenge]
secret = "test"

[[shop.kinds]]
slug = "test"
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.server_config().prefix, "/api/v2");
    }

    #[test]
    fn server_config_shop_override_wins() {
        let raw = r#"
version = 1

[http]
prefix = "/api/v2"

[shop.server]
prefix = "/v1"

[shop.challenge]
secret = "test"

[[shop.kinds]]
slug = "test"
"#;
        let cfg: AppConfig = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.server_config().prefix, "/v1");
    }
}
