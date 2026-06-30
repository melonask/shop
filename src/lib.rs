//! `shop` is a config-driven Rust API backend.
//!
//! It reads shared root configuration such as `[log]`, `[runtime]`, `[http]`,
//! `[stores.*]`, and `[transports.*]`, while Shop-owned endpoints and tasks
//! live only under `[shop]`. This lets the same `Config.toml` be mounted into
//! `artur`, `bria`, `ladon`, `oracles`, and `pano`; each package reads its own
//! namespace and shared profiles.
//!
//! Shop maps TOML-defined HTTP endpoints for spaces, tasks, uploads, challenges,
//! and rates. It manages SQLite state, presigned S3 uploads, idempotency,
//! rate limits, and child-process orchestration.

pub mod api;
pub mod config;
pub mod error;
pub mod orchestrator;
pub mod security;
pub mod state;
pub mod storage;

pub use config::{AppConfig, load_config};
pub use state::AppState;
