//! Shop — Config-driven API backend entry point.

use clap::Parser;
use shop::config::{load_config, resolve_sqlite_path};
use shop::orchestrator;
use shop::state::AppState;
use std::net::SocketAddr;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Config-driven API backend with spaces, tasks, uploads, and package orchestration.
#[derive(Parser, Debug)]
#[command(name = "shop", version, about)]
struct Cli {
    /// Path or URL to the Config.toml file.
    #[arg(short, long, default_value = "Config.toml", env = "SHOP_CONFIG")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shop=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("loading config from {}", cli.config);

    // Load configuration
    let config = load_config(&cli.config).await?;
    let server_config = config.server_config();

    // Resolve database path
    let db_path = resolve_sqlite_path(&config.stores);
    tracing::info!("using database at {}", db_path.display());

    // Initialize application state
    let state = AppState::new(config.clone(), db_path).await?;

    // Build the router
    let api_router = shop::api::build_router(state.clone(), &server_config);

    // Wrap with tower-http middleware
    use axum::http::Request;
    use tower_http::cors::{Any, CorsLayer};
    use tower_http::limit::RequestBodyLimitLayer;
    use tower_http::request_id::{MakeRequestId, RequestId, SetRequestIdLayer};
    use tower_http::trace::TraceLayer;

    #[derive(Clone)]
    struct UuidRequestId;
    impl MakeRequestId for UuidRequestId {
        fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
            let id = uuid::Uuid::new_v4().to_string();
            let hv = axum::http::HeaderValue::from_str(&id).ok()?;
            Some(RequestId::new(hv))
        }
    }

    let app = api_router
        .layer(SetRequestIdLayer::new(
            axum::http::HeaderName::from_static("x-request-id"),
            UuidRequestId,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(RequestBodyLimitLayer::new(server_config.body_limit_bytes));

    // Launch enabled packages
    let package_handles = orchestrator::launch_packages(cli.config.clone(), &config.shop.packages);

    // Bind and serve
    let addr: SocketAddr = format!("{}:{}", server_config.bind, server_config.port)
        .parse()
        .expect("invalid bind address");

    tracing::info!("listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Wait for package processes to exit
    for handle in package_handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl-C handler");
    tracing::info!("shutting down");
}
