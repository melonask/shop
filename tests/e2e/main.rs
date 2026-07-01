//! Shop end-to-end integration tests.
//!
//! **All external services must be running and reachable.** If any service is
//! unavailable the test fails with a clear message to start docker compose.
//!
//! Prerequisites:
//!   docker compose -f tests/e2e/docker-compose.yml up -d --wait
//!
//! Run:
//!   SHOP_E2E=1 cargo test --test e2e -- --ignored --nocapture --test-threads=1

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::Value;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

static NEXT_PORT: AtomicU16 = AtomicU16::new(47300);

fn e2e_enabled() -> bool {
    std::env::var("SHOP_E2E").is_ok()
}

fn unique_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

/// Require that a service at `url` responds successfully, or panic with a
/// helpful message to start docker compose.
async fn require_service(client: &reqwest::Client, name: &str, url: &str) {
    let (_method, body) = if url.contains("8545") || url.contains("8899") || url.contains("18443") {
        // JSON-RPC endpoints need POST
        (
            "POST",
            Some(
                serde_json::json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}),
            ),
        )
    } else {
        ("GET", None)
    };

    for _ in 0..10 {
        let req = if let Some(b) = &body {
            client.post(url).json(b)
        } else {
            client.get(url)
        };
        match req.send().await {
            Ok(r) if r.status().is_success() => return,
            // RustFS returns 403 on root GET (requires auth) — service is alive
            Ok(r) if name == "RustFS" && r.status().as_u16() == 403 => return,
            Ok(r)
                if name == "Solana"
                    && (r.status().as_u16() == 405 || r.status().as_u16() == 400) =>
            {
                return;
            } // Solana RPC accepts POST only
            Ok(r) if name == "Anvil" && r.status().as_u16() == 400 => return, // Anvil RPC POST returns 400 for GET
            Ok(r) => {
                panic!(
                    "{name} at {url} returned HTTP {} — check docker compose",
                    r.status()
                );
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    panic!(
        "{name} at {url} is not reachable — start: docker compose -f tests/e2e/docker-compose.yml up -d --wait"
    );
}

struct E2EContext {
    base_url: String,
    db_path: String,
    client: reqwest::Client,
    _shop_process: Option<Child>,
}

impl E2EContext {
    async fn new() -> Self {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        let port = unique_port();
        let db_path = format!("{manifest_dir}/tests/e2e/shop-e2e-{port}.db");
        let _ = std::fs::remove_file(&db_path);

        let config_str = format!(
            r#"
version = 1

[log]
level = "info"
format = "text"

[runtime]
worker_threads = 2
shutdown_timeout_secs = 10

[http]
bind = "127.0.0.1"
port = {port}
prefix = "/v1"

[stores.shop]
driver = "sqlite"
url = "sqlite://{db_path}"

[shop]
enabled = true

[shop.challenge]
secret = "e2e-test-secret-key-must-be-long-enough-32"
ttl_secs = 600
cost = 0

[shop.rates]
static_rates = {{ ETH = 3000.0, USDC = 1.0, SOL = 150.0, BTC = 65000.0 }}

[shop.storage]
endpoint = "http://127.0.0.1:9000"
access_key = "rustfsadmin"
secret_key = "rustfsadmin"
bucket = "shop-e2e-uploads"
region = "us-east-1"
public_base_url = "http://127.0.0.1:9000/shop-e2e-uploads"
presigned_expiry_secs = 3600

[shop.idempotency]
ttl_secs = 86400

[shop.rate_limit]
capacity = 1000
window_secs = 60

[shop.chains.anvil]
rpc_url = "http://127.0.0.1:8545"
deposit_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
asset = "ETH"
chain_id = 31337
confirmations = 1

[shop.chains.solana]
rpc_url = "http://127.0.0.1:8899"
deposit_address = "11111111111111111111111111111111"
asset = "SOL"
chain_id = 0
confirmations = 1

[shop.chains.bitcoin]
rpc_url = "http://shop:shoppass@127.0.0.1:18443"
deposit_address = "bcrt1qtestdepositaddresssimulated1"
asset = "BTC"
chain_id = 0
confirmations = 1

[[shop.kinds]]
slug = "process.file"
name = "Process File"
description = "Transform an uploaded file and return metadata"
price_cents = 0
concurrency = 1

[[shop.kinds.steps]]
id = "transform"
command = "bash"
args = ["tests/e2e/scripts/task-processor.sh"]
timeout_ms = 30000
inherit_env = true
"#
        );

        let config_path = format!("/tmp/shop-e2e-config-{port}.toml");
        std::fs::write(&config_path, &config_str).expect("write config");

        let shop_binary = std::env::var("SHOP_BINARY")
            .unwrap_or_else(|_| format!("{manifest_dir}/target/debug/shop"));

        if !std::path::Path::new(&shop_binary).exists() {
            let status = Command::new("cargo")
                .args(["build", "--bin", "shop"])
                .current_dir(&manifest_dir)
                .status()
                .expect("failed to build shop");
            assert!(status.success(), "shop binary build failed");
        }

        let mut child = Command::new(&shop_binary)
            .args(["--config", &config_path])
            .current_dir(&manifest_dir)
            .spawn()
            .expect("failed to start shop");

        let client = reqwest::Client::new();
        let base_url = format!("http://127.0.0.1:{port}/v1");

        // Wait for shop to become ready
        for _ in 0..75 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Ok(resp) = client.get(format!("{base_url}/challenge")).send().await
                && resp.status().is_success()
            {
                break;
            }
            if let Ok(Some(_)) = child.try_wait() {
                panic!("shop process exited prematurely");
            }
        }

        Self {
            base_url,
            db_path,
            client,
            _shop_process: Some(child),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl Drop for E2EContext {
    fn drop(&mut self) {
        if let Some(mut child) = self._shop_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(&self.db_path);
    }
}

// ---------------------------------------------------------------------------
// Helper: S3-compatible bucket creation via SigV4-signed PUT
// ---------------------------------------------------------------------------

fn sigv4_put_bucket(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> reqwest::Request {
    sigv4_signed_request(
        "PUT",
        endpoint,
        &format!("/{bucket}"),
        "",
        region,
        access_key,
        secret_key,
    )
}

fn sigv4_get_object(
    endpoint: &str,
    bucket: &str,
    key: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> reqwest::Request {
    sigv4_signed_request(
        "GET",
        endpoint,
        &format!("/{bucket}/{key}"),
        "",
        region,
        access_key,
        secret_key,
    )
}

fn sigv4_signed_request(
    method: &str,
    endpoint: &str,
    canonical_uri: &str,
    canonical_querystring: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> reqwest::Request {
    use sha2::Digest;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let amz_date = format_amz_date(now.as_secs());
    let date_stamp = &amz_date[..8];

    let host = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");

    let payload_hash = "UNSIGNED-PAYLOAD";
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_request.as_bytes());
    let canonical_hash = hex::encode(hasher.finalize());

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{region}/s3/aws4_request\n{canonical_hash}"
    );

    let signing_key_initial = format!("AWS4{secret_key}");
    let date_key = hmac_sign_bytes(signing_key_initial.as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sign_bytes(&date_key, region.as_bytes());
    let service_key = hmac_sign_bytes(&region_key, b"s3");
    let signing_key = hmac_sign_bytes(&service_key, b"aws4_request");

    let signature = hex::encode(hmac_sign_bytes(&signing_key, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{date_stamp}/{region}/s3/aws4_request,SignedHeaders={signed_headers},Signature={signature}"
    );

    let url = format!("{endpoint}{canonical_uri}");

    match method {
        "GET" => reqwest::Client::new()
            .get(&url)
            .header("Host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("Authorization", auth)
            .build(),
        _ => reqwest::Client::new()
            .put(&url)
            .header("Host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("Authorization", auth)
            .build(),
    }
    .expect("build signed request")
}

fn hmac_sign_bytes(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn format_amz_date(unix_secs: u64) -> String {
    let days = unix_secs / 86400;
    let (y, m, d) = civil_from_days(days as i64);
    let rem = unix_secs % 86400;
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        y,
        m,
        d,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// 1. Challenge
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_challenge() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;
    let resp: Value = ctx
        .client
        .get(ctx.url("/challenge"))
        .send()
        .await
        .expect("GET /challenge failed")
        .json()
        .await
        .expect("challenge JSON parse");

    assert!(resp["algorithm"].is_string());
    assert_eq!(resp["challenge"].as_str().unwrap().len(), 64);
    assert_eq!(resp["salt"].as_str().unwrap().len(), 64);
    assert_eq!(resp["cost"], 0);
}

// ---------------------------------------------------------------------------
// 2. Spaces + Chains + Deposits
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_spaces_and_deposits() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    // Chains endpoint
    let chains: Value = ctx
        .client
        .get(ctx.url("/chains"))
        .send()
        .await
        .expect("GET /chains failed")
        .json()
        .await
        .expect("chains parse");
    let cs = chains["chains"].as_array().unwrap();
    assert!(cs.iter().any(|c| c["name"] == "anvil"));
    assert!(cs.iter().any(|c| c["name"] == "solana"));
    assert!(cs.iter().any(|c| c["name"] == "bitcoin"));

    // Create space
    let create: Value = ctx
        .client
        .post(ctx.url("/spaces"))
        .json(&serde_json::json!({"metadata": {"name": "e2e-test"}}))
        .send()
        .await
        .expect("POST /spaces failed")
        .json()
        .await
        .expect("create space parse");
    let sid = create["sid"].as_str().unwrap();
    assert_eq!(sid.len(), 24);
    assert!(sid.chars().all(|c| c.is_ascii_digit()));

    // Record deposits on all 3 chains
    for (chain, asset, amount) in [
        ("anvil", "ETH", "2.0"),
        ("solana", "SOL", "5.5"),
        ("bitcoin", "BTC", "0.1"),
    ] {
        let dep: Value = ctx
            .client
            .post(ctx.url(&format!("/spaces/{sid}/deposits")))
            .json(&serde_json::json!({
                "chain": chain,
                "address": "0xDepositAddr",
                "asset": asset,
                "amount": amount,
                "tx_hash": format!("0x{chain}_test")
            }))
            .send()
            .await
            .unwrap_or_else(|_| panic!("POST deposit {chain} failed"))
            .json()
            .await
            .expect("deposit parse");
        assert_eq!(dep["status"], "confirmed");
    }

    // Verify space endpoint consistency
    let space: Value = ctx
        .client
        .get(ctx.url(&format!("/spaces/{sid}")))
        .send()
        .await
        .expect("GET space failed")
        .json()
        .await
        .expect("space parse");

    assert_eq!(space["sid"], sid);
    assert_eq!(space["metadata"]["name"], "e2e-test");
    let deps = space["deposits"].as_array().unwrap();
    assert_eq!(deps.len(), 3);
    assert!(deps.iter().any(|d| d["chain"] == "anvil"));
    assert!(deps.iter().any(|d| d["chain"] == "solana"));
    assert!(deps.iter().any(|d| d["chain"] == "bitcoin"));
    assert!(space["balances"].is_array());
    assert!(chrono::DateTime::parse_from_rfc3339(space["created_at"].as_str().unwrap()).is_ok());

    // List spaces
    let list: Value = ctx
        .client
        .get(ctx.url("/spaces"))
        .send()
        .await
        .expect("GET /spaces failed")
        .json()
        .await
        .expect("list spaces parse");
    assert!(
        list["spaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["sid"] == sid)
    );
}

// ---------------------------------------------------------------------------
// 3. Real Anvil EVM deposit — requires live Anvil RPC
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_real_anvil() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    require_service(&ctx.client, "Anvil", "http://127.0.0.1:8545").await;

    // Verify block number increases (proof of liveness)
    let bn1: Value = ctx
        .client
        .post("http://127.0.0.1:8545")
        .json(&serde_json::json!({
            "jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1
        }))
        .send()
        .await
        .expect("Anvil RPC must be reachable")
        .json()
        .await
        .expect("block number parse");
    let block_1 =
        u64::from_str_radix(bn1["result"].as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
    assert!(block_1 > 0, "Anvil must have mined at least one block");

    // Send a real transaction
    let tx_resp: Value = ctx
        .client
        .post("http://127.0.0.1:8545")
        .json(&serde_json::json!({
            "jsonrpc":"2.0","method":"eth_sendTransaction","params":[{
                "from": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                "to": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "value": "0xDE0B6B3A7640000",
                "gas": "0x5208",
                "gasPrice": "0x77359400"
            }],"id":1
        }))
        .send()
        .await
        .expect("Anvil eth_sendTransaction must succeed")
        .json()
        .await
        .expect("tx response parse");

    let tx_hash = tx_resp["result"]
        .as_str()
        .expect("tx hash must be present in RPC response");
    assert!(tx_hash.starts_with("0x") && tx_hash.len() == 66);

    // Poll for transaction receipt
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let receipt: Value = ctx
            .client
            .post("http://127.0.0.1:8545")
            .json(&serde_json::json!({
                "jsonrpc":"2.0","method":"eth_getTransactionByHash","params":[tx_hash],"id":1
            }))
            .send()
            .await
            .expect("eth_getTransactionByHash must succeed")
            .json()
            .await
            .expect("receipt parse");
        if !receipt["result"].is_null() {
            // Tx confirmed on chain — record deposit
            let create: Value = ctx
                .client
                .post(ctx.url("/spaces"))
                .json(&serde_json::json!({}))
                .send()
                .await
                .expect("POST spaces failed")
                .json()
                .await
                .expect("create space parse");
            let sid = create["sid"].as_str().unwrap();

            let dep: Value = ctx
                .client
                .post(ctx.url(&format!("/spaces/{sid}/deposits")))
                .json(&serde_json::json!({
                    "chain": "anvil",
                    "address": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                    "asset": "ETH",
                    "amount": "1.0",
                    "tx_hash": tx_hash
                }))
                .send()
                .await
                .expect("deposit POST failed")
                .json()
                .await
                .expect("deposit parse");
            assert_eq!(dep["status"], "confirmed");
            assert_eq!(dep["tx_hash"], tx_hash);
            return;
        }
    }

    panic!("Transaction {tx_hash} was not confirmed on Anvil within 5 seconds");
}

// ---------------------------------------------------------------------------
// 4. Real Solana deposit — requires live Solana RPC
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_real_solana() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    require_service(&ctx.client, "Solana", "http://127.0.0.1:8899").await;

    // Verify health
    let health: Value = ctx
        .client
        .post("http://127.0.0.1:8899")
        .json(&serde_json::json!({"jsonrpc":"2.0","method":"getHealth","params":[],"id":1}))
        .send()
        .await
        .expect("Solana RPC must be reachable")
        .json()
        .await
        .expect("health parse");
    assert_eq!(health["result"], "ok", "Solana must report healthy");

    // Request airdrop to get a real signature
    let airdrop: Value = ctx
        .client
        .post("http://127.0.0.1:8899")
        .json(&serde_json::json!({
            "jsonrpc":"2.0","method":"requestAirdrop",
            "params":["11111111111111111111111111111111", 1_000_000_000_u64],
            "id":1
        }))
        .send()
        .await
        .expect("Solana requestAirdrop must succeed")
        .json()
        .await
        .expect("airdrop parse");

    let sig = airdrop["result"]
        .as_str()
        .expect("airdrop must return a transaction signature");
    assert!(!sig.is_empty(), "airdrop signature must not be empty");

    // Poll for confirmation via getSignatureStatuses
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let status: Value = ctx
            .client
            .post("http://127.0.0.1:8899")
            .json(&serde_json::json!({
                "jsonrpc":"2.0","method":"getSignatureStatuses",
                "params":[[sig], {"searchTransactionHistory": true}],
                "id":1
            }))
            .send()
            .await
            .expect("getSignatureStatuses must succeed")
            .json()
            .await
            .expect("status parse");

        if let Some(val) = status["result"]["value"][0]["confirmationStatus"].as_str()
            && (val == "finalized" || val == "confirmed")
        {
            // Record deposit with real signature
            let create: Value = ctx
                .client
                .post(ctx.url("/spaces"))
                .json(&serde_json::json!({}))
                .send()
                .await
                .expect("POST spaces failed")
                .json()
                .await
                .expect("create space parse");
            let sid = create["sid"].as_str().unwrap();

            let dep: Value = ctx
                .client
                .post(ctx.url(&format!("/spaces/{sid}/deposits")))
                .json(&serde_json::json!({
                    "chain": "solana",
                    "address": "11111111111111111111111111111111",
                    "asset": "SOL",
                    "amount": "1.0",
                    "tx_hash": sig
                }))
                .send()
                .await
                .expect("deposit POST failed")
                .json()
                .await
                .expect("deposit parse");
            assert_eq!(dep["status"], "confirmed");
            assert_eq!(dep["tx_hash"], sig);

            // Verify balance increased
            let bal: Value = ctx
                .client
                .post("http://127.0.0.1:8899")
                .json(&serde_json::json!({
                    "jsonrpc":"2.0","method":"getBalance",
                    "params":["11111111111111111111111111111111"],
                    "id":1
                }))
                .send()
                .await
                .expect("getBalance failed")
                .json()
                .await
                .expect("balance parse");
            let balance = bal["result"]["value"].as_u64().unwrap_or(0);
            assert!(
                balance > 0,
                "Solana account must have positive balance after airdrop"
            );
            return;
        }
    }

    panic!("Solana airdrop signature {sig} was not confirmed within 15 seconds");
}

// ---------------------------------------------------------------------------
// 5. Real Bitcoin regtest deposit — requires live Bitcoin RPC
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_real_bitcoin() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    let auth = format!(
        "Basic {}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "shop:shoppass")
    );
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&auth).unwrap(),
    );

    // Require Bitcoin RPC
    let rpc = |method: &str, params: &[Value]| {
        let h = headers.clone();
        let c = &ctx.client;
        let m = method.to_string();
        let p = params.to_vec();
        async move {
            c.post("http://127.0.0.1:18443")
                .headers(h)
                .json(&serde_json::json!({
                    "jsonrpc":"1.0","method":m,"params":p,"id":1
                }))
                .send()
                .await
        }
    };

    // Verify connectivity
    let bc: Value = rpc("getblockchaininfo", &[])
        .await
        .expect("Bitcoin RPC must be reachable")
        .json()
        .await
        .expect("blockchain info parse");
    assert_eq!(
        bc["result"]["chain"], "regtest",
        "Bitcoin must be in regtest mode"
    );

    // Create wallet (ignore if already exists)
    let _ = rpc("createwallet", &[Value::String("e2e-wallet".into())]).await;

    // Generate address
    let addr_v: Value = rpc(
        "getnewaddress",
        &[Value::String("e2e".into()), Value::String("bech32".into())],
    )
    .await
    .expect("getnewaddress must succeed")
    .json()
    .await
    .expect("address parse");
    let addr = addr_v["result"]
        .as_str()
        .expect("address must be present")
        .to_string();
    assert!(!addr.is_empty());

    // Mine blocks to get spendable coins
    let _: Value = rpc(
        "generatetoaddress",
        &[Value::from(101), Value::String(addr.clone())],
    )
    .await
    .expect("generatetoaddress must succeed")
    .json()
    .await
    .expect("mine parse");

    // Verify balance
    let bal: Value = rpc("getbalance", &[])
        .await
        .expect("getbalance must succeed")
        .json()
        .await
        .expect("balance parse");
    let balance = bal["result"].as_f64().unwrap_or(0.0);
    assert!(
        balance > 0.0,
        "must have mined Bitcoin balance > 0, got {balance}"
    );

    // Send BTC to create a real transaction
    let new_addr: Value = rpc(
        "getnewaddress",
        &[
            Value::String("e2e-recv".into()),
            Value::String("bech32".into()),
        ],
    )
    .await
    .expect("getnewaddress 2 must succeed")
    .json()
    .await
    .expect("address 2 parse");
    let send_addr = new_addr["result"].as_str().unwrap();

    let txid: Value = rpc(
        "sendtoaddress",
        &[Value::String(send_addr.into()), Value::from(1.0)],
    )
    .await
    .expect("sendtoaddress must succeed")
    .json()
    .await
    .expect("send parse");
    let tx_hash = txid["result"]
        .as_str()
        .expect("txid must be present")
        .to_string();
    assert!(!tx_hash.is_empty());

    // Mine a block to confirm
    let _: Value = rpc(
        "generatetoaddress",
        &[Value::from(1), Value::String(addr.clone())],
    )
    .await
    .expect("generate confirm block failed")
    .json()
    .await
    .expect("confirm parse");

    // Verify transaction
    let tx_info: Value = rpc("gettransaction", &[Value::String(tx_hash.clone())])
        .await
        .expect("gettransaction must succeed")
        .json()
        .await
        .expect("tx info parse");
    assert!(
        tx_info["result"]["confirmations"].as_u64().unwrap_or(0) >= 1,
        "transaction must be confirmed"
    );

    // Record deposit via shop
    let create: Value = ctx
        .client
        .post(ctx.url("/spaces"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("POST spaces failed")
        .json()
        .await
        .expect("create space parse");
    let sid = create["sid"].as_str().unwrap();

    let dep: Value = ctx
        .client
        .post(ctx.url(&format!("/spaces/{sid}/deposits")))
        .json(&serde_json::json!({
            "chain": "bitcoin",
            "address": send_addr,
            "asset": "BTC",
            "amount": "1.0",
            "tx_hash": tx_hash
        }))
        .send()
        .await
        .expect("deposit POST failed")
        .json()
        .await
        .expect("deposit parse");
    assert_eq!(dep["status"], "confirmed");
    assert_eq!(dep["tx_hash"], tx_hash);
}

// ---------------------------------------------------------------------------
// 6. Rates
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_rates() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;
    let resp: Value = ctx
        .client
        .get(ctx.url("/rates"))
        .send()
        .await
        .expect("GET /rates failed")
        .json()
        .await
        .expect("rates parse");
    assert_eq!(resp["rates"]["ETH"], 3000.0);
    assert_eq!(resp["rates"]["USDC"], 1.0);
    assert_eq!(resp["source"], "static");
}

// ---------------------------------------------------------------------------
// 7. Presigned upload to RustFS — real bucket, real upload, real verify
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_presigned_upload() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    require_service(&ctx.client, "RustFS", "http://127.0.0.1:9000").await;

    // Ensure the bucket exists (create it via SigV4-signed PUT if needed)
    let bucket_req = sigv4_put_bucket(
        "http://127.0.0.1:9000",
        "shop-e2e-uploads",
        "us-east-1",
        "rustfsadmin",
        "rustfsadmin",
    );
    let bucket_resp = ctx.client.execute(bucket_req).await;
    match bucket_resp {
        Ok(r) => {
            let status = r.status();
            // 200 = bucket already exists, 201/204 = created
            assert!(
                status == 200 || status == 201 || status == 204 || status == 409,
                "bucket creation returned unexpected status {status}"
            );
        }
        Err(e) => panic!("SigV4 bucket creation request failed: {e}"),
    }

    // Get presigned POST from shop
    let presigned: Value = ctx
        .client
        .post(ctx.url("/upload"))
        .json(&serde_json::json!({
            "key": "e2e-test-upload.txt",
            "content_type": "text/plain",
            "max_size_bytes": 1048576
        }))
        .send()
        .await
        .expect("POST /upload failed")
        .json()
        .await
        .expect("presigned parse");

    let fields = presigned["fields"]
        .as_object()
        .expect("fields must be object");
    assert!(fields.contains_key("key"));
    assert!(fields.contains_key("x-amz-algorithm"));
    assert!(fields.contains_key("x-amz-credential"));
    assert!(fields.contains_key("x-amz-date"));
    assert!(fields.contains_key("policy"));
    assert!(fields.contains_key("x-amz-signature"));
    assert_eq!(fields["key"], "e2e-test-upload.txt");

    // Validate SigV4 field format
    assert_eq!(fields["x-amz-algorithm"], "AWS4-HMAC-SHA256");
    assert!(
        fields["x-amz-credential"]
            .as_str()
            .unwrap()
            .starts_with("rustfsadmin")
    );
    assert_eq!(fields["x-amz-signature"].as_str().unwrap().len(), 64);

    // Validate policy JSON
    let policy_str = fields["policy"].as_str().unwrap();
    let policy_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, policy_str)
            .expect("policy must be valid base64");
    let policy: Value = serde_json::from_slice(&policy_bytes).expect("policy must be valid JSON");
    assert!(policy["expiration"].is_string());
    assert!(policy["conditions"].is_array());

    let url = presigned["url"].as_str().unwrap();
    assert!(url.contains("127.0.0.1:9000"));
    assert!(url.contains("shop-e2e-uploads"));

    // Upload real fixture content
    let test_content = "Hello, Shop E2E presigned upload!\nLine 2.\nLine 3.\n";
    let mut form = reqwest::multipart::Form::new();
    for (k, v) in fields {
        form = form.text(k.clone(), v.as_str().unwrap_or("").to_string());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::text(test_content.to_string())
            .file_name("e2e-test-upload.txt")
            .mime_str("text/plain")
            .unwrap(),
    );

    let upload_resp = ctx
        .client
        .post(url)
        .multipart(form)
        .send()
        .await
        .expect("presigned POST upload request must succeed");

    let status = upload_resp.status();
    assert!(
        status == 200 || status == 201 || status == 204,
        "upload must succeed, got HTTP {status}: {}",
        upload_resp.text().await.unwrap_or_default()
    );

    // Verify the object exists via authenticated S3 GET (private bucket)
    let get_req = sigv4_get_object(
        "http://127.0.0.1:9000",
        "shop-e2e-uploads",
        "e2e-test-upload.txt",
        "us-east-1",
        "rustfsadmin",
        "rustfsadmin",
    );
    let get_resp = ctx
        .client
        .execute(get_req)
        .await
        .expect("authenticated GET object request must succeed");

    assert!(
        get_resp.status().is_success(),
        "object must be retrievable with authentication, got {}",
        get_resp.status()
    );

    let body = get_resp.text().await.expect("object body read");
    assert_eq!(
        body, test_content,
        "uploaded object content must match original"
    );
}

// ---------------------------------------------------------------------------
// 8. Full task workflow: idempotency, SSE, SQLite persistence
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires E2E services"]
async fn e2e_full_task_workflow() {
    if !e2e_enabled() {
        return;
    }
    let ctx = E2EContext::new().await;

    // List kinds
    let tasks: Value = ctx
        .client
        .get(ctx.url("/tasks"))
        .send()
        .await
        .expect("GET /tasks failed")
        .json()
        .await
        .expect("tasks parse");
    let kinds = tasks["kinds"].as_array().unwrap();
    assert!(kinds.iter().any(|k| k["slug"] == "process.file"));

    // Create space
    let create: Value = ctx
        .client
        .post(ctx.url("/spaces"))
        .json(&serde_json::json!({"metadata": {"task_test": true}}))
        .send()
        .await
        .expect("POST spaces failed")
        .json()
        .await
        .expect("create space parse");
    let sid = create["sid"].as_str().unwrap();

    // Record a deposit
    let _: Value = ctx
        .client
        .post(ctx.url(&format!("/spaces/{sid}/deposits")))
        .json(&serde_json::json!({
            "chain": "anvil",
            "address": "0xTaskTest",
            "asset": "ETH",
            "amount": "0.5",
            "tx_hash": "0xworkflow"
        }))
        .send()
        .await
        .expect("deposit failed")
        .json()
        .await
        .unwrap();

    let idemp_key = format!("e2e-wf-{}", uuid::Uuid::new_v4());
    let task_body = serde_json::json!({
        "sid": sid,
        "input": {"file_url": "tests/e2e/fixtures/test-input.txt"},
        "idempotency_key": idemp_key,
    });

    // Submit task
    let task_resp: Value = ctx
        .client
        .post(ctx.url("/tasks/process.file"))
        .json(&task_body)
        .send()
        .await
        .expect("POST task failed")
        .json()
        .await
        .expect("task parse");
    let tid = task_resp["tid"].as_str().unwrap();
    assert_eq!(task_resp["kind"], "process.file");
    assert_eq!(task_resp["status"], "pending");

    // Idempotency replay
    let cached: Value = ctx
        .client
        .post(ctx.url("/tasks/process.file"))
        .json(&task_body)
        .send()
        .await
        .expect("idempotent replay failed")
        .json()
        .await
        .expect("cached parse");
    assert!(cached["cached"].as_bool().unwrap_or(false));
    assert_eq!(cached["response"]["tid"], tid);

    // SSE stream — collect events
    let sse_resp = ctx
        .client
        .get(ctx.url(&format!("/tasks/process.file/{tid}/events")))
        .send()
        .await
        .expect("SSE GET failed");
    assert!(sse_resp.status().is_success());

    let mut stream = sse_resp.bytes_stream().eventsource();
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        if tokio::time::Instant::now() > deadline {
            break;
        }
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(ev))) => {
                let is_done = ev.event == "done";
                events.push(ev.event);
                if is_done {
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(!events.is_empty(), "must receive SSE events");
    assert!(
        events.iter().any(|e| e == "task_event"),
        "must have task_event type"
    );

    // Final task status
    let final_resp: Value = ctx
        .client
        .get(ctx.url(&format!("/tasks/process.file/{tid}")))
        .send()
        .await
        .expect("GET task status failed")
        .json()
        .await
        .expect("task status parse");

    let status = final_resp["status"].as_str().unwrap_or("unknown");
    assert!(
        matches!(status, "completed" | "running" | "pending" | "failed"),
        "task status must be valid: {status}"
    );

    // SQLite persistence verification
    let dbp = std::path::Path::new(&ctx.db_path);
    assert!(dbp.exists(), "SQLite database file must exist");

    let conn = rusqlite::Connection::open(dbp).expect("must open DB");

    let space_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM spaces WHERE sid = ?1", [sid], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(space_count, 1, "space must be in DB");

    let deposit_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM deposits WHERE sid = ?1", [sid], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(deposit_count > 0, "deposits must be in DB");

    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM jobs WHERE tid = ?1", [tid], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(job_count, 1, "job must be in DB");

    let idem_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM idempotency WHERE sid = ?1",
            [sid],
            |r| r.get(0),
        )
        .unwrap();
    assert!(idem_count > 0, "idempotency records must be in DB");

    let events_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM task_events WHERE tid = ?1",
            [tid],
            |r| r.get(0),
        )
        .unwrap();
    assert!(events_count > 0, "task_events must be in DB");

    // Kind-filtered listing
    let kind_list: Value = ctx
        .client
        .get(format!("{}?sid={sid}", ctx.url("/tasks/process.file")))
        .send()
        .await
        .expect("kind list failed")
        .json()
        .await
        .expect("kind list parse");
    assert!(kind_list["jobs"].is_array());
}
