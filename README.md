# Shop

Config-driven API backend with spaces, tasks, uploads, and package orchestration.

[![CI](https://github.com/melonask/shop/actions/workflows/ci.yml/badge.svg)](https://github.com/melonask/shop/actions/workflows/ci.yml)

## Quick Start

```bash
cargo install shop
shop --config Config.toml
```

Or load config from a URL:

```bash
shop --config https://example.com/Config.toml
```

## Architecture

### Configuration Model

Shop reads a single `Config.toml` that follows a shared schema (`version = 1`):

| Section | Purpose |
|---------|---------|
| `[log]` | Logging level, format, optional file output |
| `[runtime]` | Worker threads, shutdown timeout, max payload |
| `[http]` | Default bind/port/prefix/body-limit shared across packages |
| `[stores.<name>]` | Database connections; shop uses `sqlite` driver |
| `[shop]` | Shop-specific configuration namespace |
| `[ladon]`, `[bria]`, `[pano]`, `[oracles]`, `[artur]` | Tolerated sibling package sections |

The `[shop]` section merges with shared `[http]`/`[runtime]`/`[stores]`:
- `[shop.server]` overrides `[http]` bind/port/prefix/body-limit
- `[shop.challenge]` — ALTCHA-style HMAC-SHA256 challenge
- `[shop.rates]` — Static or proxied exchange rates
- `[shop.storage]` — S3-compatible storage (RustFS/MinIO/AWS)
- `[shop.idempotency]` — Idempotency key TTL
- `[shop.rate_limit]` — Per-IP rate limiting
- `[[shop.kinds]]` — Task kind definitions with steps
- `[shop.packages.<name>]` — Companion package launchers
- `[shop.chains.<name>]` — Blockchain deposit configurations

### Endpoint Flow

Default prefix `/v1` (configurable):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/challenge` | Request a challenge (HMAC-SHA256 PoW) |
| `GET` | `/v1/chains` | List configured blockchain chains and deposit addresses |
| `POST` | `/v1/spaces` | Create a new billing space (24-digit CSPRNG sid) |
| `GET` | `/v1/spaces` | List all spaces |
| `GET` | `/v1/spaces/{sid}` | Get space with deposits and balances |
| `POST` | `/v1/spaces/{sid}/deposits` | Record a blockchain deposit for a space |
| `GET` | `/v1/rates` | Current rates (static or proxied) |
| `POST` | `/v1/upload` | Presigned S3 POST upload policy (SigV4) |
| `GET` | `/v1/tasks` | List available task kinds |
| `GET` | `/v1/tasks/{kind}` | List jobs for a kind (`?sid=...`) |
| `POST` | `/v1/tasks/{kind}` | Submit a task (supports idempotency) |
| `GET` | `/v1/tasks/{kind}/{tid}` | Job status and result |
| `GET` | `/v1/tasks/{kind}/{tid}/events` | SSE stream of job events |

### Challenge / Idempotency / Rate Limit / Security

- **Challenge**: `GET /v1/challenge` returns `{algorithm, challenge, salt, cost, expires}`. The challenge is `HMAC-SHA256(secret, salt||expires)`. Clients solve locally and submit the hex solution with task POSTs. Configurable via `[shop.challenge]`.
- **Idempotency**: Task submissions with an `idempotency_key` are stored in SQLite. Duplicate submissions return the cached response. Records auto-purge after TTL (`[shop.idempotency].ttl_secs`).
- **Rate Limiting**: In-memory token-bucket per client IP (via `X-Forwarded-For` or `X-Real-IP`). Configurable via `[shop.rate_limit]`.
- **Error Format**: All errors return `{"error":{"code","message","request_id"}}` with `x-request-id` header.

### Storage / RustFS

Uploads use AWS Signature Version 4 presigned POST policies:
1. Client calls `POST /v1/upload` with desired `key`, `content_type`, and optional `metadata`.
2. Shop returns a signed POST form (`url` + `fields` map) with `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`, `policy` (base64 JSON), and `x-amz-signature`.
3. Client POSTs the file directly to the S3-compatible endpoint using the form fields.
4. Compatible with RustFS, MinIO, AWS S3, and any SigV4-compatible object store.

### Task Execution

Tasks are defined as `[[shop.kinds]]` with:
- `slug` — unique kind identifier (e.g., `process.file`)
- `price_cents` — cost in USD cents (0 = free)
- `steps` — ordered command steps with dependency graph

Step execution:
- Steps with empty `depends_on` run first, in parallel up to `concurrency`.
- Dependent steps wait for predecessors; outputs injected via `STEP_OUTPUT_*` env vars.
- Each step has configurable timeout, allowed exit codes, and stdout/stderr limits.
- Events published via `tokio::sync::broadcast` for SSE streaming.

### Package Launching

Companion packages configured in `[shop.packages.<name>]` are spawned as child processes when Shop starts. Each receives `--config <same config>` so they share the same configuration file. Restart-on-exit with configurable delay is supported.

### Database Schema

SQLite tables (auto-migrated on startup):

| Table | Purpose |
|-------|---------|
| `spaces` | Billing spaces (sid, created_at, metadata) |
| `deposits` | Blockchain deposits (chain, address, asset, amount, tx_hash, status) |
| `balances` | Per-space asset balances (sid, asset, amount) |
| `jobs` | Task jobs (tid, sid, kind, status, input, result, price_cents) |
| `idempotency` | Idempotency records (key, sid, kind, response, created_at) |
| `task_events` | SSE-able task events (tid, status, step_id, data, created_at) |

## Development

### Unit Tests

```bash
cargo test
```

### Linting

```bash
cargo fmt --check
cargo clippy --tests --examples -- -D warnings
```

### Publishing

Publishing to crates.io is handled by GitHub Actions when a GitHub Release is published, or manually from the `Publish` workflow.

Required repository secret:
- `CARGO_REGISTRY_TOKEN` — crates.io API token with publish permission for this crate.

The workflow runs `cargo publish --dry-run --locked` before uploading the crate.

### Pre-commit Hook

```bash
bash scripts/install-git-hooks.sh
```

This installs a pre-commit hook that runs `fmt`, `clippy`, and `test`.

### E2E Tests

E2E tests exercise real external services via Docker Compose:

```bash
# Start services
docker compose -f tests/e2e/docker-compose.yml up -d

# Run e2e tests
SHOP_E2E=1 cargo test --test e2e -- --ignored --nocapture

# Stop services
docker compose -f tests/e2e/docker-compose.yml down -v
```

The Docker Compose stack includes:
- **RustFS** (S3-compatible storage on port 9000)
- **Anvil** (Foundry EVM localnet on port 8545)
- **Solana test validator** (on port 8899)
- **Bitcoin Core regtest** (on port 18443)

### E2E Test Coverage

| Test | What it verifies |
|------|-----------------|
| `e2e_challenge` | Challenge generation, algorithm, salt, format |
| `e2e_spaces` | Space creation, retrieval, listing, sid format |
| `e2e_chains` | Chain configuration endpoint |
| `e2e_deposits` | Deposit recording and retrieval |
| `e2e_real_anvil` | Real Anvil EVM transaction + deposit recording |
| `e2e_real_solana` | Real Solana airdrop + deposit recording |
| `e2e_real_bitcoin` | Real Bitcoin regtest mining + deposit recording |
| `e2e_rates` | Configured static rates values |
| `e2e_presigned_upload` | SigV4 presigned POST + RustFS upload + retrieval |
| `e2e_task_kinds` | Task kind listing |
| `e2e_task_idempotent_and_sse` | Idempotency, SSE streaming, task status |
| `e2e_sqlite_persistence` | Direct SQLite table verification |
| `e2e_space_consistency` | Space endpoint deposits/balances/created_at consistency |

## License

MIT
