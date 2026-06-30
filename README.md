# Shop

Config-driven API backend with spaces, tasks, uploads, and package orchestration.

## Quick Start

```bash
cargo install shop
shop --config Config.toml
```

Or load config from a URL:

```bash
shop --config https://example.com/Config.toml
```

## Configuration

Copy `Config.example.toml` to `Config.toml` and customize:

- `[shop.challenge]` — ALTCHA-style HMAC-SHA256 challenge for task submission.
- `[shop.rates]` — Static rates or proxy an external HTTP source.
- `[shop.storage]` — S3-compatible storage (RustFS/MinIO/AWS) for presigned uploads.
- `[[shop.kinds]]` — Task kinds with command steps and pricing.
- `[shop.packages.<name>]` — Launch companion packages alongside Shop.

## API Endpoints

Default prefix: `/v1` (configurable via `[shop.server.prefix]` or `[http.prefix]`).

| Method | Path                        | Description                                  |
|--------|-----------------------------|----------------------------------------------|
| GET    | `/v1/challenge`             | Request a challenge for PoW verification     |
| POST   | `/v1/spaces`                | Create a new billing space                   |
| GET    | `/v1/spaces`                | List all spaces                              |
| GET    | `/v1/spaces/{sid}`          | Get space with deposits and balances         |
| GET    | `/v1/rates`                 | Get current rates (static or proxied)        |
| POST   | `/v1/upload`                | Get a presigned S3 POST upload policy        |
| GET    | `/v1/tasks`                 | List available task kinds                    |
| GET    | `/v1/tasks/{kind}`          | List jobs for a kind (query: `?sid=...`)     |
| POST   | `/v1/tasks/{kind}`          | Submit a new task                            |
| GET    | `/v1/tasks/{kind}/{tid}`    | Get job status and result                    |
| GET    | `/v1/tasks/{kind}/{tid}/events` | SSE stream of task events                |

### Error Format

All errors return JSON with `x-request-id` header:

```json
{
  "error": {
    "code": "not_found",
    "message": "space ... not found",
    "request_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

## License

MIT
