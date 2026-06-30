//! Challenge generation and verification (ALTCHA-style) and in-memory
//! rate limiting.

use crate::config::ChallengeConfig;
use crate::error::{Result, ShopError};
use hmac::KeyInit;
use hmac::{Hmac, Mac};
use rand::RngExt;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Challenge
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// A generated challenge sent to the client.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Challenge {
    pub algorithm: String,
    pub challenge: String,
    pub salt: String,
    pub cost: u32,
    pub expires: String,
}

/// Generate a new challenge for the given configuration.
pub fn generate_challenge(cfg: &ChallengeConfig) -> Challenge {
    let salt = random_hex(32);
    let expires = chrono::Utc::now() + chrono::Duration::seconds(cfg.ttl_secs as i64);
    let expires_str = expires.to_rfc3339();
    let raw = format!("{}{}", salt, expires_str);

    let mut mac = HmacSha256::new_from_slice(cfg.secret.as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(raw.as_bytes());
    let challenge = hex::encode(mac.finalize().into_bytes());

    Challenge {
        algorithm: cfg.algorithm.clone(),
        challenge,
        salt,
        cost: cfg.cost,
        expires: expires_str,
    }
}

/// Verify that a solution matches the challenge.  The solution is the
/// hex-encoded HMAC the client computed locally and returned to us.
pub fn verify_challenge(
    cfg: &ChallengeConfig,
    salt: &str,
    expires: &str,
    solution: &str,
) -> Result<bool> {
    // Check expiry
    let expires_dt = chrono::DateTime::parse_from_rfc3339(expires)
        .map_err(|e| ShopError::BadRequest(format!("invalid expires format: {e}")))?;
    if expires_dt < chrono::Utc::now() {
        return Ok(false);
    }

    let raw = format!("{salt}{expires}");
    let mut mac = HmacSha256::new_from_slice(cfg.secret.as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(raw.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    Ok(expected == solution)
}

fn random_hex(len_bytes: usize) -> String {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..len_bytes).map(|_| rng.random::<u8>()).collect();
    hex::encode(&bytes)
}

// ---------------------------------------------------------------------------
// Rate limiter — in-memory token bucket per key
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    rate: f64, // tokens per second
}

impl TokenBucket {
    fn new(capacity: u32, window_secs: u64) -> Self {
        let capacity = capacity as f64;
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            capacity,
            rate: capacity / window_secs as f64,
        }
    }

    /// Attempt to consume one token. Returns `true` if allowed.
    fn try_consume(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<HashMap<String, TokenBucket>>>,
    capacity: u32,
    window_secs: u64,
}

impl RateLimiter {
    pub fn new(capacity: u32, window_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            capacity,
            window_secs,
        }
    }

    /// Check if the given key is allowed. Returns `true` if allowed.
    pub async fn check(&self, key: &str) -> bool {
        let mut map = self.inner.lock().await;
        let bucket = map
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(self.capacity, self.window_secs));
        bucket.try_consume(Instant::now())
    }

    /// Periodically prune stale entries.
    pub async fn prune(&self, max_age: Duration) {
        let mut map = self.inner.lock().await;
        let now = Instant::now();
        map.retain(|_, bucket| now.duration_since(bucket.last_refill) < max_age);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_challenge_config() -> ChallengeConfig {
        ChallengeConfig {
            secret: "test-secret-key-12345".into(),
            ttl_secs: 600,
            cost: 5000,
            algorithm: "PBKDF2/SHA-256".into(),
        }
    }

    #[test]
    fn challenge_generate_and_verify() {
        let cfg = test_challenge_config();
        let challenge = generate_challenge(&cfg);
        assert_eq!(challenge.algorithm, "PBKDF2/SHA-256");
        assert_eq!(challenge.cost, 5000);
        assert_eq!(challenge.salt.len(), 64); // 32 bytes hex
        assert_eq!(challenge.challenge.len(), 64); // SHA-256 hex

        // Verification with the correct challenge should succeed
        let valid = verify_challenge(
            &cfg,
            &challenge.salt,
            &challenge.expires,
            &challenge.challenge,
        );
        assert!(valid.unwrap());
    }

    #[test]
    fn challenge_verify_wrong_solution_fails() {
        let cfg = test_challenge_config();
        let challenge = generate_challenge(&cfg);
        let valid = verify_challenge(&cfg, &challenge.salt, &challenge.expires, "deadbeef");
        assert!(!valid.unwrap());
    }

    #[test]
    fn challenge_verify_expired_fails() {
        let cfg = test_challenge_config();
        let challenge = generate_challenge(&cfg);
        let expired = "2020-01-01T00:00:00+00:00";
        let valid = verify_challenge(&cfg, &challenge.salt, expired, &challenge.challenge);
        assert!(!valid.unwrap());
    }

    #[test]
    fn challenge_salt_randomness() {
        let cfg = test_challenge_config();
        let c1 = generate_challenge(&cfg);
        let c2 = generate_challenge(&cfg);
        assert_ne!(c1.salt, c2.salt);
        assert_ne!(c1.challenge, c2.challenge);
    }

    #[tokio::test]
    async fn rate_limiter_allows_and_blocks() {
        let rl = RateLimiter::new(3, 1); // 3 tokens per second
        let key = "test-client";

        // First 3 should be allowed
        assert!(rl.check(key).await);
        assert!(rl.check(key).await);
        assert!(rl.check(key).await);

        // 4th should be blocked
        assert!(!rl.check(key).await);
    }

    #[tokio::test]
    async fn rate_limiter_different_keys_independent() {
        let rl = RateLimiter::new(1, 60);
        assert!(rl.check("a").await);
        assert!(!rl.check("a").await);
        assert!(rl.check("b").await);
    }
}
