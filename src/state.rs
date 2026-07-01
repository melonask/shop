use crate::config::AppConfig;
use crate::error::{Result, ShopError};
use crate::orchestrator::EventBus;
use crate::security::RateLimiter;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<Mutex<Connection>>,
    pub db_path: PathBuf,
    pub bus: EventBus,
    pub rate_limiter: RateLimiter,
}

impl AppState {
    /// Create a new application state, initializing the SQLite database.
    pub async fn new(config: AppConfig, db_path: PathBuf) -> Result<Self> {
        if let Some(parent) = db_path.parent()
            && !parent.as_os_str().is_empty()
            && parent.as_os_str() != "."
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(ShopError::Io)?;
        }

        let conn = Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;

        let rate_limiter = RateLimiter::new(
            config.shop.rate_limit.capacity,
            config.shop.rate_limit.window_secs,
        );

        let state = Self {
            config: Arc::new(config),
            db: Arc::new(Mutex::new(conn)),
            db_path,
            bus: EventBus::new(),
            rate_limiter,
        };
        state.migrate().await?;
        Ok(state)
    }

    /// Run schema migrations.
    async fn migrate(&self) -> Result<()> {
        let db = self.db.lock().await;
        db.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS spaces (
                sid         TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL,
                metadata    TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS deposits (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sid         TEXT NOT NULL REFERENCES spaces(sid),
                chain       TEXT NOT NULL,
                address     TEXT NOT NULL,
                asset       TEXT NOT NULL,
                amount      TEXT NOT NULL,
                tx_hash     TEXT NOT NULL DEFAULT '',
                status      TEXT NOT NULL DEFAULT 'pending',
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_deposits_sid ON deposits(sid);

            CREATE TABLE IF NOT EXISTS balances (
                sid         TEXT NOT NULL REFERENCES spaces(sid),
                asset       TEXT NOT NULL,
                amount      TEXT NOT NULL DEFAULT '0',
                updated_at  TEXT NOT NULL,
                PRIMARY KEY (sid, asset)
            );

            CREATE TABLE IF NOT EXISTS jobs (
                tid         TEXT PRIMARY KEY,
                sid         TEXT NOT NULL REFERENCES spaces(sid),
                kind        TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                input       TEXT NOT NULL DEFAULT '{}',
                result      TEXT,
                price_cents INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_sid ON jobs(sid);
            CREATE INDEX IF NOT EXISTS idx_jobs_kind ON jobs(kind);

            CREATE TABLE IF NOT EXISTS idempotency (
                key         TEXT PRIMARY KEY,
                sid         TEXT NOT NULL,
                kind        TEXT NOT NULL,
                response    TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_idempotency_expiry ON idempotency(created_at);

            CREATE TABLE IF NOT EXISTS task_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                tid         TEXT NOT NULL REFERENCES jobs(tid),
                status      TEXT NOT NULL,
                step_id     TEXT NOT NULL DEFAULT '',
                data        TEXT NOT NULL DEFAULT '{}',
                created_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_task_events_tid ON task_events(tid);
            ",
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Space operations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Space {
    pub sid: String,
    pub created_at: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deposit {
    pub id: i64,
    pub sid: String,
    pub chain: String,
    pub address: String,
    pub asset: String,
    pub amount: String,
    pub tx_hash: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub sid: String,
    pub asset: String,
    pub amount: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub tid: String,
    pub sid: String,
    pub kind: String,
    pub status: String,
    pub input: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub price_cents: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub id: i64,
    pub tid: String,
    pub status: String,
    pub step_id: String,
    pub data: serde_json::Value,
    pub created_at: String,
}

/// Generate a 24-digit secret random sid using CSPRNG.
pub fn generate_sid() -> String {
    use rand::RngExt;
    let mut rng = rand::rng();
    let mut sid = String::with_capacity(24);
    for _ in 0..24 {
        sid.push(char::from(b'0' + rng.random_range(0..10)));
    }
    sid
}

/// Generate a ULID for task IDs.
pub fn generate_tid() -> String {
    ulid::Ulid::new().to_string()
}

impl AppState {
    /// Create a new space.
    pub async fn create_space(&self, metadata: serde_json::Value) -> Result<Space> {
        let sid = generate_sid();
        let now = chrono::Utc::now().to_rfc3339();
        let metadata_str = serde_json::to_string(&metadata)?;
        let db = self.db.lock().await;
        db.execute(
            "INSERT INTO spaces (sid, created_at, metadata) VALUES (?1, ?2, ?3)",
            rusqlite::params![sid, now, metadata_str],
        )?;
        Ok(Space {
            sid,
            created_at: now,
            metadata,
        })
    }

    /// Get a space by sid.
    pub async fn get_space(&self, sid: &str) -> Result<Option<Space>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare("SELECT sid, created_at, metadata FROM spaces WHERE sid = ?1")?;
        let mut rows = stmt.query(rusqlite::params![sid])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Space {
                sid: row.get(0)?,
                created_at: row.get(1)?,
                metadata: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    }

    /// List all spaces.
    pub async fn list_spaces(&self) -> Result<Vec<Space>> {
        let db = self.db.lock().await;
        let mut stmt =
            db.prepare("SELECT sid, created_at, metadata FROM spaces ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], |row| {
            Ok(Space {
                sid: row.get(0)?,
                created_at: row.get(1)?,
                metadata: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
            })
        })?;
        let mut spaces = Vec::new();
        for space in rows {
            spaces.push(space?);
        }
        Ok(spaces)
    }

    /// List deposits for a space.
    pub async fn get_deposits(&self, sid: &str) -> Result<Vec<Deposit>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT id, sid, chain, address, asset, amount, tx_hash, status, created_at, updated_at
             FROM deposits WHERE sid = ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(rusqlite::params![sid], |row| {
            Ok(Deposit {
                id: row.get(0)?,
                sid: row.get(1)?,
                chain: row.get(2)?,
                address: row.get(3)?,
                asset: row.get(4)?,
                amount: row.get(5)?,
                tx_hash: row.get(6)?,
                status: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        })?;
        let mut deposits = Vec::new();
        for deposit in rows {
            deposits.push(deposit?);
        }
        Ok(deposits)
    }

    /// Record a deposit for a space.
    pub async fn record_deposit(
        &self,
        sid: &str,
        chain: &str,
        address: &str,
        asset: &str,
        amount: &str,
        tx_hash: &str,
    ) -> Result<Deposit> {
        let now = chrono::Utc::now().to_rfc3339();
        let db = self.db.lock().await;
        db.execute(
            "INSERT INTO deposits (sid, chain, address, asset, amount, tx_hash, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'confirmed', ?7, ?8)",
            rusqlite::params![sid, chain, address, asset, amount, tx_hash, now, now],
        )?;
        let id = db.last_insert_rowid();
        Ok(Deposit {
            id,
            sid: sid.to_string(),
            chain: chain.to_string(),
            address: address.to_string(),
            asset: asset.to_string(),
            amount: amount.to_string(),
            tx_hash: tx_hash.to_string(),
            status: "confirmed".to_string(),
            created_at: now.clone(),
            updated_at: now,
        })
    }

    /// Get balances for a space.
    pub async fn get_balances(&self, sid: &str) -> Result<Vec<Balance>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT sid, asset, amount, updated_at FROM balances WHERE sid = ?1 ORDER BY asset",
        )?;
        let rows = stmt.query_map(rusqlite::params![sid], |row| {
            Ok(Balance {
                sid: row.get(0)?,
                asset: row.get(1)?,
                amount: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        let mut balances = Vec::new();
        for balance in rows {
            balances.push(balance?);
        }
        Ok(balances)
    }

    /// Create a new job.
    pub async fn create_job(
        &self,
        sid: &str,
        kind: &str,
        input: &serde_json::Value,
        price_cents: i64,
    ) -> Result<Job> {
        let tid = generate_tid();
        let now = chrono::Utc::now().to_rfc3339();
        let input_str = serde_json::to_string(input)?;
        let db = self.db.lock().await;
        db.execute(
            "INSERT INTO jobs (tid, sid, kind, status, input, price_cents, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'pending', ?4, ?5, ?6, ?7)",
            rusqlite::params![tid, sid, kind, input_str, price_cents, now, now],
        )?;
        Ok(Job {
            tid,
            sid: sid.to_string(),
            kind: kind.to_string(),
            status: "pending".to_string(),
            input: input.clone(),
            result: None,
            price_cents,
            created_at: now.clone(),
            updated_at: now,
        })
    }

    /// Get a job by tid.
    pub async fn get_job(&self, tid: &str) -> Result<Option<Job>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT tid, sid, kind, status, input, result, price_cents, created_at, updated_at
             FROM jobs WHERE tid = ?1",
        )?;
        let mut rows = stmt.query(rusqlite::params![tid])?;
        if let Some(row) = rows.next()? {
            let result_str: Option<String> = row.get(5)?;
            Ok(Some(Job {
                tid: row.get(0)?,
                sid: row.get(1)?,
                kind: row.get(2)?,
                status: row.get(3)?,
                input: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                result: result_str.and_then(|s| serde_json::from_str(&s).ok()),
                price_cents: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// List jobs for a space, optionally filtered by kind.
    pub async fn get_jobs(&self, sid: &str, kind: Option<&str>) -> Result<Vec<Job>> {
        let db = self.db.lock().await;
        let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(k) = kind {
            (
                "SELECT tid, sid, kind, status, input, result, price_cents, created_at, updated_at
                 FROM jobs WHERE sid = ?1 AND kind = ?2 ORDER BY created_at DESC",
                vec![Box::new(sid.to_string()), Box::new(k.to_string())],
            )
        } else {
            (
                "SELECT tid, sid, kind, status, input, result, price_cents, created_at, updated_at
                 FROM jobs WHERE sid = ?1 ORDER BY created_at DESC",
                vec![Box::new(sid.to_string())],
            )
        };
        let mut stmt = db.prepare(sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let result_str: Option<String> = row.get(5)?;
            Ok(Job {
                tid: row.get(0)?,
                sid: row.get(1)?,
                kind: row.get(2)?,
                status: row.get(3)?,
                input: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                result: result_str.and_then(|s| serde_json::from_str(&s).ok()),
                price_cents: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            })
        })?;
        let mut jobs = Vec::new();
        for job in rows {
            jobs.push(job?);
        }
        Ok(jobs)
    }

    /// Update job status.
    pub async fn update_job_status(
        &self,
        tid: &str,
        status: &str,
        result: Option<&serde_json::Value>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let result_str = result.map(serde_json::to_string).transpose()?;
        let db = self.db.lock().await;
        db.execute(
            "UPDATE jobs SET status = ?1, result = ?2, updated_at = ?3 WHERE tid = ?4",
            rusqlite::params![status, result_str, now, tid],
        )?;
        Ok(())
    }

    /// Record a task event (for SSE streaming).
    pub async fn record_task_event(
        &self,
        tid: &str,
        status: &str,
        step_id: &str,
        data: &serde_json::Value,
    ) -> Result<TaskEvent> {
        let now = chrono::Utc::now().to_rfc3339();
        let data_str = serde_json::to_string(data)?;
        let db = self.db.lock().await;
        db.execute(
            "INSERT INTO task_events (tid, status, step_id, data, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![tid, status, step_id, data_str, now],
        )?;
        let id = db.last_insert_rowid();
        Ok(TaskEvent {
            id,
            tid: tid.to_string(),
            status: status.to_string(),
            step_id: step_id.to_string(),
            data: data.clone(),
            created_at: now,
        })
    }

    /// Get events for a job.
    pub async fn get_task_events(&self, tid: &str) -> Result<Vec<TaskEvent>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT id, tid, status, step_id, data, created_at FROM task_events WHERE tid = ?1 ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![tid], |row| {
            Ok(TaskEvent {
                id: row.get(0)?,
                tid: row.get(1)?,
                status: row.get(2)?,
                step_id: row.get(3)?,
                data: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                created_at: row.get(5)?,
            })
        })?;
        let mut events = Vec::new();
        for event in rows {
            events.push(event?);
        }
        Ok(events)
    }

    // -----------------------------------------------------------------------
    // Idempotency
    // -----------------------------------------------------------------------

    /// Check if an idempotency key exists and return the stored response.
    pub async fn check_idempotency(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare("SELECT response FROM idempotency WHERE key = ?1")?;
        let mut rows = stmt.query(rusqlite::params![key])?;
        if let Some(row) = rows.next()? {
            let response_str: String = row.get(0)?;
            let response: serde_json::Value = serde_json::from_str(&response_str)?;
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }

    /// Store an idempotency record.
    pub async fn store_idempotency(
        &self,
        key: &str,
        sid: &str,
        kind: &str,
        response: &serde_json::Value,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let response_str = serde_json::to_string(response)?;
        let db = self.db.lock().await;
        db.execute(
            "INSERT OR IGNORE INTO idempotency (key, sid, kind, response, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![key, sid, kind, response_str, now],
        )?;
        Ok(())
    }

    /// Purge expired idempotency records.
    pub async fn purge_expired_idempotency(&self, ttl_secs: u64) -> Result<()> {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(ttl_secs as i64);
        let cutoff_str = cutoff.to_rfc3339();
        let db = self.db.lock().await;
        db.execute(
            "DELETE FROM idempotency WHERE created_at < ?1",
            rusqlite::params![cutoff_str],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sid_is_24_digits() {
        let sid = generate_sid();
        assert_eq!(sid.len(), 24);
        assert!(sid.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn sid_are_unique() {
        let mut sids = std::collections::BTreeSet::new();
        for _ in 0..1000 {
            assert!(sids.insert(generate_sid()));
        }
    }

    #[test]
    fn tid_is_valid_ulid() {
        let tid = generate_tid();
        assert_eq!(tid.len(), 26);
        assert!(tid.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn tid_are_unique() {
        let mut tids = std::collections::BTreeSet::new();
        for _ in 0..100 {
            tids.insert(generate_tid());
        }
        assert_eq!(tids.len(), 100);
    }
}
