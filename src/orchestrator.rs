//! Task execution engine and SSE broadcast.
//!
//! Orchestrates child-process steps defined in `[[shop.kinds.steps]]`.
//! Supports sequential dependency chains and bounded parallel fan-out.
//! Publishes real-time `TaskEvent` updates via `tokio::sync::broadcast` so
//! SSE endpoints can stream job progress.

use crate::config::{KindConfig, PackageConfig, TaskStepConfig};
use crate::state::{AppState, Job};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Single SSE-streamable event.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StreamEvent {
    pub tid: String,
    pub kind: String,
    pub status: String,
    pub step_id: String,
    pub data: Value,
    pub created_at: String,
}

/// An event bus that fans out task events to all SSE subscribers.
#[derive(Clone)]
pub struct EventBus {
    /// Channel per task tid.  We use a modest capacity to avoid unbounded
    /// back-pressure; slow consumers are dropped.
    senders: Arc<tokio::sync::Mutex<BTreeMap<String, broadcast::Sender<StreamEvent>>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    /// Create (or return existing) sender for a given task.
    async fn channel(&self, tid: &str) -> broadcast::Sender<StreamEvent> {
        let mut map = self.senders.lock().await;
        if let Some(sender) = map.get(tid) {
            sender.clone()
        } else {
            let (tx, _) = broadcast::channel(64);
            map.insert(tid.to_string(), tx.clone());
            tx
        }
    }

    /// Subscribe to events for a task.  Returns a receiver that starts from
    /// the moment of subscription (not replay).
    pub async fn subscribe(&self, tid: &str) -> broadcast::Receiver<StreamEvent> {
        self.channel(tid).await.subscribe()
    }

    /// Publish an event; also persists it to the database.
    pub async fn publish(&self, state: &AppState, event: StreamEvent) {
        // Persist
        let _ = state
            .record_task_event(&event.tid, &event.status, &event.step_id, &event.data)
            .await;

        // Publish to subscribers (ignore errors — no subscribers is fine).
        let tx = self.channel(&event.tid).await;
        let _ = tx.send(event);
    }

    /// Purge channels for completed tasks older than `max_age`.
    pub async fn prune(&self, state: &AppState, max_age_secs: u64) {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_secs as i64);
        let mut map = self.senders.lock().await;
        // Retain only tasks that may still be active.
        map.retain(|tid, tx| {
            if tx.receiver_count() > 0 {
                return true;
            }
            tokio::task::block_in_place(|| {
                let db = state.db.blocking_lock();
                let mut stmt = db
                    .prepare("SELECT status, updated_at FROM jobs WHERE tid = ?1")
                    .ok();
                if let Some(stmt) = stmt.as_mut() {
                    let result: std::result::Result<(String, String), _> = stmt
                        .query_row(rusqlite::params![tid], |r| {
                            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                        });
                    if let Ok(row) = result {
                        let status = row.0;
                        let updated: chrono::DateTime<chrono::Utc> =
                            chrono::DateTime::parse_from_rfc3339(&row.1)
                                .map(|d| d.into())
                                .unwrap_or(chrono::Utc::now());
                        if status == "completed" || status == "failed" && updated < cutoff {
                            return false;
                        }
                    }
                }
                true
            })
        });
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Task runner
// ---------------------------------------------------------------------------

/// Execute all steps of a task kind for a specific job, publishing events as
/// progress is made.
pub async fn run_task(state: AppState, bus: EventBus, job: Job, kind: KindConfig, input: Value) {
    let tid = job.tid.clone();
    let kind_slug = kind.slug.clone();

    // Publish "started"
    bus.publish(
        &state,
        StreamEvent {
            tid: tid.clone(),
            kind: kind_slug.clone(),
            status: "running".to_string(),
            step_id: String::new(),
            data: serde_json::json!({"message": "task started"}),
            created_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await;

    // Update job status to running
    let _ = state.update_job_status(&tid, "running", None).await;

    // Run steps with dependency ordering
    let result = execute_steps(
        &state,
        &bus,
        &tid,
        &kind_slug,
        &kind.steps,
        &input,
        kind.concurrency,
    )
    .await;

    match result {
        Ok(output) => {
            let _ = state
                .update_job_status(&tid, "completed", Some(&output))
                .await;
            bus.publish(
                &state,
                StreamEvent {
                    tid: tid.clone(),
                    kind: kind_slug.clone(),
                    status: "completed".to_string(),
                    step_id: String::new(),
                    data: output,
                    created_at: chrono::Utc::now().to_rfc3339(),
                },
            )
            .await;
        }
        Err(e) => {
            let error_data = serde_json::json!({"error": e.to_string()});
            let _ = state
                .update_job_status(&tid, "failed", Some(&error_data))
                .await;
            bus.publish(
                &state,
                StreamEvent {
                    tid: tid.clone(),
                    kind: kind_slug.clone(),
                    status: "failed".to_string(),
                    step_id: String::new(),
                    data: error_data,
                    created_at: chrono::Utc::now().to_rfc3339(),
                },
            )
            .await;
        }
    }
}

/// Run steps respecting the dependency graph.
async fn execute_steps(
    state: &AppState,
    bus: &EventBus,
    tid: &str,
    kind_slug: &str,
    steps: &[TaskStepConfig],
    input: &Value,
    concurrency: u32,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if steps.is_empty() {
        return Ok(serde_json::json!({"message": "no steps defined"}));
    }

    // Build completion tracking
    let mut completed: BTreeSet<String> = BTreeSet::new();
    let mut step_outputs: BTreeMap<String, Value> = BTreeMap::new();
    let mut final_error: Option<String> = None;

    // Keep looping until all steps are done or a non-continuable error occurs.
    while completed.len() < steps.len() {
        // Find steps ready to run (all deps satisfied, not yet completed)
        let ready: Vec<&TaskStepConfig> = steps
            .iter()
            .filter(|s| {
                !completed.contains(&s.id) && s.depends_on.iter().all(|d| completed.contains(d))
            })
            .collect();

        if ready.is_empty() {
            break; // Circular dependency or all done
        }

        let batch_size = if concurrency == 0 {
            ready.len()
        } else {
            concurrency as usize
        };
        let batch = &ready[..batch_size.min(ready.len())];

        // Run the batch in parallel
        let mut handles = Vec::new();
        for step in batch {
            let step = (*step).clone();
            let tid = tid.to_string();
            let kind_slug = kind_slug.to_string();
            let state = state.clone();
            let bus = bus.clone();
            let input = input.clone();
            let step_outputs = step_outputs.clone();
            let continue_on_error = step.continue_on_error;
            let step_id = step.id.clone();

            let handle = tokio::spawn(async move {
                run_single_step(&state, &bus, &tid, &kind_slug, &step, &input, &step_outputs).await
            });
            handles.push((step_id, continue_on_error, handle));
        }

        for (step_id, continue_on_error, handle) in handles {
            match handle.await {
                Ok(Ok(output)) => {
                    completed.insert(step_id.clone());
                    step_outputs.insert(step_id.clone(), output);
                }
                Ok(Err(e)) => {
                    if continue_on_error {
                        completed.insert(step_id.clone());
                        step_outputs
                            .insert(step_id.clone(), serde_json::json!({"error": e.to_string()}));
                    } else {
                        final_error = Some(e.to_string());
                        break;
                    }
                }
                Err(join_err) => {
                    let msg = format!("step {step_id} panicked: {join_err}");
                    if continue_on_error {
                        completed.insert(step_id.clone());
                        step_outputs.insert(step_id, serde_json::json!({"error": msg}));
                    } else {
                        final_error = Some(msg);
                        break;
                    }
                }
            }
        }

        if final_error.is_some() {
            break;
        }
    }

    if let Some(err) = final_error {
        return Err(err.into());
    }

    Ok(serde_json::json!({
        "message": "all steps completed",
        "steps": step_outputs,
    }))
}

/// Run a single command step.
async fn run_single_step(
    state: &AppState,
    bus: &EventBus,
    tid: &str,
    kind_slug: &str,
    step: &TaskStepConfig,
    _input: &Value,
    step_outputs: &BTreeMap<String, Value>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let step_id = if step.id.is_empty() {
        "default".to_string()
    } else {
        step.id.clone()
    };

    // Publish step start
    bus.publish(
        state,
        StreamEvent {
            tid: tid.to_string(),
            kind: kind_slug.to_string(),
            status: "step_started".to_string(),
            step_id: step_id.clone(),
            data: serde_json::json!({"command": step.command, "args": step.args}),
            created_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await;

    // Build environment
    let mut env_map: BTreeMap<String, String> = BTreeMap::new();
    if step.inherit_env {
        for (k, v) in std::env::vars() {
            env_map.insert(k, v);
        }
    }
    for (k, v) in &step.env {
        env_map.insert(k.clone(), v.clone());
    }
    // Inject step outputs as env vars
    for (name, val) in step_outputs {
        let key = format!("STEP_OUTPUT_{}", name.replace('.', "_").to_uppercase());
        env_map.insert(key, serde_json::to_string(val).unwrap_or_default());
    }

    let working_dir = step.working_dir.clone().unwrap_or_else(|| {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    });

    let mut cmd = tokio::process::Command::new(&step.command);
    cmd.args(&step.args);
    cmd.env_clear();
    for (k, v) in &env_map {
        cmd.env(k, v);
    }
    cmd.current_dir(&working_dir);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);

    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn '{}': {e}", step.command))?;

    // Wait with timeout — use the child's id for possible kill
    let child_id = child.id().unwrap_or(0);

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(step.timeout_ms),
        child.wait_with_output(),
    )
    .await;

    let output = match result {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => return Err(format!("process error: {e}").into()),
        Err(_) => {
            // Timeout — kill the process by pid
            if child_id > 0 {
                let _ = std::process::Command::new("kill")
                    .arg("-9")
                    .arg(child_id.to_string())
                    .output();
            }
            return Err(format!("step timed out after {}ms", step.timeout_ms).into());
        }
    };

    // Truncate output
    let stdout_str =
        String::from_utf8_lossy(&output.stdout[..step.max_stdout_bytes.min(output.stdout.len())])
            .to_string();
    let stderr_str =
        String::from_utf8_lossy(&output.stderr[..step.max_stderr_bytes.min(output.stderr.len())])
            .to_string();

    let exit_code = output.status.code().unwrap_or(-1);
    let success = step.success_exit_codes.contains(&exit_code);

    let step_data = serde_json::json!({
        "command": step.command,
        "args": step.args,
        "exit_code": exit_code,
        "stdout": stdout_str,
        "stderr": stderr_str,
        "success": success,
    });

    // Publish step completion
    let event_status = if success {
        "step_completed"
    } else {
        "step_failed"
    };
    bus.publish(
        state,
        StreamEvent {
            tid: tid.to_string(),
            kind: kind_slug.to_string(),
            status: event_status.to_string(),
            step_id: step_id.clone(),
            data: step_data.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await;

    if !success {
        return Err(format!("step '{step_id}' exited with code {exit_code}").into());
    }

    Ok(step_data)
}

// ---------------------------------------------------------------------------
// Package launcher
// ---------------------------------------------------------------------------

/// Launch all enabled packages from the config, passing `--config <path>`.
/// Returns handles that can be awaited for graceful shutdown.
pub fn launch_packages(
    config_path: String,
    packages: &BTreeMap<String, PackageConfig>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    for (name, pkg) in packages {
        if !pkg.enabled || pkg.command.is_empty() {
            continue;
        }

        let name = name.clone();
        let command = pkg.command.clone();
        let args = pkg.args.clone();
        let env = pkg.env.clone();
        let inherit_env = pkg.inherit_env;
        let restart = pkg.restart;
        let restart_delay = pkg.restart_delay_secs;
        let config_path = config_path.clone();

        let handle = tokio::spawn(async move {
            loop {
                tracing::info!(package = %name, "launching package");
                let mut cmd = tokio::process::Command::new(&command);
                cmd.args(&args);
                cmd.arg("--config");
                cmd.arg(&config_path);
                cmd.kill_on_drop(true);

                if inherit_env {
                    for (k, v) in std::env::vars() {
                        cmd.env(k, v);
                    }
                }
                for (k, v) in &env {
                    cmd.env(k, v);
                }

                match cmd.spawn() {
                    Ok(mut child) => {
                        let status = child.wait().await;
                        match status {
                            Ok(s) => tracing::info!(
                                package = %name,
                                exit_code = ?s.code(),
                                "package exited"
                            ),
                            Err(e) => tracing::error!(
                                package = %name,
                                error = %e,
                                "package process error"
                            ),
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            package = %name,
                            error = %e,
                            "failed to launch package"
                        );
                    }
                }

                if !restart {
                    break;
                }
                tracing::info!(
                    package = %name,
                    delay_secs = restart_delay,
                    "restarting package"
                );
                tokio::time::sleep(std::time::Duration::from_secs(restart_delay)).await;
            }
        });

        handles.push(handle);
    }

    handles
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn event_bus_subscribe_and_publish() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe("test-tid").await;

        let event = StreamEvent {
            tid: "test-tid".into(),
            kind: "img.edit".into(),
            status: "running".into(),
            step_id: "resize".into(),
            data: serde_json::json!({"progress": 50}),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let tx = bus.channel("test-tid").await;
        tx.send(event.clone()).unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.tid, "test-tid");
        assert_eq!(received.kind, "img.edit");
        assert_eq!(received.status, "running");
        assert_eq!(received.step_id, "resize");
        assert_eq!(received.data["progress"], 50);
    }
}
