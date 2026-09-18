//! Process supervision for the app's managed children: the local `chopflow
//! broker start` process and the optional `chopflow mcp --http` gateway.
//!
//! Design (see docs/superpowers/specs/2026-09-17-macos-app-design.md):
//! - The broker is a spawned binary, never embedded — crash isolation and
//!   version decoupling from the app build.
//! - The child gets `--parent-pid <app pid>` so a force-quit of the app can
//!   never orphan it (the broker's own watchdog self-terminates).
//! - If something already listens on the local HTTP port, we probe it: a
//!   live ChopFlow broker is *adopted* (used, never killed); anything else
//!   makes us fall back to the next free port.
//! - SQLite storage under the app-data dir keeps state durable across
//!   restarts, so killing the broker on quit loses nothing.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

/// Default ports for the managed local broker. Distinct from the CLI defaults
/// only in that they are owned by the app; a port clash falls through to the
/// adopt-or-rebind logic in [`BrokerSupervisor::ensure_started`].
pub const LOCAL_GRPC_PORT: u16 = 8000;
pub const LOCAL_HTTP_PORT: u16 = 8080;
/// Port for the optional MCP HTTP gateway child.
pub const MCP_HTTP_PORT: u16 = 8810;

const LOG_RING: usize = 2000;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(15);
const HEALTH_POLL: Duration = Duration::from_millis(200);

/// A managed child process plus how it came to exist.
#[derive(Debug)]
struct ManagedChild {
    child: Child,
    /// true when we spawned it (we own its lifecycle); false when we adopted
    /// an external process we must never kill.
    spawned_by_us: bool,
}

/// What the frontend needs to know about the local broker.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LocalBrokerStatus {
    /// Spawned and healthy (owned by the app).
    Running {
        http_port: u16,
        grpc_port: u16,
        pid: Option<u32>,
    },
    /// A foreign ChopFlow broker was found on the port and adopted.
    Adopted { http_port: u16, grpc_port: u16 },
    /// Not running (never started, or stopped).
    Stopped,
    /// Start attempted but it failed / never became healthy.
    Failed { reason: String },
}

/// Identity probe result for a broker HTTP endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct BrokerIdentity {
    pub env: String,
    pub region: String,
}

/// Supervises the local broker + MCP gateway children.
pub struct Supervisor {
    broker: Mutex<Option<ManagedChild>>,
    mcp: Mutex<Option<Child>>,
    logs: Arc<Mutex<VecDeque<String>>>,
    ports: Mutex<(u16, u16)>, // (http, grpc) currently in use
    /// True while a foreign broker on our port has been adopted (we connect,
    /// we never kill it).
    adopted: Mutex<bool>,
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            broker: Mutex::new(None),
            mcp: Mutex::new(None),
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(LOG_RING))),
            ports: Mutex::new((LOCAL_HTTP_PORT, LOCAL_GRPC_PORT)),
            adopted: Mutex::new(false),
        }
    }

    /// Resolve the `chopflow` binary: prefer PATH (respects `brew upgrade`),
    /// which is also how `tauri dev` runs. The bundled sidecar is the
    /// fallback for packaged builds.
    pub fn resolve_binary() -> Result<PathBuf, String> {
        if let Some(path) = which_chopflow() {
            return Ok(path);
        }
        // Sidecar convention: Tauri renames externalBins next to the app
        // binary as `chopflow-<triple>`. In dev there is no sidecar, so this
        // misses and we surface an actionable error.
        Err(concat!(
            "`chopflow` binary not found on PATH. ",
            "Install it with `brew install chopflow` (or cargo), ",
            "or run from a checkout with target/debug on PATH."
        )
        .to_string())
    }

    /// Ensure the local broker is up: adopt a foreign ChopFlow broker on the
    /// port, or spawn (rebinding to the next free port if something
    /// non-ChopFlow occupies ours). Returns the status once healthy.
    pub async fn ensure_started(
        &self,
        app_data_dir: PathBuf,
        parent_pid: u32,
    ) -> Result<LocalBrokerStatus, String> {
        // Already running?
        {
            let mut guard = self.broker.lock().await;
            if let Some(managed) = guard.as_mut() {
                if managed.child.try_wait().ok().flatten().is_none() {
                    let (http, grpc) = *self.ports.lock().await;
                    let pid = managed.child.id();
                    let status = if managed.spawned_by_us {
                        LocalBrokerStatus::Running {
                            http_port: http,
                            grpc_port: grpc,
                            pid,
                        }
                    } else {
                        LocalBrokerStatus::Adopted {
                            http_port: http,
                            grpc_port: grpc,
                        }
                    };
                    return Ok(status);
                }
            }
        }

        let binary = Self::resolve_binary()?;
        let mut http_port = LOCAL_HTTP_PORT;
        let mut grpc_port = LOCAL_GRPC_PORT;

        // Adopt-or-rebind: probe our preferred HTTP port.
        if port_open(LOCAL_HTTP_PORT).await {
            match probe_broker(&format!("http://127.0.0.1:{LOCAL_HTTP_PORT}")).await {
                // A live ChopFlow broker someone else started: adopt it.
                Some(ident) => {
                    *self.broker.lock().await = None; // nothing to own
                    *self.ports.lock().await = (LOCAL_HTTP_PORT, LOCAL_GRPC_PORT);
                    *self.adopted.lock().await = true;
                    self.log(format!(
                        "adopted external broker at 127.0.0.1:{LOCAL_HTTP_PORT} (env={}, region={})",
                        ident.env, ident.region
                    ))
                    .await;
                    return Ok(LocalBrokerStatus::Adopted {
                        http_port: LOCAL_HTTP_PORT,
                        grpc_port: LOCAL_GRPC_PORT,
                    });
                }
                // Not a ChopFlow broker — find the next free pair of ports.
                None => {
                    http_port = next_free_port(LOCAL_HTTP_PORT).await;
                    grpc_port = next_free_port(LOCAL_GRPC_PORT).await;
                    self.log(format!(
                        "port {LOCAL_HTTP_PORT} occupied by a non-ChopFlow process; rebinding to {http_port}/{grpc_port}"
                    ))
                    .await;
                }
            }
        }

        // Spawn: sqlite under app-data, local identity, parent watchdog.
        let db_path = app_data_dir.join("chopflow.db");
        let mut cmd = Command::new(&binary);
        cmd.args([
            "broker",
            "start",
            "--host",
            "127.0.0.1",
            "--port",
            &grpc_port.to_string(),
            "--http-port",
            &http_port.to_string(),
            "--storage",
            "sqlite",
            "--db-path",
        ])
        .arg(&db_path)
        .args([
            "--env",
            "local",
            "--region",
            "local",
            "--parent-pid",
            &parent_pid.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
        // Sidecars need the cwd to not matter; the db path is absolute.

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", binary.display()))?;
        let pid = child.id();
        self.log(format!(
            "spawned broker pid={:?} grpc={grpc_port} http={http_port} db={}",
            pid,
            db_path.display()
        ))
        .await;

        // Capture child output into the log ring.
        if let Some(stdout) = child.stdout.take() {
            spawn_log_pipe(stdout, self.logs.clone());
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_pipe(stderr, self.logs.clone());
        }

        *self.ports.lock().await = (http_port, grpc_port);
        *self.broker.lock().await = Some(ManagedChild {
            child,
            spawned_by_us: true,
        });

        // Ready gate: poll /healthz until 200 or timeout.
        let base = format!("http://127.0.0.1:{http_port}");
        let healthy = timeout(HEALTH_TIMEOUT, async {
            loop {
                if reqwest::get(format!("{base}/healthz"))
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
                {
                    break true;
                }
                sleep(HEALTH_POLL).await;
            }
        })
        .await
        .unwrap_or(false);

        if !healthy {
            // Reap the half-started child and report.
            if let Some(mut managed) = self.broker.lock().await.take() {
                let _ = managed.child.kill().await;
            }
            *self.ports.lock().await = (LOCAL_HTTP_PORT, LOCAL_GRPC_PORT);
            return Ok(LocalBrokerStatus::Failed {
                reason: format!(
                    "broker did not become healthy within {:?} (see logs)",
                    HEALTH_TIMEOUT
                ),
            });
        }
        self.log("broker healthy".into()).await;
        Ok(LocalBrokerStatus::Running {
            http_port,
            grpc_port,
            pid,
        })
    }

    /// Stop the managed broker (only if we spawned it — adopted brokers are
    /// never killed). Also stops the MCP gateway.
    pub async fn stop_broker(&self) -> LocalBrokerStatus {
        self.stop_mcp().await;
        if let Some(mut managed) = self.broker.lock().await.take() {
            if managed.spawned_by_us {
                let _ = managed.child.kill().await;
                self.log("broker stopped".into()).await;
            }
        }
        *self.ports.lock().await = (LOCAL_HTTP_PORT, LOCAL_GRPC_PORT);
        *self.adopted.lock().await = false;
        LocalBrokerStatus::Stopped
    }

    /// Synchronous liveness check for the tray (adopted broker → alive; managed
    /// child → not yet exited).
    pub fn broker_alive(&self) -> bool {
        if self.adopted.try_lock().map(|g| *g).unwrap_or(false) {
            return true;
        }
        match self.broker.try_lock() {
            Ok(mut guard) => guard
                .as_mut()
                .map(|m| m.child.try_wait().ok().flatten().is_none())
                .unwrap_or(false),
            // Lock busy (an async op in flight) — optimistically report as
            // alive rather than flashing "stopped" mid-operation.
            Err(_) => true,
        }
    }

    /// Snapshot of the broker status without starting anything.
    pub async fn status(&self) -> LocalBrokerStatus {
        if *self.adopted.lock().await {
            let (http, grpc) = *self.ports.lock().await;
            return LocalBrokerStatus::Adopted {
                http_port: http,
                grpc_port: grpc,
            };
        }
        let mut guard = self.broker.lock().await;
        if let Some(managed) = guard.as_mut() {
            if managed.child.try_wait().ok().flatten().is_none() {
                let (http, grpc) = *self.ports.lock().await;
                return LocalBrokerStatus::Running {
                    http_port: http,
                    grpc_port: grpc,
                    pid: managed.child.id(),
                };
            }
        }
        LocalBrokerStatus::Stopped
    }

    /// Start (or restart) the MCP HTTP gateway against `broker_http_base`,
    /// forwarding the connection's Bearer token when the broker requires one.
    pub async fn start_mcp(
        &self,
        broker_http_base: &str,
        api_token: Option<&str>,
    ) -> Result<String, String> {
        self.stop_mcp().await;
        let binary = Self::resolve_binary()?;
        let mut cmd = Command::new(&binary);
        cmd.args([
            "mcp",
            "--http",
            &format!("127.0.0.1:{MCP_HTTP_PORT}"),
            "--broker",
            broker_http_base,
        ]);
        if let Some(token) = api_token {
            cmd.args(["--api-token", token]);
        }
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn mcp: {e}"))?;
        if let Some(stderr) = child.stderr.take() {
            spawn_log_pipe(stderr, self.logs.clone());
        }
        if let Some(stdout) = child.stdout.take() {
            spawn_log_pipe(stdout, self.logs.clone());
        }
        *self.mcp.lock().await = Some(child);
        let url = format!("http://127.0.0.1:{MCP_HTTP_PORT}/mcp");
        self.log(format!(
            "mcp gateway started at {url} (broker {broker_http_base})"
        ))
        .await;
        Ok(url)
    }

    /// Kill the MCP gateway if running.
    pub async fn stop_mcp(&self) {
        if let Some(mut child) = self.mcp.lock().await.take() {
            let _ = child.kill().await;
            self.log("mcp gateway stopped".into()).await;
        }
    }

    pub fn mcp_running(&self) -> bool {
        self.mcp
            .try_lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
    }

    /// Local broker HTTP base for the current ports.
    pub async fn local_http_base(&self) -> String {
        let (http, _) = *self.ports.lock().await;
        format!("http://127.0.0.1:{http}")
    }

    /// Current (http, grpc) ports for the managed local broker.
    pub async fn ports(&self) -> (u16, u16) {
        *self.ports.lock().await
    }

    pub async fn logs(&self) -> Vec<String> {
        self.logs.lock().await.iter().cloned().collect()
    }

    /// Append a supervisor-level line to the log ring.
    async fn log(&self, line: String) {
        let mut logs = self.logs.lock().await;
        logs.push_back(line);
        while logs.len() > LOG_RING {
            logs.pop_front();
        }
    }

    /// Tear everything down (app quit).
    pub async fn shutdown(&self) {
        self.stop_mcp().await;
        self.stop_broker().await;
    }

    /// Blocking teardown for command handlers (app_reset) that cannot await.
    pub fn shutdown_blocking(&self) {
        tauri::async_runtime::block_on(async {
            self.stop_mcp().await;
            self.stop_broker().await;
        });
    }
}

/// `which chopflow` without a dependency: search PATH manually.
fn which_chopflow() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join("chopflow"))
        .find(|candidate| candidate.is_file())
}

/// Is anything listening on this localhost port?
async fn port_open(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_ok()
}

/// First free port at or after `start`.
async fn next_free_port(start: u16) -> u16 {
    let mut port = start;
    while port < start.saturating_add(100) && port_open(port).await {
        port += 1;
    }
    port
}

/// Probe an HTTP base for a live ChopFlow broker (healthz + stats identity).
async fn probe_broker(base: &str) -> Option<BrokerIdentity> {
    let health = reqwest::get(format!("{base}/healthz")).await.ok()?;
    if !health.status().is_success() {
        return None;
    }
    let stats: serde_json::Value = reqwest::get(format!("{base}/api/stats"))
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    Some(BrokerIdentity {
        env: stats.get("env")?.as_str()?.to_string(),
        region: stats.get("region")?.as_str()?.to_string(),
    })
}

/// Pipe a child's stdout/stderr lines into the log ring.
fn spawn_log_pipe<T: tokio::io::AsyncRead + Unpin + Send + 'static>(
    stream: T,
    ring: Arc<Mutex<VecDeque<String>>>,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut logs = ring.lock().await;
            logs.push_back(line);
            while logs.len() > LOG_RING {
                logs.pop_front();
            }
        }
    });
}
