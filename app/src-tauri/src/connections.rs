//! App-owned connection store, persisted as JSON in the Tauri app-data dir.
//!
//! The app's world is: one managed "local" broker (spawned by the app) plus
//! zero or more user-declared remote brokers (name + HTTP URL). Switching
//! between them is pure UI state (`setApiBase` on the dashboard side); this
//! store remembers the list and the last-used connection across launches.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const STORE_FILE: &str = "connections.json";

/// A labelled API token the app's local broker accepts. `token` is shown in
/// full exactly once (when created) — afterwards only the identifier is
/// displayed; the value lives on in the app-data store so the app, workers,
/// and its MCP gateway can authenticate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalToken {
    /// Operator-chosen name: which client / person holds this token.
    pub id: String,
    /// The Bearer value (never displayed after creation).
    pub token: String,
}

/// A user-declared remote broker.
///
/// `token` is the optional Bearer token the remote broker requires when it was
/// started with `--api-token`. It is stored only in the app's local store
/// (mode-0600 app-data), never in the fleet catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Remote {
    pub name: String,
    pub http_url: String,
    #[serde(default)]
    pub token: Option<String>,
}

/// Persisted app state. `last_used` is `"local"` or a remote name; missing
/// means "local" for a fresh install.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ConnectionStore {
    #[serde(default)]
    pub remotes: Vec<Remote>,
    #[serde(default)]
    pub last_used: Option<String>,
    #[serde(default)]
    pub mcp_enabled: bool,
    #[serde(default)]
    pub first_run_done: bool,
    /// Labelled Bearer tokens the app's local broker requires (passed as
    /// `--api-token` when spawning it; each is a separate credential, e.g.
    /// one per client). All are equally valid at the broker; the `id` is
    /// app-side metadata so an operator knows which client holds which
    /// token. Empty = no auth, exactly like a bare `chopflow broker start`.
    #[serde(default)]
    pub local_tokens: Vec<LocalToken>,
    /// Optional token the MCP gateway itself requires (passed as
    /// `--access-token`). None = anyone who can reach the endpoint can use
    /// it. Independent from broker auth (`local_tokens`).
    #[serde(default)]
    pub mcp_access_token: Option<String>,
}

impl ConnectionStore {
    /// Load from `<dir>/connections.json`. A missing file is a fresh install,
    /// not an error — the default store is returned.
    pub fn load(dir: &Path) -> Self {
        let path: PathBuf = dir.join(STORE_FILE);
        match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
                eprintln!("connections store corrupt ({e}), starting fresh: {path:?}");
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }

    /// Persist atomically-ish: write to a temp file then rename over the
    /// target, so a crash mid-write can never truncate the store.
    pub fn save(&self, dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("create app-data dir: {e}"))?;
        let path = dir.join(STORE_FILE);
        let tmp = dir.join(format!("{STORE_FILE}.tmp"));
        let raw = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, raw).map_err(|e| format!("write store: {e}"))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("rename store: {e}"))?;
        Ok(())
    }

    /// Insert or update a remote by name.
    pub fn upsert_remote(&mut self, remote: Remote) {
        match self.remotes.iter_mut().find(|r| r.name == remote.name) {
            Some(existing) => *existing = remote,
            None => self.remotes.push(remote),
        }
    }

    /// Remove a remote by name. Returns whether it existed.
    pub fn remove_remote(&mut self, name: &str) -> bool {
        let before = self.remotes.len();
        self.remotes.retain(|r| r.name != name);
        // If the removed remote was the last-used connection, fall back to
        // local so the app never restores a dangling connection.
        if self.last_used.as_deref() == Some(name) {
            self.last_used = None;
        }
        self.remotes.len() != before
    }

    /// The connection to restore on launch: the saved one, or "local".
    pub fn effective_last_used(&self) -> String {
        self.last_used
            .clone()
            .unwrap_or_else(|| "local".to_string())
    }

    pub fn remote(&self, name: &str) -> Option<&Remote> {
        self.remotes.iter().find(|r| r.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_is_fresh_default() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ConnectionStore::load(tmp.path());
        assert!(store.remotes.is_empty());
        assert!(!store.first_run_done);
        assert_eq!(store.effective_last_used(), "local");
    }

    #[test]
    fn save_then_load_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = ConnectionStore::default();
        store.upsert_remote(Remote {
            name: "prod".into(),
            http_url: "http://broker.prod:8080".into(),
            token: None,
        });
        store.last_used = Some("prod".into());
        store.first_run_done = true;
        store.save(tmp.path()).unwrap();

        let loaded = ConnectionStore::load(tmp.path());
        assert_eq!(loaded.remotes.len(), 1);
        assert_eq!(loaded.remotes[0].name, "prod");
        assert_eq!(loaded.last_used.as_deref(), Some("prod"));
        assert!(loaded.first_run_done);
    }

    #[test]
    fn upsert_updates_existing_by_name() {
        let mut store = ConnectionStore::default();
        store.upsert_remote(Remote {
            name: "staging".into(),
            http_url: "http://a:8080".into(),
            token: None,
        });
        store.upsert_remote(Remote {
            name: "staging".into(),
            http_url: "http://b:9090".into(),
            token: None,
        });
        assert_eq!(store.remotes.len(), 1);
        assert_eq!(store.remotes[0].http_url, "http://b:9090");
    }

    #[test]
    fn removing_last_used_remote_falls_back_to_local() {
        let mut store = ConnectionStore::default();
        store.upsert_remote(Remote {
            name: "prod".into(),
            http_url: "http://p:8080".into(),
            token: None,
        });
        store.last_used = Some("prod".into());
        assert!(store.remove_remote("prod"));
        assert_eq!(store.effective_last_used(), "local");
        assert!(!store.remove_remote("prod"), "second remove is a no-op");
    }

    #[test]
    fn corrupt_store_starts_fresh() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(STORE_FILE), "{not json").unwrap();
        assert_eq!(
            ConnectionStore::load(tmp.path()),
            ConnectionStore::default()
        );
    }
}
