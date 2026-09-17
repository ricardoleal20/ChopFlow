/*!
# Environment / fleet configuration

An "environment" is a named, region-tagged broker deployment (e.g. `local`,
`staging`, `prod` in `us-east-1`). A broker knows its *own* identity (from
`--env`/`--region`) and optionally holds a read-only catalog of the other
brokers in the fleet so the dashboard can switch between them.

The catalog lives in `config/environments.yml` and is served as-is at
`GET /api/environments`. It is intentionally declarative and static: there is
no federation, no worker multi-homing, and no auth — a broker never reaches
into another broker's storage. The catalog only lets a *dashboard* point its
HTTP client at a different broker's API.
*/

use serde::{Deserialize, Serialize};

/// One entry in the fleet catalog: a named broker deployment and the URLs its
/// gRPC (workers + CLI) and HTTP (dashboard) listeners are reachable at.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Environment {
    /// Short identifier, e.g. `local`, `prod`, `staging`. Matches the running
    /// broker's `--env` for the entry that represents *this* broker.
    pub name: String,
    /// Region tag, e.g. `default`, `us-east-1`, `eu-west-1`. Free-form.
    pub region: String,
    /// gRPC endpoint (workers + CLI connect here), e.g. `http://localhost:8000`.
    #[serde(default)]
    pub grpc_url: String,
    /// HTTP/JSON + dashboard endpoint, e.g. `http://localhost:8080`. The
    /// dashboard switches its API base to `{http_url}/api` when this entry is
    /// selected.
    #[serde(default)]
    pub http_url: String,
}

/// The parsed `environments.yml` — a flat list of known environments. There is
/// no `current` key on disk: the running broker contributes its own identity
/// (`--env`/`--region`) and the HTTP layer marks the matching catalog entry as
/// current at serve time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvironmentsConfig {
    #[serde(default)]
    pub environments: Vec<Environment>,
}

impl EnvironmentsConfig {
    /// Load and parse an `environments.yml` file. A missing file is not an
    /// error — it yields an empty catalog, so a standalone broker with no
    /// fleet config still runs (it just advertises itself as the only
    /// environment).
    pub fn load(path: &str) -> std::result::Result<Self, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text).map_err(ConfigError::Yaml),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(ConfigError::Io(e)),
        }
    }

    /// Parse an `environments.yml` document from a string.
    pub fn parse(text: &str) -> std::result::Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(text)
    }

    /// Find the catalog entry whose `name` matches `env`.
    pub fn find(&self, env: &str) -> Option<&Environment> {
        self.environments.iter().find(|e| e.name == env)
    }
}

/// Errors that can arise loading the environment catalog.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read environments config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse environments.yml: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_environments_yaml() {
        let text = "\
environments:
  - name: local
    region: default
    grpc_url: http://localhost:8000
    http_url: http://localhost:8080
  - name: prod
    region: us-east-1
    grpc_url: http://broker.prod:8000
    http_url: http://broker.prod:8080
";
        let cfg = EnvironmentsConfig::parse(text).unwrap();
        assert_eq!(cfg.environments.len(), 2);
        assert_eq!(cfg.find("prod").unwrap().region, "us-east-1");
        assert_eq!(cfg.find("missing"), None);
    }

    #[test]
    fn empty_yaml_yields_empty_catalog() {
        let cfg = EnvironmentsConfig::parse("").unwrap();
        assert!(cfg.environments.is_empty());
    }

    #[test]
    fn missing_file_is_not_an_error() {
        // A nonexistent path returns an empty catalog, not an error, so a
        // standalone broker with no environments.yml still boots.
        let cfg = EnvironmentsConfig::load("definitely/not/a/path.yml").unwrap();
        assert!(cfg.environments.is_empty());
    }

    #[test]
    fn grpc_url_defaults_to_empty_when_omitted() {
        let text = "\
environments:
  - name: local
    region: default
    http_url: http://localhost:8080
";
        let cfg = EnvironmentsConfig::parse(text).unwrap();
        assert_eq!(cfg.environments[0].grpc_url, "");
    }
}
