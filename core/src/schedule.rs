/*!
# Schedule module

A `Schedule` is a recurring or one-shot task template that the broker ticker
materializes into `Task`s. See the design spec section 3.
*/

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

use crate::error::{ChopFlowError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTemplate {
    pub name: String,
    pub payload: serde_json::Value,
    pub tags: Vec<String>,
    pub resources: HashMap<String, u32>,
    pub max_retries: u32,
    /// Dispatch priority for tasks materialized from this template. Defaults to
    /// `0` (FIFO among unprioritized tasks).
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScheduleKind {
    OneShot { eta: DateTime<Utc> },
    Cron { cron: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverlapPolicy {
    Skip,
    Coalesce,
    Allow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: Uuid,
    pub name: String,
    pub task_template: TaskTemplate,
    pub kind: ScheduleKind,
    pub overlap_policy: OverlapPolicy,
    pub enabled: bool,
    pub last_fired: Option<DateTime<Utc>>,
    pub next_fire: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl Schedule {
    pub fn new(
        name: String,
        task_template: TaskTemplate,
        kind: ScheduleKind,
        overlap_policy: OverlapPolicy,
    ) -> Result<Self> {
        let now = Utc::now();
        let next_fire = initial_next_fire(&kind, now)?;
        Ok(Self {
            id: Uuid::new_v4(),
            name,
            task_template,
            kind,
            overlap_policy,
            enabled: true,
            last_fired: None,
            next_fire,
            created_at: now,
        })
    }
}

/// The `cron` crate expects 6–7 fields (leading seconds). Users write familiar
/// 5-field cron; this prepends "0 " (seconds) when exactly 5 fields are given.
pub fn normalize_cron(expr: &str) -> String {
    let fields = expr.split_whitespace().count();
    if fields == 5 {
        format!("0 {}", expr.trim())
    } else {
        expr.trim().to_string()
    }
}

/// Next fire time strictly after `after` for the given kind.
pub fn next_fire(kind: &ScheduleKind, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match kind {
        ScheduleKind::OneShot { eta } => Ok(*eta),
        ScheduleKind::Cron { cron } => {
            let sched = cron::Schedule::from_str(&normalize_cron(cron)).map_err(|e| {
                ChopFlowError::Other(anyhow::anyhow!("invalid cron '{}': {}", cron, e))
            })?;
            sched.after(&after).next().ok_or_else(|| {
                ChopFlowError::Other(anyhow::anyhow!("cron '{}' has no future fire", cron))
            })
        }
    }
}

/// Initial `next_fire` for a freshly created schedule. OneShot → its eta;
/// Cron → next match from `now`.
pub fn initial_next_fire(kind: &ScheduleKind, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match kind {
        ScheduleKind::OneShot { eta } => Ok(*eta),
        ScheduleKind::Cron { .. } => next_fire(kind, now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cron_kind(expr: &str) -> ScheduleKind {
        ScheduleKind::Cron { cron: expr.into() }
    }

    #[test]
    fn normalize_cron_prepends_seconds_for_5_fields() {
        assert_eq!(normalize_cron("0 9 * * *"), "0 0 9 * * *");
        assert_eq!(normalize_cron("*/2 * * * *"), "0 */2 * * * *");
        // 6-field left alone
        assert_eq!(normalize_cron("0 0 9 * * *"), "0 0 9 * * *");
    }

    #[test]
    fn cron_next_fire_is_in_the_future_and_advances() {
        let now = Utc::now();
        let kind = cron_kind("*/2 * * * *");
        let first = next_fire(&kind, now).unwrap();
        assert!(first > now);
        let second = next_fire(&kind, first).unwrap();
        assert!(second > first);
        // roughly 2 minutes apart
        let delta = second - first;
        assert!(delta.num_seconds() >= 119 && delta.num_seconds() <= 121);
    }

    #[test]
    fn oneslot_next_fire_is_the_eta() {
        let eta = Utc::now() + chrono::Duration::hours(1);
        let kind = ScheduleKind::OneShot { eta };
        assert_eq!(next_fire(&kind, Utc::now()).unwrap(), eta);
    }

    #[test]
    fn invalid_cron_errors() {
        let kind = cron_kind("not a cron");
        assert!(next_fire(&kind, Utc::now()).is_err());
    }

    #[test]
    fn schedule_new_computes_next_fire() {
        let tmpl = TaskTemplate {
            name: "x".into(),
            payload: serde_json::json!({}),
            tags: vec![],
            resources: HashMap::new(),
            max_retries: 3,
            priority: 0,
        };
        let s = Schedule::new(
            "n".into(),
            tmpl,
            cron_kind("*/5 * * * *"),
            OverlapPolicy::Skip,
        )
        .unwrap();
        assert!(s.enabled);
        assert!(s.next_fire > Utc::now());
        assert!(s.last_fired.is_none());
    }
}
