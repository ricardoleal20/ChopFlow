/*!
# Resources Module

This module defines resource management abstractions for ChopFlow.

Resource management is essential for:
- Expressing task requirements (CPU, GPU, memory, etc.)
- Tracking worker capabilities and available resources
- Making intelligent scheduling decisions
- Preventing resource overcommitment
- Monitoring resource utilization

Key components include:
- `ResourceRequirements` - specifies what resources a task needs
- `ResourceAvailability` - tracks a worker's current resource state
- `RefillSpec` - declares a replenishing (rate-limited) resource
- `ResourceUsageMetrics` - provides utilization statistics

Resources come in two flavors:
- **Static** (`cpu:4`): `allocate` decrements, `release` restores.
- **Replenishing** (`llm.rpm:10@10/60`): a lazy token bucket. `allocate`
  consumes tokens; `release` does **not** restore them; tokens return only
  via time-based refill (`refill_amount` per `period_secs`, capped at
  capacity). This models rate limits (e.g. API requests per minute) where
  finishing work early does not buy back budget.

The module enables ChopFlow to make resource-aware scheduling decisions,
ensuring tasks are only assigned to workers capable of executing them
and preventing resource contention.
*/

use crate::error::{ChopFlowError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Resource requirements for a task
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    /// Map of resource name to required amount
    pub resources: HashMap<String, u32>,
}

impl ResourceRequirements {
    /// Create a new empty set of resource requirements
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    /// Add a resource requirement
    pub fn add(&mut self, resource: impl Into<String>, amount: u32) {
        self.resources.insert(resource.into(), amount);
    }

    /// Create from resource requirements
    pub fn with_resource(resource: impl Into<String>, amount: u32) -> Self {
        let mut requirements = Self::new();
        requirements.add(resource, amount);
        requirements
    }

    /// Check if these requirements can be satisfied by the given availability
    ///
    /// For replenishing resources any pending (not yet credited) refill is
    /// applied lazily first — the fractional bucket is floored when compared
    /// against the integer requirement — so a rate-limited task becomes
    /// satisfiable as soon as a full token has accrued.
    pub fn can_be_satisfied_by(&self, availability: &ResourceAvailability) -> bool {
        for (resource, required) in &self.resources {
            match availability.effective_available(resource) {
                Some(available) if *required <= available => {}
                _ => return false,
            }
        }

        true
    }
}

/// Refill rate of a replenishing (rate-limited) resource: `amount` tokens
/// accrue every `period_secs` seconds, capped at the resource's capacity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefillSpec {
    /// Number of tokens credited per period
    pub amount: u32,

    /// Length of one refill period, in seconds
    pub period_secs: u32,
}

/// Internal lazy token-bucket bookkeeping for one replenishing resource.
///
/// `tokens` is the authoritative (fractional) token count; the public
/// `available` map always holds its floor. `last_credit` is when tokens were
/// last credited, on a monotonic clock.
#[derive(Debug, Clone)]
struct TokenBucket {
    /// Fractional token count for this resource
    tokens: f64,

    /// Monotonic instant of the last refill credit
    last_credit: Instant,
}

/// Available resources on a worker
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceAvailability {
    /// Map of resource name to available amount
    pub available: HashMap<String, u32>,

    /// Map of resource name to total amount
    pub total: HashMap<String, u32>,

    /// Replenishing (rate-limited) resources and their refill rates. A
    /// resource absent from this map is static: `allocate` decrements it and
    /// `release` restores it, exactly as before this field existed.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub refill: HashMap<String, RefillSpec>,

    /// Lazy token-bucket state per replenishing resource. Not serialized:
    /// after a serde round-trip the buckets are recreated lazily (starting
    /// from the current `available` counts) on the next refill step.
    #[serde(skip)]
    bucket: HashMap<String, TokenBucket>,
}

impl ResourceAvailability {
    /// Create a new empty resource availability
    pub fn new() -> Self {
        Self {
            available: HashMap::new(),
            total: HashMap::new(),
            refill: HashMap::new(),
            bucket: HashMap::new(),
        }
    }

    /// Create a purely static availability where every resource is fully
    /// available (`available == total == capacities`, no refill specs).
    ///
    /// This is the constructor for the worker/broker registration pattern
    /// that previously used a `ResourceAvailability { available, total }`
    /// struct literal; the private token-bucket state makes literals
    /// unavailable outside this crate. Use [`Self::add_replenishing_resource`]
    /// afterwards to declare rate-limited resources.
    pub fn from_capacities(capacities: HashMap<String, u32>) -> Self {
        Self {
            available: capacities.clone(),
            total: capacities,
            refill: HashMap::new(),
            bucket: HashMap::new(),
        }
    }

    /// Add a static resource. `allocate` decrements it and `release`
    /// restores it. Adding a resource that was previously replenishing
    /// converts it back to static (any refill state is dropped).
    pub fn add_resource(&mut self, resource: impl Into<String>, amount: u32) {
        let resource = resource.into();
        self.available.insert(resource.clone(), amount);
        self.total.insert(resource.clone(), amount);
        self.refill.remove(&resource);
        self.bucket.remove(&resource);
    }

    /// Add a replenishing (rate-limited) resource: a lazy token bucket that
    /// starts full at `capacity` and accrues `refill_amount` tokens every
    /// `period_secs` seconds (capped at `capacity`). `allocate` consumes
    /// tokens; `release` does not restore them — tokens return only via
    /// time-based refill. A `period_secs` of `0` is treated as `1` (a
    /// zero-length period would mean an infinite rate).
    pub fn add_replenishing_resource(
        &mut self,
        resource: impl Into<String>,
        capacity: u32,
        refill_amount: u32,
        period_secs: u32,
    ) {
        let resource = resource.into();
        self.available.insert(resource.clone(), capacity);
        self.total.insert(resource.clone(), capacity);
        self.refill.insert(
            resource.clone(),
            RefillSpec {
                amount: refill_amount,
                period_secs,
            },
        );
        self.bucket.insert(
            resource,
            TokenBucket {
                tokens: capacity as f64,
                last_credit: Instant::now(),
            },
        );
    }

    /// Effective integer availability of `resource`, with any pending lazy
    /// refill applied first (the fractional bucket is floored). Static
    /// resources read `available` directly.
    ///
    /// This is a non-mutating view: the credited tokens are only persisted
    /// by the next mutating refill step ([`ResourceAvailability::refill_now`],
    /// called at the top of `allocate`). If a refill spec exists but no
    /// bucket state does (e.g. right after a serde round-trip), the raw
    /// `available` count is returned conservatively.
    pub fn effective_available(&self, resource: &str) -> Option<u32> {
        if !self.refill.contains_key(resource) {
            return self.available.get(resource).copied();
        }
        match self.bucket.get(resource) {
            None => self.available.get(resource).copied(),
            Some(bucket) => {
                let Some(spec) = self.refill.get(resource) else {
                    return self.available.get(resource).copied();
                };
                let pending = self.pending_gain(bucket, spec);
                let cap = self.total.get(resource).copied().unwrap_or(0) as f64;
                Some(((bucket.tokens + pending).min(cap)).floor() as u32)
            }
        }
    }

    /// Tokens accrued but not yet credited for a bucket, based on real
    /// elapsed time since its last credit. Never negative.
    fn pending_gain(&self, bucket: &TokenBucket, spec: &RefillSpec) -> f64 {
        let elapsed = Instant::now().saturating_duration_since(bucket.last_credit);
        Self::gain(spec, elapsed)
    }

    /// Tokens accrued over `elapsed` for the given spec.
    fn gain(spec: &RefillSpec, elapsed: Duration) -> f64 {
        let period = spec.period_secs.max(1) as f64;
        elapsed.as_secs_f64() * (spec.amount as f64 / period)
    }

    /// Credit `elapsed` worth of refill tokens to every replenishing bucket,
    /// cap each at its capacity, and refresh the integer `available` view.
    ///
    /// Public so tests (and future callers) can drive refill deterministically
    /// with simulated time; production code uses
    /// [`ResourceAvailability::refill_now`] instead. Missing buckets (e.g.
    /// after a serde round-trip) are created from the current `available`
    /// count, so a deserialized availability self-heals on the first refill
    /// step.
    pub fn refill(&mut self, elapsed: Duration) {
        let names: Vec<String> = self.refill.keys().cloned().collect();
        for name in names {
            let Some(spec) = self.refill.get(&name).cloned() else {
                continue;
            };
            let cap = self.total.get(&name).copied().unwrap_or(0) as f64;
            let current = self.available.get(&name).copied().unwrap_or(0);
            let bucket = self.bucket.entry(name.clone()).or_insert(TokenBucket {
                tokens: current as f64,
                last_credit: Instant::now(),
            });
            bucket.tokens = (bucket.tokens + Self::gain(&spec, elapsed)).min(cap);
            bucket.last_credit = bucket
                .last_credit
                .checked_add(elapsed)
                .unwrap_or(bucket.last_credit);
            let avail = bucket.tokens.floor() as u32;
            self.available.insert(name, avail);
        }
    }

    /// Apply pending refill based on real elapsed time (the lazy token
    /// bucket step — no background timers). Called at the top of `allocate`;
    /// also safe to call directly.
    pub fn refill_now(&mut self) {
        let credits: Vec<(String, Duration)> = self
            .bucket
            .iter()
            .map(|(name, bucket)| {
                (
                    name.clone(),
                    Instant::now().saturating_duration_since(bucket.last_credit),
                )
            })
            .collect();
        for (name, elapsed) in credits {
            let Some(spec) = self.refill.get(&name).cloned() else {
                continue;
            };
            let Some(bucket) = self.bucket.get_mut(&name) else {
                continue;
            };
            let cap = self.total.get(&name).copied().unwrap_or(0) as f64;
            bucket.tokens = (bucket.tokens + Self::gain(&spec, elapsed)).min(cap);
            bucket.last_credit = Instant::now();
            let avail = bucket.tokens.floor() as u32;
            self.available.insert(name, avail);
        }
        // Self-heal buckets that are missing refill state (serde round-trips):
        // seed them from the current available count so future refills accrue.
        for name in self.refill.keys() {
            let current = self.available.get(name).copied().unwrap_or(0);
            self.bucket
                .entry(name.clone())
                .or_insert_with(|| TokenBucket {
                    tokens: current as f64,
                    last_credit: Instant::now(),
                });
        }
    }

    /// Allocate resources
    ///
    /// Pending refill is applied first, so accrued rate-limit tokens are
    /// spent before the requirement check. On success the integer `available`
    /// counts and the fractional buckets are both decremented.
    pub fn allocate(&mut self, requirements: &ResourceRequirements) -> bool {
        // Apply pending refill first so rate-limited resources reflect
        // tokens accrued since the last credit.
        self.refill_now();

        // First check if we can satisfy the requirements
        if !requirements.can_be_satisfied_by(self) {
            return false;
        }

        // Then allocate the resources
        for (resource, required) in &requirements.resources {
            if let Some(available) = self.available.get_mut(resource) {
                *available = available.saturating_sub(*required);
            }
            // Keep the fractional bucket in sync for replenishing resources.
            if let Some(bucket) = self.bucket.get_mut(resource) {
                bucket.tokens = (bucket.tokens - *required as f64).max(0.0);
            }
        }

        true
    }

    /// Release resources
    ///
    /// Static resources are restored (clamped to their total). Replenishing
    /// resources are skipped: consumed rate-limit tokens come back only via
    /// time-based refill.
    pub fn release(&mut self, requirements: &ResourceRequirements) {
        for (resource, amount) in &requirements.resources {
            // Replenishing resources are NOT restored on release.
            if self.refill.contains_key(resource) {
                continue;
            }
            if let Some(available) = self.available.get_mut(resource) {
                let total = self.total.get(resource).unwrap_or(&0);
                *available = std::cmp::min(*available + *amount, *total);
            }
        }
    }
}

/// Parse an extended resource declaration string, e.g.
/// `"cpu:4,llm.rpm:10@10/60"`.
///
/// Each comma-separated entry is either
/// - `name:capacity` — a static resource, or
/// - `name:capacity@refill_amount/period_secs` — a replenishing
///   (rate-limited) resource (capacity 10, refilling 10 tokens per 60s).
///
/// Returns the capacity map (every entry) and the refill-spec map (only the
/// replenishing entries). A malformed entry is an error, not a silent skip.
/// An empty (or all-blank) input yields empty maps — callers enforce their
/// own "at least one resource" policy. The worker crate's legacy
/// `parse_resources` accepts the plain `name:amount` subset of this syntax.
pub fn parse_resources_ext(s: &str) -> Result<(HashMap<String, u32>, HashMap<String, RefillSpec>)> {
    let mut capacities: HashMap<String, u32> = HashMap::new();
    let mut refill: HashMap<String, RefillSpec> = HashMap::new();

    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        let usage = "expected 'name:capacity' or 'name:capacity@refill_amount/period_secs' (e.g. cpu:2, llm.rpm:10@10/60)";
        let (name, rest) = entry.split_once(':').ok_or_else(|| {
            ChopFlowError::ResourceError(format!("invalid resource '{}': {}", entry, usage))
        })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(ChopFlowError::ResourceError(format!(
                "invalid resource '{}': empty resource name",
                entry
            )));
        }

        let (capacity_str, refill_str) = match rest.split_once('@') {
            Some((capacity, refill)) => (capacity, Some(refill)),
            None => (rest, None),
        };
        let capacity = parse_resource_amount(capacity_str.trim(), entry)?;
        capacities.insert(name.to_string(), capacity);

        if let Some(refill_str) = refill_str {
            let (amount_str, period_str) =
                refill_str.split_once('/').ok_or_else(|| {
                    ChopFlowError::ResourceError(format!(
                        "invalid refill spec in '{}': expected '@refill_amount/period_secs' (e.g. @10/60)",
                        entry
                    ))
                })?;
            let amount = parse_resource_amount(amount_str.trim(), entry)?;
            let period_secs = parse_resource_amount(period_str.trim(), entry)?;
            if period_secs == 0 {
                return Err(ChopFlowError::ResourceError(format!(
                    "invalid refill period in '{}': period_secs must be greater than zero",
                    entry
                )));
            }
            refill.insert(
                name.to_string(),
                RefillSpec {
                    amount,
                    period_secs,
                },
            );
        }
    }

    Ok((capacities, refill))
}

/// Parse one non-negative integer field of a resource entry.
fn parse_resource_amount(raw: &str, entry: &str) -> Result<u32> {
    raw.parse::<u32>()
        .map_err(|e| ChopFlowError::ResourceError(format!("invalid amount in '{}': {}", entry, e)))
}

/// Resource usage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageMetrics {
    /// Map of resource name to usage percentage (0.0 to 1.0)
    pub usage: HashMap<String, f64>,

    /// Timestamp of the metrics
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Default for ResourceUsageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceUsageMetrics {
    /// Create a new set of resource usage metrics
    pub fn new() -> Self {
        Self {
            usage: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Calculate usage metrics from availability
    ///
    /// For replenishing resources any pending refill is applied first (the
    /// fractional bucket is floored), so utilization reflects accrued
    /// rate-limit tokens rather than the last persisted count.
    pub fn from_availability(availability: &ResourceAvailability) -> Self {
        let mut usage = HashMap::new();

        for (resource, total) in &availability.total {
            if let Some(available) = availability.effective_available(resource) {
                if *total > 0 {
                    let usage_value = 1.0 - (available as f64 / *total as f64);
                    usage.insert(resource.clone(), usage_value);
                }
            }
        }

        Self {
            usage,
            timestamp: chrono::Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn avail(cpu: u32, gpu: u32) -> ResourceAvailability {
        let mut a = ResourceAvailability::new();
        a.add_resource("cpu", cpu);
        a.add_resource("gpu", gpu);
        a
    }

    fn req(pairs: &[(&str, u32)]) -> ResourceRequirements {
        ResourceRequirements {
            resources: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        }
    }

    #[test]
    fn can_be_satisfied_by_checks_each_resource() {
        let a = avail(4, 1);
        let ok = ResourceRequirements {
            resources: [("cpu".to_string(), 4), ("gpu".to_string(), 1)].into(),
        };
        assert!(ok.can_be_satisfied_by(&a));

        let too_much = ResourceRequirements {
            resources: [("cpu".to_string(), 8)].into(),
        };
        assert!(!too_much.can_be_satisfied_by(&a));

        // Missing resource entirely -> not satisfied.
        let missing = ResourceRequirements {
            resources: [("tpu".to_string(), 1)].into(),
        };
        assert!(!missing.can_be_satisfied_by(&a));
    }

    #[test]
    fn allocate_then_release_restores_available() {
        let mut a = avail(4, 1);
        let r = req(&[("cpu", 3)]);

        assert!(a.allocate(&r));
        assert_eq!(a.available.get("cpu"), Some(&1));

        a.release(&r);
        assert_eq!(a.available.get("cpu"), Some(&4)); // back to total
    }

    #[test]
    fn allocate_fails_without_starving() {
        let mut a = avail(2, 0);
        let r = req(&[("cpu", 5)]);
        // Failing allocation must not mutate available.
        assert!(!a.allocate(&r));
        assert_eq!(a.available.get("cpu"), Some(&2));
    }

    #[test]
    fn release_clamps_to_total() {
        let mut a = avail(4, 1);
        // Release more than was ever allocated; must not exceed total.
        let r = req(&[("cpu", 10)]);
        a.release(&r);
        assert_eq!(a.available.get("cpu"), Some(&4));
    }

    #[test]
    fn usage_metrics_from_availability() {
        let mut a = avail(4, 1);
        let r = req(&[("cpu", 3)]);
        a.allocate(&r);
        let m = ResourceUsageMetrics::from_availability(&a);
        // cpu: 3/4 used -> 0.75
        assert!((m.usage["cpu"] - 0.75).abs() < 1e-9);
        // gpu: 0/1 used -> 0.0
        assert!((m.usage["gpu"] - 0.0).abs() < 1e-9);
    }

    // ------------------------------------------------------------------
    // Replenishing (rate-limited) resources
    // ------------------------------------------------------------------

    #[test]
    fn replenishing_bucket_starts_full() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 10, 10, 60);
        assert_eq!(a.available.get("llm.rpm"), Some(&10));
        assert_eq!(a.total.get("llm.rpm"), Some(&10));
        assert_eq!(
            a.refill.get("llm.rpm"),
            Some(&RefillSpec {
                amount: 10,
                period_secs: 60
            })
        );
        // Satisfiable immediately while full.
        assert!(req(&[("llm.rpm", 10)]).can_be_satisfied_by(&a));
    }

    #[test]
    fn refill_accrues_over_simulated_time() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 3, 1, 60);

        // Drain the bucket.
        assert!(a.allocate(&req(&[("llm.rpm", 3)])));
        assert_eq!(a.available.get("llm.rpm"), Some(&0));
        assert!(!req(&[("llm.rpm", 1)]).can_be_satisfied_by(&a));

        // One full period -> one token.
        a.refill(Duration::from_secs(60));
        assert_eq!(a.available.get("llm.rpm"), Some(&1));

        // Half a period -> half a token: floor keeps availability at 1, but
        // the fraction is preserved...
        a.refill(Duration::from_secs(30));
        assert_eq!(a.available.get("llm.rpm"), Some(&1));
        assert_eq!(a.effective_available("llm.rpm"), Some(1));

        // ...so the next half period yields a whole second token.
        a.refill(Duration::from_secs(30));
        assert_eq!(a.available.get("llm.rpm"), Some(&2));
    }

    #[test]
    fn refill_caps_at_capacity() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 2, 5, 60);
        assert!(a.allocate(&req(&[("llm.rpm", 2)])));

        // An hour of refill credit is far more than the capacity of 2.
        a.refill(Duration::from_secs(3600));
        assert_eq!(a.available.get("llm.rpm"), Some(&2));
        // Further refill stays capped.
        a.refill(Duration::from_secs(3600));
        assert_eq!(a.available.get("llm.rpm"), Some(&2));
    }

    #[test]
    fn release_does_not_restore_replenishing_tokens() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 5, 5, 60);
        let r = req(&[("llm.rpm", 2)]);

        assert!(a.allocate(&r));
        assert_eq!(a.available.get("llm.rpm"), Some(&3));

        // Release must NOT give the tokens back...
        a.release(&r);
        assert_eq!(a.available.get("llm.rpm"), Some(&3));

        // ...but time-based refill does.
        a.refill(Duration::from_secs(60));
        assert_eq!(a.available.get("llm.rpm"), Some(&5));
    }

    #[test]
    fn mixed_static_and_replenishing_worker() {
        let mut a = ResourceAvailability::new();
        a.add_resource("cpu", 4);
        a.add_replenishing_resource("llm.rpm", 2, 1, 60);

        // A task needing both: allocation consumes both.
        let r = req(&[("cpu", 2), ("llm.rpm", 2)]);
        assert!(a.allocate(&r));
        assert_eq!(a.available.get("cpu"), Some(&2));
        assert_eq!(a.available.get("llm.rpm"), Some(&0));

        // Release restores the static cpu but not the rate-limited llm.rpm.
        a.release(&r);
        assert_eq!(a.available.get("cpu"), Some(&4));
        assert_eq!(a.available.get("llm.rpm"), Some(&0));

        // The static path still behaves exactly as before: a fresh allocate
        // of only cpu succeeds and releases cleanly.
        let cpu_req = req(&[("cpu", 3)]);
        assert!(a.allocate(&cpu_req));
        a.release(&cpu_req);
        assert_eq!(a.available.get("cpu"), Some(&4));
    }

    #[test]
    fn allocate_spends_fractional_tokens_with_floor_check() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 2, 1, 60);
        assert!(a.allocate(&req(&[("llm.rpm", 2)])));

        // 1.5 tokens accrued: effective availability floors to 1.
        a.refill(Duration::from_secs(90));
        assert_eq!(a.effective_available("llm.rpm"), Some(1));

        // Allocating 1 spends the whole 1.5 (bucket keeps the 0.5 fraction).
        assert!(a.allocate(&req(&[("llm.rpm", 1)])));
        assert_eq!(a.available.get("llm.rpm"), Some(&0));

        // Another 30s -> 1.0 token again.
        a.refill(Duration::from_secs(30));
        assert_eq!(a.available.get("llm.rpm"), Some(&1));
    }

    #[test]
    fn usage_metrics_reflects_pending_refill() {
        let mut a = ResourceAvailability::new();
        a.add_replenishing_resource("llm.rpm", 10, 1, 60);
        assert!(a.allocate(&req(&[("llm.rpm", 10)])));
        assert_eq!(a.available.get("llm.rpm"), Some(&0));

        a.refill(Duration::from_secs(60));
        let m = ResourceUsageMetrics::from_availability(&a);
        // 1 of 10 tokens refilled -> 0.9 used.
        assert!((m.usage["llm.rpm"] - 0.9).abs() < 1e-9);
    }

    #[test]
    fn serde_round_trip_preserves_refill_specs() {
        let mut a = ResourceAvailability::new();
        a.add_resource("cpu", 4);
        a.add_replenishing_resource("llm.rpm", 10, 10, 60);
        let json = serde_json::to_string(&a).unwrap();
        let back: ResourceAvailability = serde_json::from_str(&json).unwrap();
        assert_eq!(back.available, a.available);
        assert_eq!(back.total, a.total);
        assert_eq!(back.refill, a.refill);

        // Static-only availability serializes exactly as before the field
        // existed (skip_serializing_if keeps old payloads compatible).
        let legacy = avail(4, 1);
        let json = serde_json::to_string(&legacy).unwrap();
        assert!(!json.contains("refill"));
        let back: ResourceAvailability = serde_json::from_str(&json).unwrap();
        assert!(back.refill.is_empty());
    }

    // ------------------------------------------------------------------
    // Extended resource parsing
    // ------------------------------------------------------------------

    #[test]
    fn parse_resources_ext_accepts_static_and_replenishing_syntax() {
        let (caps, specs) = parse_resources_ext("cpu:4,llm.rpm:10@10/60").unwrap();
        assert_eq!(caps.get("cpu"), Some(&4));
        assert_eq!(caps.get("llm.rpm"), Some(&10));
        assert_eq!(caps.len(), 2);
        assert_eq!(
            specs.get("llm.rpm"),
            Some(&RefillSpec {
                amount: 10,
                period_secs: 60
            })
        );
        assert_eq!(specs.len(), 1, "cpu has no refill spec");

        // Plain static-only declarations parse to an empty refill map.
        let (caps, specs) = parse_resources_ext("cpu:4,gpu:1").unwrap();
        assert_eq!(caps.len(), 2);
        assert!(specs.is_empty());

        // Whitespace around entries is tolerated, like the worker parser.
        let (caps, _) = parse_resources_ext(" cpu:4 , llm.rpm:10@10/60 ").unwrap();
        assert_eq!(caps.get("cpu"), Some(&4));

        // Empty input -> empty maps (callers enforce non-empty policy).
        let (caps, specs) = parse_resources_ext("").unwrap();
        assert!(caps.is_empty());
        assert!(specs.is_empty());
    }

    #[test]
    fn parse_resources_ext_rejects_garbage() {
        for bad in [
            "cpu",                  // no capacity
            "cpu:",                 // empty capacity
            ":4",                   // empty name
            "cpu:abc",              // non-numeric capacity
            "cpu:-1",               // negative
            "llm.rpm:10@10",        // refill without period
            "llm.rpm:10@/60",       // empty refill amount
            "llm.rpm:10@10/",       // empty period
            "llm.rpm:10@10/0",      // zero period
            "llm.rpm:10@10/60@5/6", // extra refill clause
            "cpu:4:2",              // stray colon
        ] {
            assert!(
                parse_resources_ext(bad).is_err(),
                "expected '{}' to be rejected",
                bad
            );
        }
    }
}
