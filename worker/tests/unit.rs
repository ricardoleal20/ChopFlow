//! Unit tests for worker internals that need no broker.

use chopflow_core::resources::ResourceAvailability;
use chopflow_worker::{
    derive_concurrency, echo_handler, parse_resources, parse_tags, TaskRegistry,
};
use serde_json::json;

#[test]
fn derive_concurrency_sums_resource_totals() {
    let r = ResourceAvailability {
        available: [("cpu".to_string(), 4)].into(),
        total: [("cpu".to_string(), 4)].into(),
    };
    assert_eq!(derive_concurrency(&r), 4);
}

#[test]
fn derive_concurrency_single_unit_is_one() {
    let r = ResourceAvailability {
        available: [("gpu".to_string(), 1)].into(),
        total: [("gpu".to_string(), 1)].into(),
    };
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn derive_concurrency_mixed_resources_defaults_to_one() {
    // Heterogeneous resources can't be summed (GB + cores), so we default to 1
    // and let the operator set --concurrency. Correctness still comes from the
    // broker's per-resource assign_task gate.
    let r = ResourceAvailability {
        available: [("cpu".to_string(), 2), ("gpu".to_string(), 1)].into(),
        total: [("cpu".to_string(), 2), ("gpu".to_string(), 1)].into(),
    };
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn derive_concurrency_ram_and_cpu_defaults_to_one() {
    // The canonical mixed case: a 16GB / 4-core box. Summing would give 20
    // (meaningless); min would give 4 (a guess). We default to 1 so the
    // operator consciously picks --concurrency (e.g. 4 for CPU-bound work).
    let r = ResourceAvailability {
        available: [("ram".to_string(), 16), ("cpu".to_string(), 4)].into(),
        total: [("ram".to_string(), 16), ("cpu".to_string(), 4)].into(),
    };
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn derive_concurrency_floors_at_one_when_empty() {
    let r = ResourceAvailability::default();
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn echo_handler_returns_status_ok_and_echoed_payload() {
    let payload = json!({"hello": "world"});
    let result = echo_handler(payload.clone()).unwrap();
    assert_eq!(result["status"], "ok");
    assert_eq!(result["echo"], payload);
}

#[test]
fn task_registry_register_and_get() {
    let mut registry = TaskRegistry::new();
    registry.register("echo", echo_handler);
    assert!(registry.get("echo").is_some());
    assert!(registry.get("missing").is_none());
    assert_eq!(registry.len(), 1);
    assert!(!registry.is_empty());
}

#[test]
fn task_registry_overrides_existing_handler() {
    let mut registry = TaskRegistry::new();
    registry.register("echo", echo_handler);
    registry.register("echo", echo_handler);
    assert_eq!(registry.len(), 1);
}

#[test]
fn task_registry_default_is_empty() {
    let registry = TaskRegistry::default();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
}

#[test]
fn parse_tags_splits_and_trims() {
    let tags = parse_tags("gpu, ml , default");
    assert_eq!(tags, vec!["gpu", "ml", "default"]);
}

#[test]
fn parse_tags_single_value() {
    let tags = parse_tags("default");
    assert_eq!(tags, vec!["default"]);
}

#[test]
fn parse_resources_basic() {
    let map = parse_resources("cpu:4,gpu:1").unwrap();
    assert_eq!(map.get("cpu"), Some(&4));
    assert_eq!(map.get("gpu"), Some(&1));
}

#[test]
fn parse_resources_trims_whitespace() {
    let map = parse_resources(" cpu : 4 , gpu:1 ").unwrap();
    assert_eq!(map.get("cpu"), Some(&4));
    assert_eq!(map.get("gpu"), Some(&1));
}

#[test]
fn parse_resources_skips_empty_entries() {
    // A trailing comma is harmless — it produces an empty entry that's skipped.
    let map = parse_resources("cpu:4,").unwrap();
    assert_eq!(map.get("cpu"), Some(&4));
    assert_eq!(map.len(), 1);
}

#[test]
fn parse_resources_rejects_malformed_entry() {
    let err = parse_resources("cpu-4").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("invalid resource"), "got: {}", msg);
}

#[test]
fn parse_resources_rejects_non_numeric_amount() {
    let err = parse_resources("cpu:lots").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("invalid resource amount"), "got: {}", msg);
}

#[test]
fn parse_resources_rejects_empty_map() {
    let err = parse_resources("").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("at least one resource"), "got: {}", msg);
}

#[test]
fn parse_resources_rejects_only_whitespace() {
    let err = parse_resources(" , ").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("at least one resource"), "got: {}", msg);
}
