//! Unit tests for worker internals that need no broker.

use chopflow_core::resources::{parse_resources_ext, ResourceAvailability};
use chopflow_worker::{
    completed_stages, derive_concurrency, echo_handler, parse_resources, parse_tags,
    resource_specs_proto, resources_from_declarations, TaskRegistry,
};
use serde_json::json;

#[test]
fn derive_concurrency_sums_resource_totals() {
    let r = ResourceAvailability::from_capacities([("cpu".to_string(), 4)].into());
    assert_eq!(derive_concurrency(&r), 4);
}

#[test]
fn derive_concurrency_single_unit_is_one() {
    let r = ResourceAvailability::from_capacities([("gpu".to_string(), 1)].into());
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn derive_concurrency_mixed_resources_defaults_to_one() {
    // Heterogeneous resources can't be summed (GB + cores), so we default to 1
    // and let the operator set --concurrency. Correctness still comes from the
    // broker's per-resource assign_task gate.
    let r = ResourceAvailability::from_capacities(
        [("cpu".to_string(), 2), ("gpu".to_string(), 1)].into(),
    );
    assert_eq!(derive_concurrency(&r), 1);
}

#[test]
fn derive_concurrency_ram_and_cpu_defaults_to_one() {
    // The canonical mixed case: a 16GB / 4-core box. Summing would give 20
    // (meaningless); min would give 4 (a guess). We default to 1 so the
    // operator consciously picks --concurrency (e.g. 4 for CPU-bound work).
    let r = ResourceAvailability::from_capacities(
        [("ram".to_string(), 16), ("cpu".to_string(), 4)].into(),
    );
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

// --- Extended resource declarations (static + replenishing) ------------------

#[test]
fn resource_specs_proto_marks_static_and_replenishing_entries() {
    // The worker --resources string "cpu:4,llm.rpm:10@10/60" must register a
    // static cpu (refill_amount == 0) and a replenishing llm.rpm with its
    // refill fields on the wire.
    let (capacities, refills) = parse_resources_ext("cpu:4,llm.rpm:10@10/60").unwrap();
    let specs = resource_specs_proto(&capacities, &refills);

    let cpu = &specs["cpu"];
    assert_eq!(
        (cpu.capacity, cpu.refill_amount, cpu.refill_period_secs),
        (4, 0, 0),
        "static entries declare refill_amount == 0"
    );

    let llm = &specs["llm.rpm"];
    assert_eq!(
        (llm.capacity, llm.refill_amount, llm.refill_period_secs),
        (10, 10, 60),
        "replenishing entries carry their refill rate"
    );
}

#[test]
fn resources_from_declarations_builds_refill_specs() {
    let (capacities, refills) = parse_resources_ext("cpu:4,llm.rpm:10@10/60").unwrap();
    let resources = resources_from_declarations(&capacities, &refills);

    // Static entry: plain capacity, no refill spec.
    assert_eq!(resources.total.get("cpu"), Some(&4));
    assert!(!resources.refill.contains_key("cpu"));

    // Replenishing entry: capacity + refill spec on the availability.
    assert_eq!(resources.total.get("llm.rpm"), Some(&10));
    assert_eq!(
        resources.refill.get("llm.rpm"),
        Some(&chopflow_core::RefillSpec {
            amount: 10,
            period_secs: 60
        })
    );
}

// --- completed_stages ordering -------------------------------------------------

fn checkpoint(stage: &str) -> chopflow_core::Checkpoint {
    chopflow_core::Checkpoint {
        task_id: uuid::Uuid::new_v4(),
        stage: stage.to_string(),
        payload: "{}".to_string(),
        recorded_at: chrono::Utc::now(),
    }
}

#[test]
fn completed_stages_orders_by_declared_stage_order() {
    // Checkpoints listed out of declaration order (the broker re-orders on
    // upsert, so listing order must not be trusted).
    let stages: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
    let checkpoints = vec![checkpoint("c"), checkpoint("a")];

    assert_eq!(
        completed_stages(Some(&stages), &checkpoints),
        vec!["a".to_string(), "c".to_string()]
    );
}

#[test]
fn completed_stages_are_distinct() {
    let stages: Vec<String> = vec!["a".into(), "b".into()];
    // Two checkpoints for the same stage (as an upsert race could produce).
    let checkpoints = vec![checkpoint("b"), checkpoint("b")];

    assert_eq!(
        completed_stages(Some(&stages), &checkpoints),
        vec!["b".to_string()]
    );
}

#[test]
fn completed_stages_without_declarations_use_first_appearance() {
    let checkpoints = vec![checkpoint("z"), checkpoint("a")];

    assert_eq!(
        completed_stages(None, &checkpoints),
        vec!["z".to_string(), "a".to_string()]
    );
}

#[test]
fn completed_stages_empty_when_no_checkpoints() {
    let stages: Vec<String> = vec!["a".into()];
    assert!(completed_stages(Some(&stages), &[]).is_empty());
    assert!(completed_stages(None, &[]).is_empty());
}

// --- Context-aware handler registry -------------------------------------------

#[tokio::test]
async fn task_registry_register_ctx_and_get() {
    let mut registry = TaskRegistry::new();
    registry.register("echo", echo_handler);
    registry.register_ctx("rag.ingest", |_ctx, payload| async move {
        Ok(json!({ "status": "ok", "echo": payload }))
    });

    // Both kinds coexist; get_ctx resolves only context-aware handlers.
    assert!(registry.get_ctx("rag.ingest").is_some());
    assert!(registry.get_ctx("echo").is_none());
    assert!(registry.get("echo").is_some());
    assert!(registry.get("rag.ingest").is_none());
    assert_eq!(registry.len(), 2);
}

#[test]
fn task_registry_register_ctx_overrides_existing() {
    let mut registry = TaskRegistry::new();
    registry.register_ctx("rag.ingest", |_ctx, _payload| async { Ok(json!({})) });
    registry.register_ctx("rag.ingest", |_ctx, _payload| async { Ok(json!({})) });
    assert_eq!(registry.len(), 1);
}
