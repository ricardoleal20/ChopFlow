//! Unit tests for pure CLI helpers (no broker needed).

use chopflow_cli::{
    build_schedule, build_schedule_kind, checkpoint_payload_preview, extract_task_options,
    merge_task_options, parse_overlap, parse_resource_map, parse_stages, resolve_broker,
    status_name, TaskOptions,
};

#[test]
fn status_name_maps_all_known_values() {
    assert_eq!(status_name(0), "CREATED");
    assert_eq!(status_name(1), "QUEUED");
    assert_eq!(status_name(2), "RUNNING");
    assert_eq!(status_name(3), "COMPLETED");
    assert_eq!(status_name(4), "FAILED");
    assert_eq!(status_name(5), "DEADLETTERED");
    assert_eq!(status_name(6), "CANCELLED");
}

#[test]
fn status_name_unknown_for_out_of_range() {
    assert_eq!(status_name(7), "UNKNOWN");
    assert_eq!(status_name(99), "UNKNOWN");
    assert_eq!(status_name(-1), "UNKNOWN");
}

#[test]
fn parse_overlap_valid() {
    assert!(matches!(
        parse_overlap("skip").unwrap(),
        chopflow_cli::chopflow::OverlapPolicy::OverlapSkip
    ));
    assert!(matches!(
        parse_overlap("coalesce").unwrap(),
        chopflow_cli::chopflow::OverlapPolicy::OverlapCoalesce
    ));
    assert!(matches!(
        parse_overlap("allow").unwrap(),
        chopflow_cli::chopflow::OverlapPolicy::OverlapAllow
    ));
}

#[test]
fn parse_overlap_rejects_unknown() {
    let err = parse_overlap("bogus").unwrap_err();
    assert!(format!("{}", err).contains("invalid overlap"));
}

#[test]
fn build_schedule_kind_requires_exactly_one_of_cron_or_eta() {
    // Neither → error.
    let err = build_schedule_kind(None, None).unwrap_err();
    assert!(format!("{}", err).contains("exactly one of --cron or --eta"));

    // Both → error.
    let err = build_schedule_kind(
        Some("0 * * * *".into()),
        Some("2026-09-06T00:00:00Z".into()),
    )
    .unwrap_err();
    assert!(format!("{}", err).contains("exactly one of --cron or --eta"));
}

#[test]
fn build_schedule_kind_cron() {
    let kind = build_schedule_kind(Some("0 * * * *".into()), None).unwrap();
    assert!(matches!(
        kind,
        Some(chopflow_cli::chopflow::schedule_kind::Kind::Cron(_))
    ));
}

#[test]
fn build_schedule_kind_oneshot() {
    let kind = build_schedule_kind(None, Some("2026-09-06T12:00:00Z".into())).unwrap();
    assert!(matches!(
        kind,
        Some(chopflow_cli::chopflow::schedule_kind::Kind::Eta(_))
    ));
}

#[test]
fn build_schedule_kind_rejects_bad_eta() {
    let err = build_schedule_kind(None, Some("not-a-date".into())).unwrap_err();
    let msg = format!("{}", err);
    assert!(!msg.is_empty(), "bad ETA should produce an error");
    // chrono's parse error mentions the invalid input in some way; we only
    // need to know it errored (the Ok path would have returned a Kind).
    assert!(matches!(err, chopflow_core::error::ChopFlowError::Other(_)));
}

#[test]
fn parse_resource_map_basic() {
    let map = parse_resource_map("cpu:4,gpu:1");
    assert_eq!(map.get("cpu"), Some(&4));
    assert_eq!(map.get("gpu"), Some(&1));
}

#[test]
fn parse_resource_map_empty() {
    assert!(parse_resource_map("").is_empty());
    assert!(parse_resource_map(" , ").is_empty());
}

#[test]
fn parse_resource_map_skips_malformed() {
    let map = parse_resource_map("cpu:4,bogus,gpu:1");
    assert_eq!(map.len(), 2);
    assert!(map.contains_key("cpu"));
    assert!(map.contains_key("gpu"));
}

#[test]
fn build_schedule_assembles_cron_schedule() {
    let schedule = build_schedule(
        "nightly".into(),
        "build".into(),
        Some("0 9 * * *".into()),
        None,
        "{}".into(),
        "ci".into(),
        "cpu:4".into(),
        3,
        "skip".into(),
        0,
    )
    .unwrap();

    assert_eq!(schedule.name, "nightly");
    assert!(schedule.enabled);
    assert_eq!(schedule.overlap_policy, 0); // OVERLAP_SKIP
    let template = schedule.task_template.unwrap();
    assert_eq!(template.name, "build");
    assert_eq!(template.tags, vec!["ci"]);
    assert_eq!(template.resources.get("cpu"), Some(&4));
    assert!(matches!(
        schedule.kind.unwrap().kind,
        Some(chopflow_cli::chopflow::schedule_kind::Kind::Cron(_))
    ));
}

#[test]
fn build_schedule_rejects_invalid_payload_json() {
    let err = build_schedule(
        "s".into(),
        "t".into(),
        Some("0 * * * *".into()),
        None,
        "{not json".into(),
        "".into(),
        "".into(),
        3,
        "skip".into(),
        0,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        chopflow_core::error::ChopFlowError::SerializationError(_)
    ));
}

// ---- resolve_broker --------------------------------------------------------

fn write_env_yml(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let p = dir.join("environments.yml");
    std::fs::write(&p, body).unwrap();
    p
}

#[test]
fn resolve_broker_returns_default_when_no_env() {
    // No --env: the explicit --broker wins.
    let got = resolve_broker("http://localhost:8000", None, "config/missing.yml").unwrap();
    assert_eq!(got, "http://localhost:8000");
}

#[test]
fn resolve_broker_resolves_env_to_grpc_url() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_env_yml(
        tmp.path(),
        "environments:\n  - name: prod\n    region: us-east-1\n    grpc_url: http://broker.prod:8000\n    http_url: http://broker.prod:8080\n",
    );
    let got = resolve_broker(
        "http://localhost:8000",
        Some("prod"),
        path.to_str().unwrap(),
    )
    .unwrap();
    assert_eq!(got, "http://broker.prod:8000");
}

#[test]
fn resolve_broker_errors_on_unknown_env() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_env_yml(
        tmp.path(),
        "environments:\n  - name: prod\n    region: us-east-1\n    grpc_url: http://broker.prod:8000\n    http_url: http://broker.prod:8080\n",
    );
    let err = resolve_broker(
        "http://localhost:8000",
        Some("staging"),
        path.to_str().unwrap(),
    )
    .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("staging"), "msg={msg}");
    assert!(msg.contains("prod"), "msg={msg}");
}

#[test]
fn resolve_broker_errors_when_grpc_url_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_env_yml(
        tmp.path(),
        "environments:\n  - name: prod\n    region: us-east-1\n    http_url: http://broker.prod:8080\n",
    );
    let err = resolve_broker(
        "http://localhost:8000",
        Some("prod"),
        path.to_str().unwrap(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("no grpc_url"));
}

// ---- Task options: --stages / --idempotency-key ----------------------------

#[test]
fn parse_stages_splits_and_trims() {
    assert_eq!(
        parse_stages("chunk,embed,index").unwrap(),
        vec!["chunk", "embed", "index"]
    );
    assert_eq!(
        parse_stages(" chunk , embed ").unwrap(),
        vec!["chunk", "embed"]
    );
    assert_eq!(parse_stages("single").unwrap(), vec!["single"]);
}

#[test]
fn parse_stages_rejects_empty_entries() {
    for bad in ["", " ", "chunk,,embed", "chunk,", ",chunk", "chunk, ,embed"] {
        let err = parse_stages(bad).unwrap_err();
        assert!(
            format!("{err}").contains("non-empty stage names"),
            "input {bad:?} should be rejected, got {err}"
        );
    }
}

#[test]
fn extract_task_options_reads_and_strips_file_fields() {
    let mut payload: serde_json::Value = serde_json::from_str(
        r#"{"document":"text","stages":["chunk","embed"],"idempotency_key":"file-key"}"#,
    )
    .unwrap();
    let options = extract_task_options(&mut payload).unwrap();
    assert_eq!(
        options,
        TaskOptions {
            stages: Some(vec!["chunk".into(), "embed".into()]),
            idempotency_key: Some("file-key".into()),
        }
    );
    // The reserved keys describe the task, not the handler input.
    assert_eq!(payload["document"], "text");
    assert!(payload.get("stages").is_none());
    assert!(payload.get("idempotency_key").is_none());
}

#[test]
fn extract_task_options_ignores_non_object_payloads() {
    let mut payload: serde_json::Value = serde_json::from_str("[1,2,3]").unwrap();
    assert_eq!(
        extract_task_options(&mut payload).unwrap(),
        TaskOptions::default()
    );
    assert_eq!(payload, serde_json::json!([1, 2, 3]));
}

#[test]
fn extract_task_options_normalizes_empty_declarations() {
    // An empty stages array or empty key string declares nothing.
    let mut payload: serde_json::Value =
        serde_json::from_str(r#"{"stages":[],"idempotency_key":""}"#).unwrap();
    assert_eq!(
        extract_task_options(&mut payload).unwrap(),
        TaskOptions::default()
    );
}

#[test]
fn extract_task_options_rejects_wrong_shapes() {
    let mut payload: serde_json::Value =
        serde_json::from_str(r#"{"stages":"chunk,embed"}"#).unwrap();
    let err = extract_task_options(&mut payload).unwrap_err();
    assert!(format!("{err}").contains(r#""stages" must be an array"#));

    let mut payload: serde_json::Value = serde_json::from_str(r#"{"stages":[1,2]}"#).unwrap();
    assert!(extract_task_options(&mut payload).is_err());

    let mut payload: serde_json::Value = serde_json::from_str(r#"{"idempotency_key":42}"#).unwrap();
    let err = extract_task_options(&mut payload).unwrap_err();
    assert!(format!("{err}").contains(r#""idempotency_key" must be a string"#));
}

#[test]
fn merge_task_options_flags_win_over_file_fields() {
    let flags = TaskOptions {
        stages: Some(vec!["a".into(), "b".into()]),
        idempotency_key: Some("flag-key".into()),
    };
    let file = TaskOptions {
        stages: Some(vec!["c".into()]),
        idempotency_key: Some("file-key".into()),
    };
    let merged = merge_task_options(flags, file);
    assert_eq!(merged.stages, Some(vec!["a".into(), "b".into()]));
    assert_eq!(merged.idempotency_key, Some("flag-key".into()));
}

#[test]
fn merge_task_options_falls_back_to_file_fields() {
    let merged = merge_task_options(
        TaskOptions::default(),
        TaskOptions {
            stages: Some(vec!["c".into()]),
            idempotency_key: Some("file-key".into()),
        },
    );
    assert_eq!(merged.stages, Some(vec!["c".into()]));
    assert_eq!(merged.idempotency_key, Some("file-key".into()));
}

#[test]
fn merge_task_options_default_when_neither_declares() {
    assert_eq!(
        merge_task_options(TaskOptions::default(), TaskOptions::default()),
        TaskOptions::default()
    );
}

// ---- Checkpoint payload preview ---------------------------------------------

#[test]
fn checkpoint_payload_preview_passthrough_when_short() {
    assert_eq!(
        checkpoint_payload_preview(r#"{"chunks":3}"#, 80),
        r#"{"chunks":3}"#
    );
}

#[test]
fn checkpoint_payload_preview_elides_long_payloads() {
    let preview = checkpoint_payload_preview(&"x".repeat(100), 80);
    assert_eq!(preview.chars().count(), 80);
    assert!(preview.ends_with('…'));
}

#[test]
fn checkpoint_payload_preview_collapses_whitespace() {
    // All whitespace runs collapse to single spaces.
    assert_eq!(
        checkpoint_payload_preview("{\n  \"chunks\":\n 3\n}\n", 80),
        "{ \"chunks\": 3 }"
    );
}

#[test]
fn checkpoint_payload_preview_empty_payload() {
    assert_eq!(checkpoint_payload_preview("", 80), "(empty)");
    assert_eq!(checkpoint_payload_preview("   \n ", 80), "(empty)");
}

#[test]
fn checkpoint_payload_preview_zero_max_is_empty() {
    assert_eq!(checkpoint_payload_preview("abc", 0), "");
}
