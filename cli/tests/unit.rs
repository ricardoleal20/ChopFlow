//! Unit tests for pure CLI helpers (no broker needed).

use chopflow_cli::{
    build_schedule, build_schedule_kind, parse_overlap, parse_resource_map, status_name,
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
