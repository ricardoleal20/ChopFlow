use chopflow_broker::{ChopFlowBrokerService, BrokerState};
use chopflow_core::storage::Storage;
use chopflow_core::schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate};
use chopflow_core::task::TaskStatus;
use std::collections::HashMap;

fn tmpl(name: &str) -> TaskTemplate {
    TaskTemplate { name: name.into(), payload: serde_json::json!({}), tags: vec![], resources: HashMap::new(), max_retries: 3 }
}

#[tokio::test]
async fn ticker_materializes_task_from_cron_schedule() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    // A cron schedule due now (every second).
    let mut sch = Schedule::new("every-sec".into(), tmpl("ping"), ScheduleKind::Cron { cron: "* * * * * *".into() }, OverlapPolicy::Allow).unwrap();
    sch.next_fire = chrono::Utc::now() - chrono::Duration::seconds(1);
    storage.insert_schedule(sch.clone()).await.unwrap();

    // Run one tick.
    service.tick_once().await.unwrap();

    // A task should have been materialized with schedule_id set + advanced next_fire.
    let in_flight = storage.in_flight_for_schedule(&sch.id).await.unwrap();
    assert_eq!(in_flight.len(), 1);
    assert_eq!(in_flight[0].name, "ping");
    assert_eq!(in_flight[0].schedule_id, Some(sch.id));
    let updated = storage.get_schedule(&sch.id).await.unwrap().unwrap();
    assert!(updated.next_fire > chrono::Utc::now());
    assert!(updated.last_fired.is_some());
}

#[tokio::test]
async fn ticker_oneshot_self_disables_after_fire() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    let eta = chrono::Utc::now() - chrono::Duration::minutes(1);
    let sch = Schedule::new("once".into(), tmpl("ping"), ScheduleKind::OneShot { eta }, OverlapPolicy::Allow).unwrap();
    let id = sch.id;
    storage.insert_schedule(sch).await.unwrap();

    service.tick_once().await.unwrap();

    let updated = storage.get_schedule(&id).await.unwrap().unwrap();
    assert!(!updated.enabled, "one-shot should self-disable");
    assert_eq!(storage.in_flight_for_schedule(&id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn ticker_overlap_skip_skips_when_in_flight() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    let mut sch = Schedule::new("skipper".into(), tmpl("ping"), ScheduleKind::Cron { cron: "* * * * * *".into() }, OverlapPolicy::Skip).unwrap();
    sch.next_fire = chrono::Utc::now() - chrono::Duration::seconds(1);
    let id = sch.id;
    storage.insert_schedule(sch).await.unwrap();

    // Pre-place a running task for this schedule.
    let mut t = chopflow_core::task::Task::new("ping".into(), serde_json::json!({}));
    t.status = TaskStatus::Running;
    t.schedule_id = Some(id);
    storage.insert(t).await.unwrap();

    service.tick_once().await.unwrap();

    // Only the pre-existing task — no new one materialized.
    assert_eq!(storage.in_flight_for_schedule(&id).await.unwrap().len(), 1);
}
