//! RAG ingestion demo — a durable, resumable three-stage pipeline.
//!
//! `rag.ingest` is registered via `TaskRegistry::register_ctx` and declares
//! the stages `["chunk", "embed", "index"]`. Each stage persists a checkpoint
//! through [`TaskCtx::checkpoint`] before moving on, so a retried task picks
//! up where the previous attempt stopped instead of restarting:
//!
//! 1. **chunk** — split `payload.document` into chunks of `payload.chunk_size`
//!    words (default 64); checkpointed as `{"chunks": [...], "chunk_size": n}`.
//!    Skipped entirely when a `"chunk"` checkpoint already exists (its payload
//!    is the source of truth for the chunk list).
//! 2. **embed** — resumable: the last `"embed"` checkpoint's `next_index`
//!    says how many chunks are already embedded. Every remaining chunk gets a
//!    deterministic pseudo-embedding and progress is checkpointed after each
//!    one (`{"next_index": i + 1}`), so a mid-flight retry demonstrates
//!    resumption at chunk granularity. The final checkpoint is
//!    `{"next_index": len, "dim": 16}`.
//! 3. **index** — `{"stored": len}`, the notional vector-store write.
//!
//! The "embedding" is a stand-in for a real embedding model: a 16-dim vector
//! derived from a stable FNV-1a hash of the chunk text (see [`embed_chunk`])
//! — no API calls, no randomness. The same chunk always embeds to the same
//! vector, which is all the demo needs to be deterministic and replayable.

use chopflow_worker::TaskCtx;
use serde_json::{json, Value};

/// The pipeline stages `rag.ingest` declares on its task.
pub(crate) const STAGES: [&str; 3] = ["chunk", "embed", "index"];

/// Dimensionality of the pseudo-embeddings.
const EMBED_DIM: usize = 16;

/// Default words per chunk when `payload.chunk_size` is absent.
const DEFAULT_CHUNK_SIZE: u64 = 64;

/// The `rag.ingest` handler: chunk → embed → index, checkpointing each stage.
pub(crate) async fn ingest(ctx: TaskCtx, payload: Value) -> std::result::Result<Value, String> {
    let document = payload
        .get("document")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "rag.ingest requires a string `document` field".to_string())?
        .to_string();
    let chunk_size = payload
        .get("chunk_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_CHUNK_SIZE)
        .max(1) as usize;

    // Where did this attempt resume from? The furthest stage in our own
    // declared pipeline (STAGES) that already had a checkpoint when the
    // handler started — `null` on a fresh run. (Computed before we record any
    // new checkpoints.)
    let resumed_from = STAGES
        .iter()
        .rev()
        .find(|stage| ctx.stage_checkpoint(stage).is_some())
        .map(|stage| stage.to_string());

    // Stage 1: chunk — skipped when a "chunk" checkpoint already exists; its
    // payload carries the chunk list forward.
    let chunks = match ctx.stage_checkpoint("chunk") {
        Some(cp) => parse_chunk_checkpoint(&cp.payload)?,
        None => {
            let chunks = chunk_words(&document, chunk_size);
            ctx.checkpoint(
                "chunk",
                json!({ "chunks": chunks, "chunk_size": chunk_size }),
            )
            .await?;
            chunks
        }
    };

    // Stage 2: embed — resumable from the last "embed" checkpoint's
    // `next_index` (0 when none). Checkpoint after every chunk so a mid-flight
    // retry resumes at chunk granularity.
    let mut next_index = match ctx.stage_checkpoint("embed") {
        Some(cp) => {
            let saved: Value = serde_json::from_str(&cp.payload)
                .map_err(|e| format!("corrupt 'embed' checkpoint: {e}"))?;
            saved
                .get("next_index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize
        }
        None => 0,
    }
    .min(chunks.len());

    while next_index < chunks.len() {
        // The pseudo-embedding stands in for a real embedding model call.
        let _embedding = embed_chunk(&chunks[next_index]);
        next_index += 1;
        ctx.checkpoint("embed", json!({ "next_index": next_index }))
            .await?;
    }
    ctx.checkpoint(
        "embed",
        json!({ "next_index": chunks.len(), "dim": EMBED_DIM }),
    )
    .await?;

    // Stage 3: index — the notional vector-store write.
    ctx.checkpoint("index", json!({ "stored": chunks.len() }))
        .await?;

    Ok(json!({
        "chunks": chunks.len(),
        "dim": EMBED_DIM,
        "indexed": true,
        "resumed_from": resumed_from,
    }))
}

/// Read the chunk list back out of a stored `"chunk"` checkpoint payload.
fn parse_chunk_checkpoint(payload: &str) -> std::result::Result<Vec<String>, String> {
    let saved: Value =
        serde_json::from_str(payload).map_err(|e| format!("corrupt 'chunk' checkpoint: {e}"))?;
    let chunks = saved
        .get("chunks")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "corrupt 'chunk' checkpoint: missing chunks".to_string())?;
    chunks
        .iter()
        .map(|c| {
            c.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| "corrupt 'chunk' checkpoint: non-string chunk".to_string())
        })
        .collect()
}

/// Split `document` into chunks of at most `chunk_size` whitespace-separated
/// words (the last chunk may be shorter). An empty (or all-whitespace)
/// document yields no chunks.
pub(crate) fn chunk_words(document: &str, chunk_size: usize) -> Vec<String> {
    let chunk_size = chunk_size.max(1);
    let mut chunks = Vec::new();
    let mut current: Vec<&str> = Vec::with_capacity(chunk_size);

    for word in document.split_whitespace() {
        current.push(word);
        if current.len() == chunk_size {
            chunks.push(current.join(" "));
            current.clear();
        }
    }
    if !current.is_empty() {
        chunks.push(current.join(" "));
    }
    chunks
}

/// Deterministic 16-dim pseudo-embedding of `text`.
///
/// Dimension `i` is derived from an FNV-1a hash of the text seeded with the
/// dimension index, mapped into `[-1.0, 1.0]` at 3-decimal resolution. This
/// stands in for a real embedding call — no API, no randomness — while
/// keeping the demo deterministic: the same chunk always embeds to the same
/// vector, on every machine and every retry.
pub(crate) fn embed_chunk(text: &str) -> Vec<f32> {
    (0..EMBED_DIM)
        .map(|dim| {
            let hash = fnv1a(text.as_bytes(), dim as u64);
            ((hash % 2001) as f32) / 1000.0 - 1.0
        })
        .collect()
}

/// FNV-1a hash of `data`, offset by `seed` so each dimension hashes
/// independently.
fn fnv1a(data: &[u8], seed: u64) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET_BASIS ^ seed;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_words_splits_by_word_count() {
        let document = "one two three four five six seven eight nine ten";
        let chunks = chunk_words(document, 4);
        assert_eq!(
            chunks,
            vec![
                "one two three four".to_string(),
                "five six seven eight".to_string(),
                "nine ten".to_string(),
            ]
        );
    }

    #[test]
    fn chunk_words_size_one_is_words() {
        let chunks = chunk_words("a b c", 1);
        assert_eq!(chunks, vec!["a", "b", "c"]);
    }

    #[test]
    fn chunk_words_larger_than_document_is_single_chunk() {
        let chunks = chunk_words("a b", 64);
        assert_eq!(chunks, vec!["a b".to_string()]);
    }

    #[test]
    fn chunk_words_empty_document_yields_no_chunks() {
        assert!(chunk_words("", 4).is_empty());
        assert!(chunk_words("   \n\t ", 4).is_empty());
    }

    #[test]
    fn embed_chunk_is_deterministic() {
        // Same input → same vector (the whole point of the pseudo-embedding).
        assert_eq!(
            embed_chunk("the quick brown fox"),
            embed_chunk("the quick brown fox")
        );
    }

    #[test]
    fn embed_chunk_has_dim_and_range() {
        let v = embed_chunk("hello world");
        assert_eq!(v.len(), EMBED_DIM);
        assert!(v.iter().all(|x| (-1.0..=1.0).contains(x)));
    }

    #[test]
    fn embed_chunk_differs_for_different_text() {
        assert_ne!(embed_chunk("hello"), embed_chunk("world"));
    }

    // --- Integration-style: rag.ingest against an in-process broker --------
    //
    // The demos crate has no separate tests/ harness; these tests run the
    // real handler through the real worker execution path (fetch + execute)
    // against an in-process broker, like worker/tests/integration.rs does.

    use chopflow_broker::chopflow::{
        self, chop_flow_broker_client::ChopFlowBrokerClient, EnqueueTaskRequest,
        GetCheckpointsRequest, GetTaskStatusRequest,
    };
    use chopflow_broker::{build_storage, ChopFlowBrokerService, StorageBackend};
    use chopflow_core::resources::parse_resources_ext;
    use chopflow_worker::{
        connect_and_register_with_refills, execute_task, fetch_tasks, resources_from_declarations,
        TaskRegistry, WorkerState,
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tonic::Request;

    /// The demo worker's resource declaration: a static cpu plus a
    /// replenishing llm.rpm bucket (what `demos/rag.sh` passes).
    const RESOURCES: &str = "cpu:4,llm.rpm:60@60/60";

    /// Start a broker on an ephemeral port; return its gRPC URL.
    async fn broker_url() -> String {
        let storage = build_storage(&StorageBackend::Memory).unwrap();
        let service = ChopFlowBrokerService::new(storage);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let _ = chopflow_broker::serve_with_listener(service, listener).await;
        });

        format!("http://{}", addr)
    }

    /// A `rag.ingest` registry exactly like the demo worker's.
    fn rag_registry() -> TaskRegistry {
        let mut registry = TaskRegistry::new();
        registry.register_ctx("rag.ingest", ingest);
        registry
    }

    async fn status_of(
        client: &mut ChopFlowBrokerClient<tonic::transport::Channel>,
        task_id: &str,
    ) -> chopflow::Task {
        client
            .get_task_status(Request::new(GetTaskStatusRequest {
                task_id: task_id.to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .task
            .unwrap()
    }

    /// Enqueue a `rag.ingest` task (stages + llm.rpm requirement, like the
    /// seed does).
    async fn enqueue_rag(
        client: &mut ChopFlowBrokerClient<tonic::transport::Channel>,
        document: &str,
    ) -> String {
        let resp = client
            .enqueue_task(Request::new(EnqueueTaskRequest {
                name: "rag.ingest".into(),
                payload: serde_json::json!({ "document": document }).to_string(),
                tags: vec!["demo".into()],
                eta: None,
                max_retries: 3,
                resources: [("llm.rpm".to_string(), 1)].into(),
                priority: 0,
                stages: STAGES.iter().map(|s| s.to_string()).collect(),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
        resp.into_inner().task_id
    }

    /// Fetch the enqueued task through the worker's own pull path and execute
    /// it (returns once the task has been acked).
    async fn fetch_and_execute(worker_state: &Arc<Mutex<WorkerState>>) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let tasks = loop {
            let fetched = fetch_tasks(worker_state, 4).await.unwrap();
            if !fetched.is_empty() {
                break fetched;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("worker never fetched the rag.ingest task within 10s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        for task in tasks {
            execute_task(worker_state, task).await.unwrap();
        }
    }

    async fn rag_worker(url: &str) -> Arc<Mutex<WorkerState>> {
        let tags = vec!["demo".to_string()];
        let (capacities, refills) = parse_resources_ext(RESOURCES).unwrap();
        let worker_id = connect_and_register_with_refills(url, &tags, &capacities, &refills)
            .await
            .unwrap();
        let resources = resources_from_declarations(&capacities, &refills);
        Arc::new(Mutex::new(
            WorkerState::with_registry(
                worker_id,
                url.to_string(),
                resources,
                tags,
                4,
                rag_registry(),
            )
            .unwrap(),
        ))
    }

    #[tokio::test]
    async fn rag_ingest_completes_and_records_all_stage_checkpoints() {
        let url = broker_url().await;
        let worker = rag_worker(&url).await;
        let mut client = ChopFlowBrokerClient::connect(url.clone()).await.unwrap();

        let document = "ChopFlow is a distributed task queue. Workers pull tasks \
                        from a broker and acknowledge results. It supports retries.";
        let task_id = enqueue_rag(&mut client, document).await;
        fetch_and_execute(&worker).await;
        let task = status_of(&mut client, &task_id).await;

        assert_eq!(task.status, chopflow::TaskStatus::Completed as i32);
        let result: Value = serde_json::from_str(&task.result).unwrap();
        assert_eq!(result["indexed"], true);
        assert_eq!(result["dim"], 16);
        assert_eq!(result["resumed_from"], serde_json::Value::Null);
        // 24 words at the default chunk_size of 64 → a single chunk.
        assert_eq!(result["chunks"], 1);

        // All three stage checkpoints exist on the broker.
        let checkpoints = client
            .get_checkpoints(Request::new(GetCheckpointsRequest {
                task_id: task.id.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .checkpoints;
        let stages: Vec<&str> = checkpoints.iter().map(|c| c.stage.as_str()).collect();
        for stage in STAGES {
            assert!(stages.contains(&stage), "missing '{stage}' checkpoint");
        }

        // The chunk checkpoint carries the chunk list + size; the final embed
        // checkpoint carries next_index == chunks.len() and the dimension.
        let chunk_cp = checkpoints.iter().find(|c| c.stage == "chunk").unwrap();
        let chunk_payload: Value = serde_json::from_str(&chunk_cp.payload).unwrap();
        assert_eq!(chunk_payload["chunk_size"], 64);
        assert_eq!(chunk_payload["chunks"].as_array().unwrap().len(), 1);

        let embed_cp = checkpoints.iter().find(|c| c.stage == "embed").unwrap();
        let embed_payload: Value = serde_json::from_str(&embed_cp.payload).unwrap();
        assert_eq!(embed_payload["next_index"], 1);
        assert_eq!(embed_payload["dim"], 16);
    }

    #[tokio::test]
    async fn rag_ingest_resumes_from_a_prior_chunk_checkpoint() {
        let url = broker_url().await;
        let worker = rag_worker(&url).await;
        let mut client = ChopFlowBrokerClient::connect(url.clone()).await.unwrap();

        // Enqueue the task, then pre-seed a "chunk" checkpoint the way a
        // crashed first attempt would have left it: 3 chunks of 2 words each.
        let document = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
        let task_id = enqueue_rag(&mut client, document).await;

        let pre_chunked = serde_json::json!({
            "chunks": ["alpha beta", "gamma delta", "epsilon zeta"],
            "chunk_size": 2,
        });
        client
            .save_checkpoint(Request::new(chopflow::SaveCheckpointRequest {
                task_id: task_id.clone(),
                stage: "chunk".to_string(),
                payload: pre_chunked.to_string(),
            }))
            .await
            .unwrap();

        fetch_and_execute(&worker).await;
        let task = status_of(&mut client, &task_id).await;

        assert_eq!(task.status, chopflow::TaskStatus::Completed as i32);
        let result: Value = serde_json::from_str(&task.result).unwrap();
        assert_eq!(result["indexed"], true);
        // Resumed from the checkpointed chunk stage, not a fresh run.
        assert_eq!(result["resumed_from"], "chunk");
        // The checkpointed chunk list (3 chunks) is what got embedded and
        // indexed — a fresh re-chunk of the 10-word document at the default
        // chunk_size of 64 would have produced a single chunk.
        assert_eq!(result["chunks"], 3);

        // The chunk checkpoint still carries the seeded chunks: the handler
        // must have skipped re-chunking (no upsert).
        let checkpoints = client
            .get_checkpoints(Request::new(GetCheckpointsRequest {
                task_id: task_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .checkpoints;
        let chunk_cp = checkpoints.iter().find(|c| c.stage == "chunk").unwrap();
        let chunk_payload: Value = serde_json::from_str(&chunk_cp.payload).unwrap();
        assert_eq!(
            chunk_payload["chunks"],
            serde_json::json!(["alpha beta", "gamma delta", "epsilon zeta"])
        );
    }
}
