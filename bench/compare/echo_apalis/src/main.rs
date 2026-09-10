// apalis (Rust + Redis) head-to-head benchmark against ChopFlow.
//
// Same workload as bench/bench.py: a no-op `echo` (or a real `resize` image
// op) task, isolating queue/dispatch overhead. One worker, 4 concurrent task
// slots — same execution shape as ChopFlow cpu:4 and Celery --concurrency=4.
// Redis-backed, so the comparison to Celery isolates Rust-vs-Python with the
// *same* broker, and to ChopFlow isolates Redis-vs-in-memory.
//
// Fairness notes (see bench/compare/README.md):
//   - The worker runs in-process (same shape as echo_temporal.py).
//   - Completion is tracked via an in-process AtomicU64 the handler bumps on
//     each finish — the cheapest neutral drain apalis offers, mirroring how
//     each system uses its own lightest completion signal (ChopFlow: /api/stats,
//     Celery: Redis result keys).
//   - Latency is sampled (uniform stride) and recorded in-process by the
//     handler, so no external status polling is needed.
//
// Usage:
//   cargo run -r -- --tasks 10000 --concurrency 32 --sample-size 500
//   cargo run -r -- --tasks 10000 --workload resize
//
// Emits the shared machine-readable line:
//   RESULT system=apalis tasks=N conc=C throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use apalis::prelude::*;
use apalis_redis::{Config, RedisStorage};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tower::limit::ConcurrencyLimitLayer;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Echo {
    i: u64,
    work: String, // "echo" | "resize"
    w: u32,
    h: u32,
}

/// Shared benchmark state injected into the worker via apalis Data.
#[derive(Default)]
struct BenchState {
    completed: AtomicU64,
    failures: AtomicU64,
    sample: RwLock<HashSet<u64>>,
    submitted_at: RwLock<HashMap<u64, Instant>>,
    completed_at: RwLock<HashMap<u64, Instant>>,
}

impl BenchState {
    fn mark_submitted(&self, i: u64) {
        if self.sample.read().unwrap().contains(&i) {
            self.submitted_at.write().unwrap().insert(i, Instant::now());
        }
    }
    fn mark_completed(&self, i: u64) {
        if self.sample.read().unwrap().contains(&i) {
            self.completed_at.write().unwrap().insert(i, Instant::now());
        }
    }
}

async fn handle(job: Echo, state: Data<Arc<BenchState>>) -> Result<(), Error> {
    let res = match job.work.as_str() {
        "resize" => do_resize(job.w, job.h),
        _ => Ok(()),
    };
    match res {
        Ok(_) => {
            state.completed.fetch_add(1, Ordering::Relaxed);
            state.mark_completed(job.i);
            Ok(())
        }
        Err(e) => {
            state.failures.fetch_add(1, Ordering::Relaxed);
            state.completed.fetch_add(1, Ordering::Relaxed);
            state.mark_completed(job.i);
            Err(Error::Failed(Arc::new(e)))
        }
    }
}

fn do_resize(w: u32, h: u32) -> Result<(), BoxDynError> {
    let w = w.max(1);
    let h = h.max(1);
    let mut img = image::ImageBuffer::new(w, h);
    for (x, y, pixel) in img.enumerate_pixels_mut() {
        let r = ((x as f32 / w as f32) * 255.0) as u8;
        let g = ((y as f32 / h as f32) * 255.0) as u8;
        *pixel = image::Rgb([r, g, 128]);
    }
    let _ = image::imageops::resize(&img, (w / 2).max(1), (h / 2).max(1), image::imageops::FilterType::Nearest);
    Ok(())
}

fn pick_sample(n: u64, sample_size: usize) -> HashSet<u64> {
    if sample_size == 0 || sample_size as u64 >= n {
        return (0..n).collect();
    }
    let stride = n as f64 / sample_size as f64;
    (0..sample_size as u64)
        .map(|i| (i as f64 * stride).round() as u64)
        .collect()
}

fn pct(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut s = values.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let k = ((q * (s.len() - 1) as f64).round() as usize).min(s.len() - 1);
    s[k]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks: u64 = 1000;
    let mut conc: usize = 16;
    let mut sample_size: usize = 500;
    let mut workload = String::from("echo");
    let mut poll_interval = 0.001f64;
    let mut time_budget = 0.0f64;
    let mut w = 256u32;
    let mut h = 256u32;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--tasks" => tasks = args.next().unwrap().parse()?,
            "--concurrency" => conc = args.next().unwrap().parse()?,
            "--sample-size" => sample_size = args.next().unwrap().parse()?,
            "--workload" => workload = args.next().unwrap(),
            "--poll-interval" => poll_interval = args.next().unwrap().parse()?,
            "--time-budget" => time_budget = args.next().unwrap().parse()?,
            "--width" => w = args.next().unwrap().parse()?,
            "--height" => h = args.next().unwrap().parse()?,
            _ => {}
        }
    }

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".into());
    let conn = apalis_redis::connect(redis_url.clone())
        .await
        .map_err(|e| format!("cannot reach Redis at {redis_url}: {e}"))?;
    // apalis-redis defaults (buffer_size=10, poll_interval=100ms) cap throughput at
    // ~100 jobs/s — an artificial throttle, not a real limit. A reasonable production
    // config (1ms poll, 1000-job fetch batch) lets Redis feed the 4 worker slots
    // without starving them. This is the apalis equivalent of Celery's
    // worker_prefetch_multiplier tuning; documented in bench/compare/README.md.
    let config = Config::default()
        .set_poll_interval(Duration::from_millis(1))
        .set_buffer_size(1000);
    let storage = RedisStorage::new_with_config(conn, config);

    let state = Arc::new(BenchState {
        completed: AtomicU64::new(0),
        failures: AtomicU64::new(0),
        sample: RwLock::new(pick_sample(tasks, sample_size)),
        submitted_at: RwLock::new(HashMap::new()),
        completed_at: RwLock::new(HashMap::new()),
    });

    eprintln!(
        "apalis driver: {tasks} tasks (submit conc {conc}, workload {workload}, latency sample {})",
        state.sample.read().unwrap().len()
    );

    // Start the worker in-process: one worker, 4 concurrent task slots.
    let worker_state = state.clone();
    let worker_storage = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = WorkerBuilder::new("apalis-bench")
            .layer(ConcurrencyLimitLayer::new(4))
            .data(worker_state)
            .backend(worker_storage)
            .build_fn(handle);
        worker.run().await
    });

    // Submit all jobs, bounded by buffer_unordered(conc).
    eprintln!("submitting {tasks} tasks…");
    let wall_start = Instant::now();
    let submit_start = Instant::now();
    let submit_state = state.clone();
    let submit_storage = storage.clone();
    let work = workload.clone();
    let submit_count = tasks;
    let push: Vec<_> = futures::stream::iter(0..submit_count)
        .map(|i| {
            let mut st = submit_storage.clone();
            let state = submit_state.clone();
            let work = work.clone();
            async move {
                state.mark_submitted(i);
                let _ = st
                    .push(Echo { i, work: work.clone(), w, h })
                    .await;
            }
        })
        .buffer_unordered(conc)
        .collect()
        .await;
    let _ = push;
    let submit_elapsed = submit_start.elapsed();
    eprintln!(
        "  submitted {tasks} tasks in {:.2}s ({:.0} submit/s)",
        submit_elapsed.as_secs_f64(),
        tasks as f64 / submit_elapsed.as_secs_f64()
    );

    // Drain: poll the in-process completion counter.
    let drain_start = Instant::now();
    loop {
        let done = state.completed.load(Ordering::Relaxed);
        let elapsed = drain_start.elapsed().as_secs_f64();
        eprint!("\r  completed={done:>7}/{tasks}  elapsed={elapsed:6.1}s");
        if done >= tasks {
            break;
        }
        if time_budget > 0.0 && wall_start.elapsed().as_secs_f64() >= time_budget {
            eprintln!("\n  ⚠ time budget ({time_budget}s) exceeded with {} pending", tasks - done);
            break;
        }
        tokio::time::sleep(Duration::from_secs_f64(poll_interval)).await;
    }
    let drain_elapsed = drain_start.elapsed();
    eprintln!();

    // Stop the worker (best effort; we exit the process anyway).
    worker.abort();

    let completed = state.completed.load(Ordering::Relaxed);
    let failures = state.failures.load(Ordering::Relaxed);
    let drained = completed;

    let submitted = state.submitted_at.read().unwrap();
    let done_map = state.completed_at.read().unwrap();
    let latencies_ms: Vec<f64> = submitted
        .iter()
        .filter_map(|(i, t0)| done_map.get(i).map(|t1| t1.duration_since(*t0).as_millis() as f64))
        .collect();

    let wall_elapsed = submit_elapsed + drain_elapsed;
    let e2e = drained as f64 / wall_elapsed.as_secs_f64();

    println!("{}", "─".repeat(52));
    println!("system:             apalis");
    println!("tasks:              {tasks}");
    println!("concurrency:        {conc}");
    println!("submit time:        {:.2}s", submit_elapsed.as_secs_f64());
    println!("drain time:         {:.2}s", drain_elapsed.as_secs_f64());
    println!("e2e throughput:     {:.0} tasks/s", e2e);
    if !latencies_ms.is_empty() {
        println!("latency p50:        {:.1} ms", pct(&latencies_ms, 0.50));
        println!("latency p95:        {:.1} ms", pct(&latencies_ms, 0.95));
        println!("latency p99:        {:.1} ms", pct(&latencies_ms, 0.99));
        println!("(end-to-end; from {} sampled tasks)", latencies_ms.len());
    } else {
        println!("latency p50/p95/p99: (no timing samples captured)");
    }
    println!("failures:           {failures}");
    println!("{}", "─".repeat(52));
    println!(
        "RESULT system=apalis tasks={tasks} conc={conc} throughput={:.0} submit_s={:.2} drain_s={:.2} p50_ms={:.2} p95_ms={:.2} p99_ms={:.2} failures={failures}",
        e2e,
        submit_elapsed.as_secs_f64(),
        drain_elapsed.as_secs_f64(),
        pct(&latencies_ms, 0.50),
        pct(&latencies_ms, 0.95),
        pct(&latencies_ms, 0.99)
    );
    Ok(())
}
