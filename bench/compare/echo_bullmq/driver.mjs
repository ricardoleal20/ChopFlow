// BullMQ (Node.js + Redis Streams) head-to-head benchmark against ChopFlow.
//
// BullMQ is a direct peer: a task queue that dispatches and forgets, with a
// separate worker process (like Celery's prefork worker and ChopFlow's worker
// binary). The worker runs as its own process (worker.mjs) and the driver
// (this file) submits + drains — same broker/worker split as ChopFlow, so the
// comparison isolates Node+BullMQ-vs-Rust+ChopFlow dispatch overhead with the
// same Redis broker Celery uses.
//
// Fairness notes (see bench/compare/README.md):
//   - Drain is network-mediated: the worker INCRs a Redis counter on each
//     completion, the driver polls GET on that counter. This is the same shape
//     as Celery's result-key drain and ChopFlow's /api/stats — NOT an in-process
//     atomic, so BullMQ isn't given a free drain the others don't get.
//   - Latency is sampled (uniform stride); the worker records completion time
//     for sampled ids only.
//   - One worker, 4 concurrency slots (worker.mjs concurrency:4).
//
// Usage:
//   node driver.mjs --tasks 10000 --concurrency 32 --sample-size 500
//   node driver.mjs --tasks 10000 --workload resize
//
// Emits the shared machine-readable line:
//   RESULT system=bullmq tasks=N conc=C workload=echo|resize throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
import { Queue } from 'bullmq';
import { createConnection, REDIS_URL } from './redis_conn.mjs';

const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : def;
}
const tasks = parseInt(arg('tasks', '1000'), 10);
const conc = parseInt(arg('concurrency', '16'), 10);
const sampleSize = parseInt(arg('sample-size', '500'), 10);
const workload = arg('workload', 'echo');
const W = parseInt(arg('width', '256'), 10);
const H = parseInt(arg('height', '256'), 10);
const pollInterval = parseFloat(arg('poll-interval', '0.1'));
const timeBudget = parseFloat(arg('time-budget', '0'));

function pickSample(n, size) {
  const s = new Set();
  if (size <= 0 || size >= n) { for (let i = 0; i < n; i++) s.add(i); return s; }
  const stride = n / size;
  for (let i = 0; i < size; i++) s.add(Math.round(i * stride));
  return s;
}
function pct(values, q) {
  if (!values.length) return 0;
  const s = [...values].sort((a, b) => a - b);
  let k = Math.round(q * (s.length - 1));
  k = Math.max(0, Math.min(s.length - 1, k));
  return s[k];
}

const conn = createConnection();
const queue = new Queue('echo', { connection: createConnection() });

// Reset bookkeeping keys. The worker records completion time only for jobs
// whose `sample` flag is true (set per-job below), so no separate sample
// seeding is needed.
await conn.del('bench:completed', 'bench:failures', 'bench:latency');
const sample = pickSample(tasks, sampleSize);

console.error(`bullmq driver: ${tasks} tasks (submit conc ${conc}, workload ${workload}, latency sample ${sample.size})`);

// Submit all jobs, bounded by a promise pool of size `conc`.
console.error(`submitting ${tasks} tasks…`);
const wallStart = Date.now();
const submitStart = Date.now();
const submittedAt = new Map();   // i -> submit ms (sampled only)
let submitted = 0;
async function submitBatch() {
  const batch = [];
  for (let i = 0; i < tasks; i++) {
    const isSample = sample.size >= tasks ? true : sample.has(i);
    if (isSample) submittedAt.set(i, Date.now());
    batch.push(
      queue.add('echo', { i, sample: isSample, w: W, h: H, workload }, {
        removeOnComplete: true, removeOnFail: 1000,
      })
    );
    if (batch.length >= conc) {
      await Promise.all(batch);
      submitted += batch.length;
      batch.length = 0;
    }
  }
  if (batch.length) { await Promise.all(batch); submitted += batch.length; }
}
await submitBatch();
// Persist submitted timestamps so latency can be computed from the worker's
// completion times even if the driver restarts (not needed here, but mirrors
// the Celery driver's submitted_at map).
const submitElapsed = (Date.now() - submitStart) / 1000;
console.error(`  submitted ${submitted} tasks in ${submitElapsed.toFixed(2)}s (${(submitted / submitElapsed).toFixed(0)} submit/s)`);

// Drain: poll the completion counter in Redis.
const drainStart = Date.now();
let done = 0;
while (true) {
  done = parseInt(await conn.get('bench:completed') || '0', 10);
  const elapsed = (Date.now() - drainStart) / 1000;
  process.stderr.write(`\r  completed=${String(done).padStart(7)}/${tasks}  elapsed=${elapsed.toFixed(1)}s`);
  if (done >= tasks) break;
  if (timeBudget > 0 && (Date.now() - wallStart) / 1000 >= timeBudget) {
    console.error(`\n  ⚠ time budget (${timeBudget}s) exceeded with ${tasks - done} pending`);
    break;
  }
  await new Promise((r) => setTimeout(r, pollInterval * 1000));
}
const drainElapsed = (Date.now() - drainStart) / 1000;
process.stderr.write('\n');

const failures = parseInt(await conn.get('bench:failures') || '0', 10);
const completed = parseInt(await conn.get('bench:completed') || '0', 10);

// Latency: for each sampled id, completion_time (from worker's bench:latency
// hash) - submitted_at.
const latPairs = await conn.hgetall('bench:latency');
const latenciesMs = [];
for (const [i, t1] of Object.entries(latPairs)) {
  const t0 = submittedAt.get(parseInt(i, 10));
  if (t0 != null) latenciesMs.push(parseInt(t1, 10) - t0);
}

const wallElapsed = submitElapsed + drainElapsed;
const e2e = completed / wallElapsed;

const p50 = pct(latenciesMs, 0.5);
const p95 = pct(latenciesMs, 0.95);
const p99 = pct(latenciesMs, 0.99);

const line = '─'.repeat(52);
console.log(line);
console.log(`system:             bullmq`);
console.log(`tasks:              ${tasks}`);
console.log(`concurrency:        ${conc}`);
console.log(`submit time:        ${submitElapsed.toFixed(2)}s`);
console.log(`drain time:         ${drainElapsed.toFixed(2)}s`);
console.log(`e2e throughput:     ${e2e.toFixed(0)} tasks/s`);
if (latenciesMs.length) {
  console.log(`latency p50:        ${p50.toFixed(1)} ms`);
  console.log(`latency p95:        ${p95.toFixed(1)} ms`);
  console.log(`latency p99:        ${p99.toFixed(1)} ms`);
  console.log(`(end-to-end; from ${latenciesMs.length} sampled tasks)`);
} else {
  console.log(`latency p50/p95/p99: (no timing samples captured)`);
}
console.log(`failures:           ${failures}`);
console.log(line);
console.log(`RESULT system=bullmq tasks=${tasks} conc=${conc} workload=${workload} throughput=${e2e.toFixed(0)} submit_s=${submitElapsed.toFixed(2)} drain_s=${drainElapsed.toFixed(2)} p50_ms=${p50.toFixed(2)} p95_ms=${p95.toFixed(2)} p99_ms=${p99.toFixed(2)} failures=${failures}`);

await queue.close();
await conn.quit();
process.exit(0);
