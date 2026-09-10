// BullMQ worker process — runs separately from the driver, mirroring
// ChopFlow's broker+worker split and Celery's prefork worker. Registered to
// the `echo` queue with 4 concurrency (matches ChopFlow cpu:4, Celery
// --concurrency=4). Completion is signalled by writing a completion record to
// Redis (a SET per sampled task id for latency, an INCR counter for the drain
// count) — a network-mediated drain, the same shape as Celery's result keys.
//
// Usage: node worker.mjs [--workload echo|resize] [--width 256] [--height 256]
import { Worker } from 'bullmq';
import { createConnection } from './redis_conn.mjs';

const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : def;
}
const workload = arg('workload', 'echo');
const W = parseInt(arg('width', '256'), 10);
const H = parseInt(arg('height', '256'), 10);

const conn = createConnection();
// A second connection for the drain counter / latency samples (BullMQ requires
// the worker connection to be dedicated; reuse a second for bookkeeping).
const bookConn = createConnection();

// Lazily load sharp only for the resize workload so echo runs don't pay the
// native-binary load cost.
let sharp = null;
async function doResize(w, h) {
  if (!sharp) sharp = (await import('sharp')).default;
  w = Math.max(1, w); h = Math.max(1, h);
  // Build a raw RGB gradient buffer (same shape as the other systems' resize).
  const buf = Buffer.alloc(w * h * 3);
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const o = (y * w + x) * 3;
      buf[o] = Math.round((x / w) * 255);
      buf[o + 1] = Math.round((y / h) * 255);
      buf[o + 2] = 128;
    }
  }
  await sharp(buf, { raw: { width: w, height: h, channels: 3 } })
    .resize({ width: Math.max(1, Math.floor(w / 2)), height: Math.max(1, Math.floor(h / 2)), kernel: 'nearest' })
    .raw()
    .toBuffer();
}

const worker = new Worker('echo', async (job) => {
  const { i, sample } = job.data;
  if (workload === 'resize') {
    await doResize(W, H);
  }
  // echo: no-op.
  // Bookkeeping: increment the completion counter, and record latency for
  // sampled tasks (driver pre-seeds a SET `bench:samples` with sampled i's).
  await bookConn.incr('bench:completed');
  if (sample) {
    const now = Date.now();
    await bookConn.hset('bench:latency', String(i), String(now));
  }
}, { connection: conn, concurrency: 4 });

worker.on('failed', async (job, err) => {
  await bookConn.incr('bench:completed');   // still counts as terminal
  await bookConn.incr('bench:failures');
});

worker.on('error', (err) => {
  console.error('worker error:', err.message);
});

// Keep alive until killed by run_compare.sh.
process.on('SIGTERM', async () => {
  await worker.close();
  await conn.quit();
  await bookConn.quit();
  process.exit(0);
});
process.on('SIGINT', async () => {
  await worker.close();
  await conn.quit();
  await bookConn.quit();
  process.exit(0);
});

console.error(`bullmq worker: queue=echo, concurrency=4, workload=${workload}`);
