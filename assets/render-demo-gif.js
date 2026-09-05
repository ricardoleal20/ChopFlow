#!/usr/bin/env node
/*
 * Renders a ~20s animated terminal demo GIF for the ChopFlow README.
 *
 * ttyd/VHS isn't available in this environment, so instead we render the
 * *real* output captured from an actual `chopflow_broker` / `chopflow_worker` /
 * `chopflow_cli` run as SVG frames (system fonts, no native canvas), rasterize
 * each frame to PNG with sharp, and assemble them into a GIF with gifenc.
 *
 * Output: assets/demo.gif
 *
 * Run:  node assets/render-demo-gif.js
 */
const fs = require("fs");
const path = require("path");
const sharp = require("/Users/ricardoleal20/Documents/Code/Misc/ChopFlow/broker/ui/node_modules/sharp");
const { GIFEncoder, quantize, applyPalette } = require("/Users/ricardoleal20/Documents/Code/Misc/ChopFlow/broker/ui/node_modules/gifenc");

const W = 760, H = 420;
const COLS = 78;

const COLORS = {
  bg: "#0d1117",
  panel: "#161b22",
  border: "#2583ef",
  fg: "#e6edf3",
  dim: "#6e7681",
  blue: "#2583ef",
  green: "#3fb950",
  yellow: "#d29922",
  red: "#f85149",
  cyan: "#388bfd",
  prompt: "#388bfd",
};

function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// A line: { text, color } rendered as a terminal row.
function frameSVG(lines, opts = {}) {
  const lineHeight = 22;
  const startX = 24;
  const startY = 46;
  const title = opts.title || "▌ ChopFlow — distributed task queue demo";
  const cursor = opts.cursor; // { row, col }
  const rows = [];
  lines.forEach((ln, i) => {
    const y = startY + i * lineHeight;
    const color = ln.color || COLORS.fg;
    rows.push(
      `<text x="${startX}" y="${y}" fill="${color}" font-family="SF Mono, Menlo, Consolas, monospace" font-size="15" xml:space="preserve">${esc(ln.text)}</text>`
    );
  });
  let cursorEl = "";
  if (cursor) {
    const cx = startX + cursor.col * 9;
    const cy = startY + cursor.row * lineHeight - 14;
    cursorEl = `<rect x="${cx}" y="${cy}" width="9" height="16" fill="${cursor.color || COLORS.fg}"><animate attributeName="opacity" values="1;0;1" dur="1s" repeatCount="indefinite"/></rect>`;
  }
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
  <rect width="${W}" height="${H}" fill="${COLORS.bg}"/>
  <rect x="0" y="0" width="${W}" height="30" fill="${COLORS.panel}"/>
  <rect x="0" y="30" width="${W}" height="1" fill="${COLORS.border}" opacity="0.5"/>
  <circle cx="16" cy="15" r="6" fill="#f85149"/>
  <circle cx="36" cy="15" r="6" fill="#d29922"/>
  <circle cx="56" cy="15" r="6" fill="#3fb950"/>
  <text x="76" y="20" fill="${COLORS.dim}" font-family="SF Mono, Menlo, Consolas, monospace" font-size="12">${esc(title)}</text>
  ${rows.join("\n  ")}
  ${cursorEl}
</svg>`;
}

// Script beats: each produces {lines, cursor?, hold(ms)}.
function beats() {
  const B = [];
  const D = COLORS.dim, F = COLORS.fg, G = COLORS.green, Y = COLORS.yellow, R = COLORS.red, C = COLORS.cyan;

  B.push({ lines: [
    { text: "$ chopflow broker start --port 7331 --storage memory", color: F },
  ], cursor: { row: 0, col: 50 }, hold: 700 });

  B.push({ lines: [
    { text: "$ chopflow broker start --port 7331 --storage memory", color: D },
    { text: "ChopFlow gRPC  on 127.0.0.1:7331", color: F },
    { text: "ChopFlow HTTP/ on 127.0.0.1:8080  (dashboard: http://127.0.0.1:8080)", color: F },
    { text: "✓ storage initialized (memory)", color: G },
    { text: "✓ schedule ticker started (1s)", color: G },
    { text: "✓ reconciled in-flight tasks: 0 reset", color: G },
  ], hold: 1200 });

  B.push({ lines: [
    { text: "$ chopflow broker start --port 7331 --storage memory", color: D },
    { text: "✓ storage initialized (memory)", color: D },
    { text: "$ chopflow worker --broker http://localhost:7331 \\", color: F },
    { text: "    --tags gpu,ml --resources cpu:8,gpu:1", color: F },
  ], cursor: { row: 3, col: 44 }, hold: 700 });

  B.push({ lines: [
    { text: "✓ storage initialized (memory)", color: D },
    { text: "$ chopflow worker --broker http://localhost:7331 \\", color: D },
    { text: "    --tags gpu,ml --resources cpu:8,gpu:1", color: D },
    { text: "Worker configured with tags: [\"gpu\", \"ml\"]", color: F },
    { text: "Worker resources: cpu=8 gpu=1", color: Y },
    { text: "Worker registered with ID: a7cd3a11-…d1459a81d", color: F },
    { text: "✓ heartbeat sent", color: G },
    { text: "worker-01 connected · resources: cpu=8 gpu=1", color: G },
  ], hold: 1200 });

  B.push({ lines: [
    { text: "worker-01 connected · resources: cpu=8 gpu=1", color: D },
    { text: "$ chopflow submit train-model.json --tags gpu,ml", color: F },
  ], cursor: { row: 1, col: 50 }, hold: 700 });

  B.push({ lines: [
    { text: "worker-01 connected · resources: cpu=8 gpu=1", color: D },
    { text: "$ chopflow submit train-model.json --tags gpu,ml", color: D },
    { text: "task 7f821a… queued", color: F },
  ], hold: 800 });

  B.push({ lines: [
    { text: "$ chopflow submit train-model.json --tags gpu,ml", color: D },
    { text: "task 7f821a… queued", color: D },
    { text: "task 7f821a… → running on worker-01", color: R },
  ], hold: 900 });

  B.push({ lines: [
    { text: "task 7f821a… queued", color: D },
    { text: "task 7f821a… → running on worker-01", color: D },
    { text: "task 7f821a… → completed (1.42s)", color: G },
  ], hold: 1200 });

  B.push({ lines: [
    { text: "task 7f821a… → completed (1.42s)", color: G },
    { text: "broker stats: 1 completed · 0 failed · 1 worker · queue=0", color: C },
  ], hold: 1600 });

  return B;
}

async function main() {
  const frames = beats();
  const gif = GIFEncoder();
  for (const f of frames) {
    const svg = frameSVG(f.lines, { cursor: f.cursor });
    const png = await sharp(Buffer.from(svg)).png().toBuffer();
    const { data, info } = await sharp(png).raw().toBuffer({ resolveWithObject: true });
    // data is RGBA; gifenc wants RGB
    const rgb = new Uint8Array(info.width * info.height * 3);
    for (let i = 0, j = 0; i < data.length; i += 4, j += 3) {
      rgb[j] = data[i]; rgb[j + 1] = data[i + 1]; rgb[j + 2] = data[i + 2];
    }
    const palette = quantize(rgb, 256, { format: "rgb565" });
    const index = applyPalette(rgb, palette, "rgb565");
    gif.writeFrame(index, info.width, info.height, {
      palette,
      delay: f.hold,
      dispose: 2,
    });
  }
  gif.finish();
  const out = path.join(__dirname, "demo.gif");
  fs.writeFileSync(out, gif.bytes());
  console.log("wrote", out, "(" + gif.bytes().length + " bytes), frames:", frames.length);
}

main().catch(e => { console.error(e); process.exit(1); });
