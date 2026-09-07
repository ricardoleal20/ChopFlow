use chopflow_core::error::{ChopFlowError, Result};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;

pub type Handler = fn(serde_json::Value) -> Result<serde_json::Value>;

pub fn registry() -> HashMap<&'static str, Handler> {
    let mut m: HashMap<&'static str, Handler> = HashMap::new();
    m.insert("resize_image", resize_image);
    m.insert("batch_compute", batch_compute);
    m.insert("simulate_pipeline", simulate_pipeline);
    m.insert("flaky_handler", flaky_handler);
    m.insert("echo", echo);
    m.insert("default", echo);
    m
}

fn echo(p: serde_json::Value) -> Result<serde_json::Value> {
    Ok(serde_json::json!({ "status": "ok", "echo": p }))
}

/// Generate a synthetic gradient PNG, resize to payload dims, return size + dims.
fn resize_image(payload: serde_json::Value) -> Result<serde_json::Value> {
    let w = payload.get("width").and_then(|v| v.as_u64()).unwrap_or(64) as u32;
    let h = payload.get("height").and_then(|v| v.as_u64()).unwrap_or(64) as u32;
    let mut img = image::ImageBuffer::new(w.max(1), h.max(1));
    for (x, y, pixel) in img.enumerate_pixels_mut() {
        let r = ((x as f32 / w.max(1) as f32) * 255.0) as u8;
        let g = ((y as f32 / h.max(1) as f32) * 255.0) as u8;
        *pixel = image::Rgb([r, g, 128]);
    }
    let resized = image::imageops::resize(
        &img,
        (w / 2).max(1),
        (h / 2).max(1),
        image::imageops::FilterType::Nearest,
    );
    Ok(serde_json::json!({
        "status": "ok",
        "input_dims": [w, h],
        "output_dims": [resized.width(), resized.height()],
        "output_pixels": resized.width() * resized.height(),
    }))
}

/// CPU-bound matrix multiply over random f64 matrices sized by payload.
fn batch_compute(payload: serde_json::Value) -> Result<serde_json::Value> {
    let n = payload
        .get("n")
        .and_then(|v| v.as_u64())
        .unwrap_or(64)
        .clamp(2, 256) as usize;
    let a = nalgebra::DMatrix::<f64>::new_random(n, n);
    let b = nalgebra::DMatrix::<f64>::new_random(n, n);
    let start = std::time::Instant::now();
    let c = &a * &b;
    let elapsed = start.elapsed();
    let checksum: f64 = c.iter().sum();
    Ok(serde_json::json!({
        "status": "ok", "matrix_size": n, "elapsed_ms": elapsed.as_millis() as u64, "checksum": checksum,
    }))
}

/// Multi-stage simulated pipeline with staged sleeps + per-stage timings.
fn simulate_pipeline(payload: serde_json::Value) -> Result<serde_json::Value> {
    let stages = payload
        .get("stages")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(3)
        .max(1);
    let names = ["download", "process", "upload"];
    let mut timings = Vec::new();
    for i in 0..stages {
        let start = std::time::Instant::now();
        let ms = 200 + (i as u64 * 150);
        std::thread::sleep(std::time::Duration::from_millis(ms));
        let name = names[i.min(2)];
        timings.push(serde_json::json!({ "stage": i, "name": name, "ms": start.elapsed().as_millis() as u64 }));
    }
    Ok(serde_json::json!({ "status": "ok", "stages": timings }))
}

/// Fails ~30% of the time (seeded by payload) to exercise retries.
fn flaky_handler(payload: serde_json::Value) -> Result<serde_json::Value> {
    let seed = payload.get("seed").and_then(|v| v.as_u64()).unwrap_or(0);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    if rng.gen_bool(0.3) {
        return Err(ChopFlowError::Other(anyhow::anyhow!(
            "flaky failure (simulated)"
        )));
    }
    Ok(serde_json::json!({ "status": "ok", "seed": seed }))
}
