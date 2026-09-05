package dev.ricardoleal20.chopflow.worker;

import com.google.gson.JsonElement;

/**
 * A task handler. Mirrors the Rust worker's {@code fn(JsonValue) -> Result<JsonValue>}
 * contract: it receives the task's JSON payload and returns a JSON result. Throwing
 * any exception reports the task as failed to the broker (which applies retry/backoff
 * per the task's {@code max_retries}).
 */
@FunctionalInterface
public interface TaskHandler {
  JsonElement handle(JsonElement payload) throws Exception;
}
