package dev.ricardoleal20.chopflow.examples;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.ricardoleal20.chopflow.worker.ChopFlowWorker;
import dev.ricardoleal20.chopflow.worker.ChopTask;

/**
 * Minimal Java worker. Demonstrates both registration styles: an annotated handler
 * discovered via {@code scan(this)}, and an imperative lambda.
 *
 * <p>Run against a local broker (gRPC on :8000):
 *
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=dev.ricardoleal20.chopflow.examples.EchoWorker
 * </pre>
 *
 * Then submit work from {@link ProducerExample} (or the CLI / dashboard).
 */
public final class EchoWorker {

  @ChopTask("echo")
  public JsonElement echo(JsonElement payload) {
    JsonObject out = new JsonObject();
    out.addProperty("status", "ok");
    out.add("echo", payload);
    return out;
  }

  public static void main(String[] args) {
    String broker = args.length > 0 ? args[0] : "localhost:8000";

    ChopFlowWorker worker =
        ChopFlowWorker.builder().broker(broker).tags("default").resources("cpu", 1).build();

    EchoWorker instance = new EchoWorker();
    worker.scan(instance); // discovers @ChopTask("echo")
    worker.register("ping", p -> { // explicit lambda registration
          JsonObject out = new JsonObject();
          out.addProperty("status", "ok");
          out.addProperty("pong", System.currentTimeMillis());
          return out;
        });

    System.out.println("Starting Java echo worker against " + broker + " — Ctrl+C to stop.");
    worker.startAndAwait();
  }
}
