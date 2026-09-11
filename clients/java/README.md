# ChopFlow Java Client

A Java worker + producer SDK for [ChopFlow](../../README.md), the distributed task
queue in Rust. The broker and all execution logic live in Rust; this SDK lets you
**define and run task handlers in Java** and **enqueue tasks from Java**, speaking
gRPC to the broker over the contract in [`proto/proto/chopflow.proto`](../../proto/proto/chopflow.proto).

> **Build status:** Verified — `mvn clean install` passes (JDK 21 + Maven 3.9.9) and
> the example round-trips end-to-end against the Rust broker (Java producer enqueues
> an echo task → Java worker acks → `AsyncResult.get()` returns `COMPLETED`).

## Requirements

- **JDK 17+**
- **Maven 3.8+** (the repo's managed toolchain ships Maven 3.9.9 via `mise`)

## Build

```bash
cd clients/java
mvn install          # compiles generated stubs + SDK, installs to ~/.m2
```

`mvn install` publishes the artifact to your local Maven repository
(`~/.m2/repository/dev/ricardoleal20/chopflow-java/0.1.0/`), which is the "release"
target for now. Publishing to GitHub Packages / Maven Central is a follow-up.

## Usage

### Worker — define and run handlers in Java

```java
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.ricardoleal20.chopflow.worker.ChopFlowWorker;
import dev.ricardoleal20.chopflow.worker.ChopTask;

class MyApp {
  @ChopTask("resize_image")
  public JsonElement resize(JsonElement payload) {
    // ...your logic, returns JSON result...
    JsonObject out = new JsonObject();
    out.addProperty("status", "ok");
    return out;
  }

  public static void main(String[] args) {
    ChopFlowWorker worker = ChopFlowWorker.builder()
        .broker("localhost:8000")
        .tags("image")
        .resources("cpu", 2)
        .build();
    worker.scan(new MyApp());        // discovers @ChopTask methods
    worker.register("ping", p -> p); // or register lambdas directly
    worker.startAndAwait();          // blocks until Ctrl+C
  }
}
```

The worker registers with the broker, sends heartbeats, polls `FetchTasks`,
dispatches each task to the handler matching `Task.name` (falling back to
`default`/`echo`), and acknowledges success or failure. A thrown exception reports
the task as failed — the broker applies retry/backoff per `max_retries`.

### Producer — enqueue and await results

```java
import java.time.Duration;
import java.util.Map;
import dev.ricardoleal20.chopflow.client.ChopFlowClient;

try (ChopFlowClient client = ChopFlowClient.connect("localhost:8000")) {
  var result = client.enqueue("resize_image")
      .payload(Map.of("width", 128, "height", 128))
      .tags("image")
      .maxRetries(3)
      .priority(5)            // higher = claimed before lower (default 0)
      .enqueue();
  var task = result.get(Duration.ofSeconds(60));
  System.out.println(task.getStatus());  // COMPLETED
  System.out.println(task.getResult());  // JSON result string
}
```

## Examples

Two runnable mains live in `dev.ricardoleal20.chopflow.examples`:

```bash
# Terminal 1: broker (gRPC :8000, HTTP/dashboard :8080)
cargo run -p chopflow_broker -- start

# Terminal 2: Java echo worker
mvn -q exec:java -Dexec.mainClass=dev.ricardoleal20.chopflow.examples.EchoWorker

# Terminal 3: Java producer
mvn -q exec:java -Dexec.mainClass=dev.ricardoleal20.chopflow.examples.ProducerExample
```

(You can also drive it with the Rust worker / CLI / dashboard — they all share the
same gRPC contract and `default` tag.)

## How codegen works

- `proto/proto/chopflow.proto` is the single source of truth. The
  `protobuf-maven-plugin` compiles it (with `java_package =
  dev.ricardoleal20.chopflow.grpc`) straight from `../../proto/proto`.
- The well-known protos (`google/protobuf/timestamp.proto`, `empty.proto`) are
  vendored under `src/main/proto-import/` as an **import-only** path, so `protoc`
  can resolve them without generating duplicate classes — those come from
  `com.google.protobuf:protobuf-java`.
- `protoc` and the `grpc-java` codegen plugin are downloaded automatically via
  `protocArtifact` / `pluginArtifact` (no system `protoc` needed).

## Troubleshooting

- **`protoc` fails to resolve `google/protobuf/timestamp.proto`** — confirm
  `src/main/proto-import/google/protobuf/{timestamp,empty}.proto` are present and
  the `<additionalProtoPathElements>` block in `pom.xml` is intact.
- **`os.detected.classifier` unresolved** — the `os-maven-plugin` extension must
  stay in `<extensions>`; it's what produces the platform classifier for the
  codegen artifacts.
- **Connection refused** — the broker's gRPC port is `--port` (default **8000**).
  The `--http-port` (default 8080) is the dashboard/HTTP API, not gRPC. The Java
  client defaults to `localhost:8000`.

## License

Apache-2.0, same as the rest of ChopFlow.
