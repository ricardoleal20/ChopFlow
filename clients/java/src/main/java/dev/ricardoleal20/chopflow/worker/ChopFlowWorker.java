package dev.ricardoleal20.chopflow.worker;

import com.google.gson.JsonElement;
import dev.ricardoleal20.chopflow.grpc.AcknowledgeTaskRequest;
import dev.ricardoleal20.chopflow.grpc.ChopFlowBrokerGrpc;
import dev.ricardoleal20.chopflow.grpc.FetchTasksRequest;
import dev.ricardoleal20.chopflow.grpc.FetchTasksResponse;
import dev.ricardoleal20.chopflow.grpc.RegisterWorkerRequest;
import dev.ricardoleal20.chopflow.grpc.ResourceAvailability;
import dev.ricardoleal20.chopflow.grpc.Task;
import dev.ricardoleal20.chopflow.grpc.WorkerHeartbeatRequest;
import dev.ricardoleal20.chopflow.util.Json;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ChopFlow worker. Connects to the Rust broker over gRPC, registers its tags and
 * resources, then polls for tasks, dispatches each to a registered {@link TaskHandler},
 * and acknowledges the result. Mirrors {@code worker/src/main.rs}.
 *
 * <p>Tasks run sequentially on the poll thread (matching the Rust worker today); a
 * future version will dispatch to a bounded pool sized by declared resources.
 *
 * <pre>{@code
 * ChopFlowWorker worker = ChopFlowWorker.builder()
 *     .broker("localhost:8000")
 *     .tags("ml")
 *     .resources("gpu", 1)
 *     .build();
 * worker.register("train", payload -> ...);
 * worker.startAndAwait();   // blocks until Ctrl+C
 * }</pre>
 */
public final class ChopFlowWorker {
  private static final Logger log = LoggerFactory.getLogger(ChopFlowWorker.class);

  private final String broker;
  private final List<String> tags;
  private final Map<String, Integer> resources;
  private final Duration heartbeatInterval;
  private final Duration pollInterval;
  private final int maxTasksPerPoll;

  private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();
  private final ManagedChannel channel;
  private final ChopFlowBrokerGrpc.ChopFlowBrokerBlockingStub stub;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(
          2,
          r -> {
            Thread t = new Thread(r, "chopflow-worker");
            t.setDaemon(true);
            return t;
          });
  private volatile String workerId;

  private ChopFlowWorker(Builder b) {
    this.broker = b.broker;
    this.tags = new ArrayList<>(b.tags);
    this.resources = new LinkedHashMap<>(b.resources);
    this.heartbeatInterval = b.heartbeatInterval;
    this.pollInterval = b.pollInterval;
    this.maxTasksPerPoll = b.maxTasksPerPoll;
    this.channel = ManagedChannelBuilder.forTarget(stripScheme(b.broker)).usePlaintext().build();
    this.stub = ChopFlowBrokerGrpc.newBlockingStub(channel);
    // Built-in echo/default fallbacks so the worker is useful out of the box,
    // matching the Rust worker's registry.
    register("echo", p -> p);
    register("default", p -> p);
  }

  /** Register a handler for a task name. Overrides a previously registered name. */
  public ChopFlowWorker register(String name, TaskHandler handler) {
    handlers.put(name, handler);
    return this;
  }

  /** Register the fallback handler used when no name-specific handler matches. */
  public ChopFlowWorker registerDefault(TaskHandler handler) {
    return register("default", handler);
  }

  /**
   * Scan {@code instance} for methods annotated {@link ChopTask} and register each.
   * Annotated methods must take a single {@link JsonElement} and may return a
   * {@link JsonElement} or any Gson-serializable object.
   */
  public ChopFlowWorker scan(Object instance) {
    for (Method m : instance.getClass().getDeclaredMethods()) {
      ChopTask a = m.getAnnotation(ChopTask.class);
      if (a == null) continue;
      m.setAccessible(true);
      handlers.put(
          a.value(),
          payload -> {
            try {
              Object res = m.invoke(instance, payload);
              return (res instanceof JsonElement je) ? je : Json.parse(Json.stringify(res));
            } catch (InvocationTargetException e) {
              Throwable cause = e.getCause();
              if (cause instanceof Exception ex) throw ex;
              throw new RuntimeException(cause);
            } catch (Exception e) {
              throw e;
            }
          });
    }
    return this;
  }

  /** Connect to the broker and start the heartbeat + poll loops. Idempotent. */
  public void start() {
    if (!running.compareAndSet(false, true)) return;
    this.workerId = connectAndRegister();
    log.info("Worker registered with id {}", workerId);
    long hbMs = heartbeatInterval.toMillis();
    long pollMs = pollInterval.toMillis();
    scheduler.scheduleAtFixedRate(this::heartbeat, hbMs, hbMs, TimeUnit.MILLISECONDS);
    scheduler.scheduleWithFixedDelay(this::pollOnce, 0, pollMs, TimeUnit.MILLISECONDS);
  }

  /** Start and block until the JVM receives a shutdown signal (Ctrl+C). */
  public void startAndAwait() {
    start();
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  stop();
                  latch.countDown();
                }));
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Stop the heartbeat + poll loops and close the channel. */
  public void stop() {
    if (!running.compareAndSet(true, false)) return;
    scheduler.shutdownNow();
    channel.shutdownNow();
    log.info("Worker stopped");
  }

  /** Connect + register, retrying with backoff until the broker is reachable. */
  private String connectAndRegister() {
    Duration backoff = Duration.ofMillis(500);
    while (running.get()) {
      try {
        RegisterWorkerRequest req =
            RegisterWorkerRequest.newBuilder()
                .setAddress("localhost")
                .addAllTags(tags)
                .putAllResources(resources)
                .build();
        return stub.registerWorker(req).getWorkerId();
      } catch (Exception e) {
        log.warn("Could not reach broker at {} ({}). Retrying in {}.", broker, e.getMessage(), backoff);
        try {
          Thread.sleep(backoff.toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return "";
        }
        backoff = Duration.ofMillis(Math.min(backoff.toMillis() * 2, 5_000));
      }
    }
    return "";
  }

  private void heartbeat() {
    try {
      ResourceAvailability ra =
          ResourceAvailability.newBuilder()
              .putAllAvailable(resources)
              .putAllTotal(resources)
              .build();
      stub.workerHeartbeat(
          WorkerHeartbeatRequest.newBuilder().setWorkerId(workerId).setResources(ra).build());
    } catch (Exception e) {
      log.warn("Heartbeat failed: {}", e.getMessage());
    }
  }

  private void pollOnce() {
    try {
      FetchTasksResponse resp =
          stub.fetchTasks(
              FetchTasksRequest.newBuilder()
                  .setWorkerId(workerId)
                  .setMaxTasks(maxTasksPerPoll)
                  .build());
      for (Task task : resp.getTasksList()) {
        executeTask(task);
      }
    } catch (Exception e) {
      log.warn("Fetch failed: {}", e.getMessage());
    }
  }

  private void executeTask(Task task) {
    String id = task.getId();
    String name = task.getName();
    log.info("Executing task {} ({})", id, name);

    TaskHandler h = handlers.get(name);
    if (h == null) h = handlers.get("default");
    if (h == null) {
      ack(id, false, Json.stringify(Map.of("status", "error", "message", "Unknown task type: " + name)));
      log.warn("No handler for task {}", name);
      return;
    }

    JsonElement payload = Json.parse(task.getPayload());
    try {
      JsonElement result = h.handle(payload);
      ack(id, true, Json.stringify(result));
      log.info("Task {} completed", id);
    } catch (Exception e) {
      String msg = e.getMessage() == null ? e.toString() : e.getMessage();
      log.error("Task {} failed: {}", id, msg);
      ack(id, false, Json.stringify(Map.of("status", "error", "message", msg)));
    }
  }

  private void ack(String taskId, boolean success, String resultJson) {
    try {
      stub.acknowledgeTask(
          AcknowledgeTaskRequest.newBuilder()
              .setWorkerId(workerId)
              .setTaskId(taskId)
              .setSuccess(success)
              .setResult(resultJson)
              .build());
    } catch (Exception e) {
      log.warn("Ack failed for task {}: {}", taskId, e.getMessage());
    }
  }

  private static String stripScheme(String target) {
    String t = target == null ? "" : target.trim();
    if (t.startsWith("http://")) return t.substring(7);
    if (t.startsWith("https://")) return t.substring(8); // NB: usePlaintext — no TLS
    return t;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String broker = "localhost:8000";
    private final List<String> tags = new ArrayList<>(List.of("default"));
    private final Map<String, Integer> resources = new LinkedHashMap<>();
    private Duration heartbeatInterval = Duration.ofSeconds(30);
    private Duration pollInterval = Duration.ofSeconds(2);
    private int maxTasksPerPoll = 4;

    public Builder broker(String b) {
      this.broker = b;
      return this;
    }

    public Builder tags(String... t) {
      this.tags.clear();
      this.tags.addAll(Arrays.asList(t));
      return this;
    }

    /** Declare an available resource (e.g. {@code resources("gpu", 1)}). */
    public Builder resources(String name, int amount) {
      this.resources.put(name, amount);
      return this;
    }

    public Builder heartbeatInterval(Duration d) {
      this.heartbeatInterval = d;
      return this;
    }

    public Builder pollInterval(Duration d) {
      this.pollInterval = d;
      return this;
    }

    public Builder maxTasksPerPoll(int n) {
      this.maxTasksPerPoll = n;
      return this;
    }

    public ChopFlowWorker build() {
      if (resources.isEmpty()) resources.put("cpu", 1);
      return new ChopFlowWorker(this);
    }
  }
}
