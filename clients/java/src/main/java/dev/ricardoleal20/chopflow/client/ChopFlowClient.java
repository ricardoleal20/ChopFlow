package dev.ricardoleal20.chopflow.client;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import dev.ricardoleal20.chopflow.grpc.CancelTaskRequest;
import dev.ricardoleal20.chopflow.grpc.ChopFlowBrokerGrpc;
import dev.ricardoleal20.chopflow.grpc.CreateScheduleRequest;
import dev.ricardoleal20.chopflow.grpc.DeleteScheduleRequest;
import dev.ricardoleal20.chopflow.grpc.EnqueueTaskRequest;
import dev.ricardoleal20.chopflow.grpc.GetQueueStatsRequest;
import dev.ricardoleal20.chopflow.grpc.GetQueueStatsResponse;
import dev.ricardoleal20.chopflow.grpc.GetTaskStatusRequest;
import dev.ricardoleal20.chopflow.grpc.GetTaskStatusResponse;
import dev.ricardoleal20.chopflow.grpc.ListSchedulesRequest;
import dev.ricardoleal20.chopflow.grpc.ListTasksRequest;
import dev.ricardoleal20.chopflow.grpc.Schedule;
import dev.ricardoleal20.chopflow.grpc.Task;
import dev.ricardoleal20.chopflow.grpc.TaskStatus;
import dev.ricardoleal20.chopflow.grpc.Worker;
import dev.ricardoleal20.chopflow.util.Json;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A blocking gRPC client for the ChopFlow broker. Use it from producers to enqueue
 * tasks, poll for results, and manage schedules. Workers should use
 * {@link dev.ricardoleal20.chopflow.worker.ChopFlowWorker} instead.
 *
 * <pre>{@code
 * try (ChopFlowClient client = ChopFlowClient.connect("localhost:8000")) {
 *   AsyncResult r = client.enqueue("resize_image")
 *       .payload(Map.of("width", 128, "height", 128))
 *       .tags("image")
 *       .maxRetries(3)
 *       .enqueue();
 *   Task done = r.get(Duration.ofSeconds(60));
 *   System.out.println(done.getResult());
 * }
 * }</pre>
 */
public final class ChopFlowClient implements AutoCloseable {
  private final ManagedChannel channel;
  private final ChopFlowBrokerGrpc.ChopFlowBrokerBlockingStub stub;

  private ChopFlowClient(String target) {
    this.channel = ManagedChannelBuilder.forTarget(stripScheme(target)).usePlaintext().build();
    this.stub = ChopFlowBrokerGrpc.newBlockingStub(channel);
  }

  /** Connect to a broker. {@code target} may be {@code host:port} or {@code http://host:port}. */
  public static ChopFlowClient connect(String target) {
    return new ChopFlowClient(target);
  }

  /** Begin building an enqueue request. */
  public EnqueueBuilder enqueue(String name) {
    return new EnqueueBuilder(this, name);
  }

  /** Convenience: enqueue {@code name} with a JSON-serializable payload and return a handle. */
  public AsyncResult enqueue(String name, Object payload) {
    return enqueue(name).payload(payload).enqueue();
  }

  AsyncResult enqueueInternal(EnqueueBuilder b) {
    EnqueueTaskRequest.Builder req =
        EnqueueTaskRequest.newBuilder()
            .setName(b.name)
            .setPayload(b.payloadJson)
            .addAllTags(b.tags)
            .setMaxRetries(b.maxRetries)
            .putAllResources(b.resources)
            .setPriority(b.priority);
    if (b.eta != null) req.setEta(toTimestamp(b.eta));
    String id = stub.enqueueTask(req.build()).getTaskId();
    return new AsyncResult(this, id);
  }

  /** Fetch the current task record (status, retries, result, …). */
  public Task getTask(String id) {
    return stub.getTaskStatus(
            GetTaskStatusRequest.newBuilder().setTaskId(id).build())
        .getTask();
  }

  /** List tasks, optionally filtered by status. */
  public List<Task> listTasks(int limit, int offset, TaskStatus... statuses) {
    ListTasksRequest.Builder req =
        ListTasksRequest.newBuilder().setLimit(limit).setOffset(offset);
    for (TaskStatus s : statuses) req.addFilterStatus(s);
    return stub.listTasks(req.build()).getTasksList();
  }

  /** Cancel a queued or running task. */
  public boolean cancel(String id) {
    return stub.cancelTask(CancelTaskRequest.newBuilder().setTaskId(id).build()).getSuccess();
  }

  /** Snapshot of the queue / worker counts. */
  public QueueStats getStats() {
    GetQueueStatsResponse r = stub.getQueueStats(GetQueueStatsRequest.newBuilder().build());
    return new QueueStats(
        r.getQueueLength(),
        r.getTasksProcessing(),
        r.getTasksCompleted(),
        r.getTasksFailed(),
        r.getActiveWorkers());
  }

  /** List registered workers. */
  public List<Worker> listWorkers() {
    return stub.listWorkers(Empty.getDefaultInstance()).getWorkersList();
  }

  /** Create a schedule; returns the new schedule id. */
  public String createSchedule(Schedule schedule) {
    return stub.createSchedule(
            CreateScheduleRequest.newBuilder().setSchedule(schedule).build())
        .getScheduleId();
  }

  /** List all schedules. */
  public List<Schedule> listSchedules() {
    return stub.listSchedules(ListSchedulesRequest.newBuilder().build()).getSchedulesList();
  }

  /** Delete a schedule by id. */
  public boolean deleteSchedule(String id) {
    return stub.deleteSchedule(DeleteScheduleRequest.newBuilder().setId(id).build()).getSuccess();
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }

  static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private static String stripScheme(String target) {
    String t = target == null ? "" : target.trim();
    if (t.startsWith("http://")) return t.substring(7);
    if (t.startsWith("https://")) return t.substring(8); // NB: usePlaintext — no TLS
    return t;
  }

  /** Fluent builder for enqueue requests. */
  public static final class EnqueueBuilder {
    private final ChopFlowClient client;
    private final String name;
    private String payloadJson = "{}";
    private final List<String> tags = new ArrayList<>();
    private final Map<String, Integer> resources = new LinkedHashMap<>();
    private int maxRetries = 0;
    private int priority = 0;
    private Instant eta = null;

    EnqueueBuilder(ChopFlowClient c, String name) {
      this.client = c;
      this.name = name;
    }

    /**
     * Set the payload. A {@link com.google.gson.JsonElement} or any Gson-serializable
     * object is serialized to JSON; a {@link String} is taken as the raw JSON payload.
     */
    public EnqueueBuilder payload(Object p) {
      this.payloadJson = (p instanceof String s) ? s : Json.stringify(p);
      return this;
    }

    public EnqueueBuilder tags(String... t) {
      this.tags.addAll(Arrays.asList(t));
      return this;
    }

    public EnqueueBuilder resources(String name, int amount) {
      this.resources.put(name, amount);
      return this;
    }

    public EnqueueBuilder maxRetries(int n) {
      this.maxRetries = n;
      return this;
    }

    /**
     * Dispatch priority (higher = claimed before lower). Defaults to {@code 0}, which
     * preserves FIFO ordering within a priority tier.
     */
    public EnqueueBuilder priority(int p) {
      this.priority = p;
      return this;
    }

    /** Schedule the task to become eligible no sooner than {@code eta}. */
    public EnqueueBuilder eta(Instant eta) {
      this.eta = eta;
      return this;
    }

    /** Submit and return an {@link AsyncResult} handle. */
    public AsyncResult enqueue() {
      return client.enqueueInternal(this);
    }
  }

  /** Immutable snapshot of {@code GetQueueStats}. */
  public record QueueStats(
      long queueLength,
      long tasksProcessing,
      long tasksCompleted,
      long tasksFailed,
      long activeWorkers) {}
}
