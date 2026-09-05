package dev.ricardoleal20.chopflow.client;

import dev.ricardoleal20.chopflow.grpc.Task;
import dev.ricardoleal20.chopflow.grpc.TaskStatus;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.TimeoutException;

/**
 * Handle to an enqueued task. Polls the broker until the task reaches a terminal
 * state ({@code COMPLETED}, {@code FAILED}, {@code DEADLETTERED}, {@code CANCELLED}),
 * the Java analogue of the documented {@code AsyncResult.get()}.
 */
public final class AsyncResult {
  private static final EnumSet<TaskStatus> TERMINAL =
      EnumSet.of(
          TaskStatus.COMPLETED,
          TaskStatus.FAILED,
          TaskStatus.DEADLETTERED,
          TaskStatus.CANCELLED);

  private static final long POLL_INTERVAL_MS = 500;

  private final ChopFlowClient client;
  private final String taskId;

  AsyncResult(ChopFlowClient client, String taskId) {
    this.client = client;
    this.taskId = taskId;
  }

  /** The task id assigned by the broker. */
  public String id() {
    return taskId;
  }

  /** Block indefinitely until the task reaches a terminal state. */
  public Task get() throws InterruptedException {
    while (true) {
      Task t = client.getTask(taskId);
      if (TERMINAL.contains(t.getStatus())) return t;
      Thread.sleep(POLL_INTERVAL_MS);
    }
  }

  /** Block until terminal or {@code timeout} elapses (then throw {@link TimeoutException}). */
  public Task get(Duration timeout) throws InterruptedException, TimeoutException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (true) {
      Task t = client.getTask(taskId);
      if (TERMINAL.contains(t.getStatus())) return t;
      if (System.nanoTime() >= deadlineNanos) {
        throw new TimeoutException("task " + taskId + " did not reach a terminal state within " + timeout);
      }
      Thread.sleep(POLL_INTERVAL_MS);
    }
  }
}
