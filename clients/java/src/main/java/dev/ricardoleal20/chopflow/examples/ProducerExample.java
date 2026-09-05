package dev.ricardoleal20.chopflow.examples;

import dev.ricardoleal20.chopflow.client.AsyncResult;
import dev.ricardoleal20.chopflow.client.ChopFlowClient;
import java.time.Duration;
import java.util.Map;

/**
 * Minimal producer: enqueue an {@code echo} task and wait for the result. Pair with
 * a running {@link EchoWorker} (Rust or Java) subscribed to the {@code default} tag.
 *
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=dev.ricardoleal20.chopflow.examples.ProducerExample
 * </pre>
 */
public final class ProducerExample {
  public static void main(String[] args) throws Exception {
    String broker = args.length > 0 ? args[0] : "localhost:8000";

    try (ChopFlowClient client = ChopFlowClient.connect(broker)) {
      AsyncResult r =
          client.enqueue("echo").payload(Map.of("hello", "from java")).tags("default").enqueue();

      System.out.println("Enqueued task " + r.id());
      var task = r.get(Duration.ofSeconds(60));
      System.out.println("Final status: " + task.getStatus());
      System.out.println("Result:       " + task.getResult());
    }
  }
}
