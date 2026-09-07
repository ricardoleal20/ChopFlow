"""Producer example: enqueue an echo task and print the result."""

from chopflow import ChopFlowClient


def main() -> None:
    broker = "localhost:8000"
    with ChopFlowClient.connect(broker) as client:
        result = (
            client.enqueue("echo")
            .payload({"hello": "from python"})
            .tags("default")
            .enqueue()
        )
        print(f"Enqueued task {result.id}")
        task = result.get(timeout=60)
        print(f"Final status: {task.status_name}")
        print(f"Result:       {task.result}")


if __name__ == "__main__":
    main()
