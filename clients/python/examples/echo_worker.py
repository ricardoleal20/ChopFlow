"""Echo worker example: register an ``echo`` handler and run until Ctrl+C."""

from chopflow import ChopFlowWorker, task


@task("echo")
def echo(payload):
    return {"status": "ok", "echo": payload}


def main() -> None:
    import sys

    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:8000"
    worker = (
        ChopFlowWorker.builder()
        .broker(broker)
        .tags("default")
        .resources("cpu", 1)
        .build()
    )
    # `echo` was registered via the @task decorator above. Also register an
    # explicit `ping` handler to show imperative registration.
    worker.register("ping", lambda p: {"status": "ok", "pong": int(__import__("time").time())})

    print(f"Starting Python echo worker against {broker} — Ctrl+C to stop.")
    worker.start_and_await()


if __name__ == "__main__":
    main()
