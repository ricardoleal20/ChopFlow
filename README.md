# ChopFlow

ChopFlow is a distributed task queue written in Rust with a Python interface, designed for both production applications and research in distributed systems.

## Features

- **Task Queue Management**: Enqueue and dequeue tasks with metadata (tags, ETA)
- **Worker Dispatch**: Assign tasks to worker instances based on tags and resource availability
- **Acknowledgments**: Workers confirm success or failure, with retry policies
- **Resource Tracking**: CPU, GPU, and memory resource allocation and tracking
- **Python Interface**: Familiar Celery-like interface with `@task` decorator and `AsyncResult`
- **Metrics**: Queue length, latency, throughput, and resource utilization metrics
- **Research Focus**: Designed for reproducible experiments and performance comparison

## Components

- **Core**: Rust library with queue, dispatcher, and worker implementations
- **Broker**: Rust executable for the broker service
- **Worker**: Rust executable for task execution
- **CLI**: Command-line interface for interacting with ChopFlow
- **Python Interface**: Python package for easy integration with Python applications
- **Experiment**: Configuration and analysis tools for research experiments

## Getting Started

### Prerequisites

- Rust (2021 edition)
- Python 3.8+
- Cargo

### Building from Source

```bash
# Clone the repository
git clone https://github.com/ricardoleal20/ChopFlow.git
cd ChopFlow

# Build Rust components
cargo build --release

# Install Python interface
cd python_interface
pip install -e .
```

### Basic Usage

Start the broker:

```bash
./target/release/chopflow_broker start --host 127.0.0.1 --port 8000
```

Start a worker:

```bash
./target/release/chopflow_worker start --broker http://localhost:8000 --tags gpu,ml --resources gpu:1,cpu:4
```

Use the CLI to enqueue a task:

```bash
./target/release/chopflow_cli enqueue --task task.json --name training --tags gpu,ml
```

### Python Interface

```python
from chopflow import task, Client

@task(tags=["ml"], resources={"gpu": 1})
def train_model(dataset, hyperparams):
    # Your training code here
    return {"accuracy": 0.95}

# Enqueue for asynchronous execution
result = train_model.delay("imagenet", {"lr": 0.001})

# Get result when ready
output = result.get(timeout=3600)
```

## Research Experiments

ChopFlow is designed to facilitate research in distributed task queues. The `experiment` directory contains tools for running reproducible experiments and analyzing results.

To run an experiment:

```bash
# Configure experiment
nano experiment/configs/exp01_ml_training.yml

# Run experiment (implementation details TBD)
python experiment/run_experiment.py --config experiment/configs/exp01_ml_training.yml

# Analyze results
python experiment/analysis_scripts/analyze_results.py --db experiment/results/exp01.db
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Inspired by systems like Celery, Ray, and Dask