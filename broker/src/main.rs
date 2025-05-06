/*!
# ChopFlow Broker

This is the broker executable for ChopFlow, a distributed task queue.

The broker serves as the central coordination point for the system:
- Maintains the task queue
- Dispatches tasks to available workers
- Tracks worker state and capabilities
- Handles task acknowledgments and retries
- Exposes HTTP/gRPC APIs for clients and workers

Future enhancements will include persistent storage, authentication,
metrics reporting, and cluster coordination.
*/

use chopflow_core::dispatcher::{Dispatcher, InMemoryDispatcher, Worker};
use chopflow_core::error::{ChopFlowError, Result};
use chopflow_core::queue::{InMemoryQueue, Queue};
use chopflow_core::task::{Task, TaskStatus};

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Generate code from protobuf definitions
pub mod chopflow {
    // The generated code from the build script will be in OUT_DIR
    tonic::include_proto!("chopflow");
}

use chopflow::{
    AcknowledgeTaskRequest,
    AcknowledgeTaskResponse,
    CancelTaskRequest,
    CancelTaskResponse,
    // RPC request/response types
    EnqueueTaskRequest,
    EnqueueTaskResponse,
    GetQueueStatsRequest,
    GetQueueStatsResponse,
    GetTaskStatusRequest,
    GetTaskStatusResponse,
    ListTasksRequest,
    ListTasksResponse,
    ListWorkersResponse,
    RegisterWorkerRequest,
    RegisterWorkerResponse,
    ResourceAvailability as ProtoResourceAvailability,
    Task as ProtoTask,
    TaskStatus as ProtoTaskStatus,
    Worker as ProtoWorker,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
};

use chopflow::chop_flow_broker_server::{ChopFlowBroker, ChopFlowBrokerServer};

/// ChopFlow Broker - Distributed Task Queue
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the broker
    Start {
        /// Path to config file
        #[arg(long, short, default_value = "config/broker.yml")]
        config: String,

        /// Host to bind to
        #[arg(long, short = 'H', default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on
        #[arg(long, short, default_value = "8000")]
        port: u16,
    },
}

// Conversions between core and proto types
impl From<Task> for ProtoTask {
    fn from(task: Task) -> Self {
        let proto_task = ProtoTask {
            id: task.id.to_string(),
            name: task.name,
            payload: serde_json::to_string(&task.payload).unwrap_or_default(),
            tags: task.tags,
            enqueue_time: Some(prost_types::Timestamp {
                seconds: task.enqueue_time.timestamp(),
                nanos: task.enqueue_time.timestamp_subsec_nanos() as i32,
            }),
            eta: task.eta.map(|eta| prost_types::Timestamp {
                seconds: eta.timestamp(),
                nanos: eta.timestamp_subsec_nanos() as i32,
            }),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            status: task.status as i32,
            resources: task.resources,
        };
        proto_task
    }
}

impl From<ProtoTaskStatus> for TaskStatus {
    fn from(status: ProtoTaskStatus) -> Self {
        match status {
            ProtoTaskStatus::Created => TaskStatus::Created,
            ProtoTaskStatus::Queued => TaskStatus::Queued,
            ProtoTaskStatus::Running => TaskStatus::Running,
            ProtoTaskStatus::Completed => TaskStatus::Completed,
            ProtoTaskStatus::Failed => TaskStatus::Failed,
            ProtoTaskStatus::Deadlettered => TaskStatus::DeadLettered,
        }
    }
}

impl From<Worker> for ProtoWorker {
    fn from(worker: Worker) -> Self {
        ProtoWorker {
            id: worker.id.to_string(),
            address: worker.address,
            tags: worker.tags,
            resources: Some(ProtoResourceAvailability {
                available: worker.resources.available.clone(),
                total: worker.resources.total.clone(),
            }),
            assigned_tasks: worker
                .assigned_tasks
                .iter()
                .map(|id| id.to_string())
                .collect(),
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: worker.last_heartbeat.timestamp(),
                nanos: worker.last_heartbeat.timestamp_subsec_nanos() as i32,
            }),
        }
    }
}

// The gRPC service implementation
struct ChopFlowBrokerService {
    queue: Arc<dyn Queue>,
    dispatcher: Arc<tokio::sync::Mutex<InMemoryDispatcher>>,
    // Stats tracking (would use metrics in a real implementation)
    tasks_completed: Arc<tokio::sync::Mutex<u32>>,
    tasks_failed: Arc<tokio::sync::Mutex<u32>>,
}

#[tonic::async_trait]
impl ChopFlowBroker for ChopFlowBrokerService {
    async fn enqueue_task(
        &self,
        request: Request<EnqueueTaskRequest>,
    ) -> std::result::Result<Response<EnqueueTaskResponse>, Status> {
        let req = request.into_inner();

        // Convert request to Task
        let payload: serde_json::Value = serde_json::from_str(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid payload JSON: {}", e)))?;

        let mut task = Task::new(req.name, payload);

        // Set tags
        task = task.with_tags(req.tags);

        // Set ETA if provided
        if let Some(eta_proto) = req.eta {
            let eta_chrono =
                chrono::DateTime::from_timestamp(eta_proto.seconds, eta_proto.nanos as u32)
                    .ok_or_else(|| Status::invalid_argument("Invalid ETA timestamp"))?;

            task = task.with_eta(eta_chrono);
        }

        // Set max retries
        if req.max_retries > 0 {
            task = task.with_max_retries(req.max_retries);
        }

        // Set resources
        for (resource, amount) in req.resources {
            task = task.with_resource(resource, amount);
        }

        // Enqueue the task
        self.queue
            .enqueue(task.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to enqueue task: {}", e)))?;

        info!("Enqueued task {} (name: {})", task.id, task.name);

        // Return the task ID
        Ok(Response::new(EnqueueTaskResponse {
            task_id: task.id.to_string(),
        }))
    }

    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> std::result::Result<Response<GetTaskStatusResponse>, Status> {
        let req = request.into_inner();

        // Parse task ID
        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        // Get the task
        let task = self
            .queue
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Task not found: {}", task_id)))?;

        // Convert to proto
        let proto_task = ProtoTask::from(task);

        Ok(Response::new(GetTaskStatusResponse {
            task: Some(proto_task),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> std::result::Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();

        // Parse task ID
        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        // Get the task from the queue
        let task_result = self
            .queue
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?;

        // Check if the task exists
        if let Some(mut task) = task_result {
            // Check if the task is in a cancellable state (not completed, failed, or already cancelled)
            if task.status != TaskStatus::Completed
                && task.status != TaskStatus::Failed
                && task.status != TaskStatus::DeadLettered
            {
                // Mark task as cancelled by setting it to Failed status with a special tag
                // In a production system, we might have a dedicated Cancelled status
                task.status = TaskStatus::Failed;
                task.tags.push("cancelled".to_string());

                // Update the task in the queue
                self.queue
                    .update(task.clone())
                    .await
                    .map_err(|e| Status::internal(format!("Failed to update task: {}", e)))?;

                // If the task is currently assigned to a worker, we need to handle that
                if task.status == TaskStatus::Running {
                    let mut dispatcher = self.dispatcher.lock().await;

                    // Manually handle task cancellation for workers
                    // In a production implementation, this would be part of the dispatcher trait
                    let workers_result = dispatcher.list_workers().await;
                    if let Ok(workers) = workers_result {
                        for worker in workers {
                            if worker.assigned_tasks.contains(&task_id) {
                                // In a real impl, we would notify the worker about the cancellation
                                // For now, we'll just unassign the task in the internal state
                                // This is simplified - in production, we'd have a better mechanism
                                if let Err(e) =
                                    dispatcher.ack_task(&worker.id, &task_id, false).await
                                {
                                    warn!("Failed to unassign cancelled task from worker: {}", e);
                                }
                                break;
                            }
                        }
                    }
                }

                info!("Cancelled task {}", task_id);
                Ok(Response::new(CancelTaskResponse { success: true }))
            } else {
                // Task exists but is in a state that cannot be cancelled
                warn!(
                    "Task {} cannot be cancelled due to its status: {:?}",
                    task_id, task.status
                );
                Ok(Response::new(CancelTaskResponse { success: false }))
            }
        } else {
            warn!("Attempted to cancel non-existent task {}", task_id);
            Ok(Response::new(CancelTaskResponse { success: false }))
        }
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> std::result::Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();

        // Create worker
        let mut worker = Worker::new(&req.address);

        // Add tags
        worker = worker.with_tags(req.tags);

        // Add resources
        for (resource, amount) in req.resources {
            worker = worker.with_resource(resource, amount);
        }

        // Register worker with dispatcher
        let mut dispatcher = self.dispatcher.lock().await;

        dispatcher
            .register_worker(worker.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to register worker: {}", e)))?;

        info!("Registered worker {} at {}", worker.id, worker.address);

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: worker.id.to_string(),
        }))
    }

    async fn worker_heartbeat(
        &self,
        request: Request<WorkerHeartbeatRequest>,
    ) -> std::result::Result<Response<WorkerHeartbeatResponse>, Status> {
        let req = request.into_inner();

        // Parse worker ID
        let worker_id = Uuid::parse_str(&req.worker_id)
            .map_err(|_| Status::invalid_argument("Invalid worker ID format"))?;

        // Update heartbeat
        let mut dispatcher = self.dispatcher.lock().await;

        match dispatcher.heartbeat(&worker_id).await {
            Ok(_) => {
                debug!("Received heartbeat from worker {}", worker_id);
                Ok(Response::new(WorkerHeartbeatResponse { success: true }))
            }
            Err(e) => {
                warn!("Failed to update heartbeat for worker {}: {}", worker_id, e);
                Ok(Response::new(WorkerHeartbeatResponse { success: false }))
            }
        }
    }

    async fn acknowledge_task(
        &self,
        request: Request<AcknowledgeTaskRequest>,
    ) -> std::result::Result<Response<AcknowledgeTaskResponse>, Status> {
        let req = request.into_inner();

        // Parse IDs
        let worker_id = Uuid::parse_str(&req.worker_id)
            .map_err(|_| Status::invalid_argument("Invalid worker ID format"))?;

        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        // Acknowledge task
        let mut dispatcher = self.dispatcher.lock().await;

        match dispatcher.ack_task(&worker_id, &task_id, req.success).await {
            Ok(_) => {
                // Update stats
                if req.success {
                    let mut completed = self.tasks_completed.lock().await;
                    *completed += 1;
                    info!("Task {} completed successfully", task_id);
                } else {
                    let mut failed = self.tasks_failed.lock().await;
                    *failed += 1;
                    warn!("Task {} failed", task_id);
                }

                Ok(Response::new(AcknowledgeTaskResponse { success: true }))
            }
            Err(e) => {
                warn!("Failed to acknowledge task {}: {}", task_id, e);
                Ok(Response::new(AcknowledgeTaskResponse { success: false }))
            }
        }
    }

    async fn get_queue_stats(
        &self,
        _: Request<GetQueueStatsRequest>,
    ) -> std::result::Result<Response<GetQueueStatsResponse>, Status> {
        // Get queue length
        let queue_length = self
            .queue
            .len()
            .await
            .map_err(|e| Status::internal(format!("Failed to get queue length: {}", e)))?;

        // Get worker count
        let dispatcher = self.dispatcher.lock().await;
        let workers = dispatcher
            .list_workers()
            .await
            .map_err(|e| Status::internal(format!("Failed to list workers: {}", e)))?;

        let active_workers = workers.iter().filter(|w| w.is_alive()).count() as u32;
        let tasks_processing = workers
            .iter()
            .map(|w| w.assigned_tasks.len())
            .sum::<usize>() as u32;

        // Get completed and failed counts
        let tasks_completed = *self.tasks_completed.lock().await;
        let tasks_failed = *self.tasks_failed.lock().await;

        Ok(Response::new(GetQueueStatsResponse {
            queue_length: queue_length as u32,
            tasks_processing,
            tasks_completed,
            tasks_failed,
            active_workers,
        }))
    }

    async fn list_tasks(
        &self,
        _request: Request<ListTasksRequest>,
    ) -> std::result::Result<Response<ListTasksResponse>, Status> {
        // Not implemented in MVP
        // Would require a way to scan all tasks in the queue
        Ok(Response::new(ListTasksResponse {
            tasks: Vec::new(),
            total_count: 0,
        }))
    }

    async fn list_workers(
        &self,
        _: Request<()>,
    ) -> std::result::Result<Response<ListWorkersResponse>, Status> {
        let dispatcher = self.dispatcher.lock().await;

        let workers = dispatcher
            .list_workers()
            .await
            .map_err(|e| Status::internal(format!("Failed to list workers: {}", e)))?;

        // Convert to proto
        let proto_workers = workers.into_iter().map(ProtoWorker::from).collect();

        Ok(Response::new(ListWorkersResponse {
            workers: proto_workers,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { config, host, port } => {
            info!("Starting ChopFlow broker on {}:{}", host, port);
            start_broker(config, host, port).await?;
        }
    }

    Ok(())
}

async fn start_broker(_config_path: String, host: String, port: u16) -> Result<()> {
    // Create the queue
    let queue = Arc::new(InMemoryQueue::new());

    // Create dispatcher
    let dispatcher = Arc::new(tokio::sync::Mutex::new(InMemoryDispatcher::new(
        queue.clone(),
    )));

    // Start the dispatcher background task
    {
        let mut dispatcher_guard = dispatcher.lock().await;
        dispatcher_guard.start().await?;
    }

    // Stats counters
    let tasks_completed = Arc::new(tokio::sync::Mutex::new(0u32));
    let tasks_failed = Arc::new(tokio::sync::Mutex::new(0u32));

    // Create the gRPC service
    let service = ChopFlowBrokerService {
        queue: queue.clone(),
        dispatcher: dispatcher.clone(),
        tasks_completed,
        tasks_failed,
    };

    // Parse address
    let addr = format!("{}:{}", host, port)
        .parse::<SocketAddr>()
        .map_err(|e| ChopFlowError::Other(e.into()))?;

    // Start gRPC server
    info!("Starting gRPC server on {}", addr);
    let server_future = Server::builder()
        .add_service(ChopFlowBrokerServer::new(service))
        .serve(addr);

    // Wait for server to complete or Ctrl+C
    tokio::select! {
        _ = server_future => {
            info!("gRPC server shut down");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }

    info!("Broker shutdown complete");

    Ok(())
}
