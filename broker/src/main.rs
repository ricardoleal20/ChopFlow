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
use chopflow_core::error::{Result};
use chopflow_core::queue::{InMemoryQueue, Queue};
use chopflow_core::resources::ResourceAvailability;
use chopflow_core::task::{Task, TaskStatus};

use clap::{Parser, Subcommand};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;
use std::time::Duration;
use tokio::time::sleep;

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

// Resource matching helper
struct ResourceMatcher {
    required: std::collections::HashMap<String, u32>,
    available: std::collections::HashMap<String, u32>,
}

impl ResourceMatcher {
    fn new(required: std::collections::HashMap<String, u32>, available: std::collections::HashMap<String, u32>) -> Self {
        Self { required, available }
    }

    fn can_fulfill(&self) -> bool {
        for (resource, amount) in &self.required {
            let available = self.available.get(resource).unwrap_or(&0);
            if available < amount {
                return false;
            }
        }
        true
    }
}

// The gRPC service implementation
#[derive(Clone)]
struct ChopFlowBrokerService {
    queue: Arc<dyn Queue>,
    dispatcher: Arc<tokio::sync::Mutex<InMemoryDispatcher>>,
    // * NOTE (WIP):
    // Stats tracking (would use metrics in the final system)
    tasks_completed: Arc<tokio::sync::Mutex<u32>>,
    tasks_failed: Arc<tokio::sync::Mutex<u32>>,
}

impl ChopFlowBrokerService {
    // Helper method to find available worker for task
    async fn find_available_worker(&self, task: &Task) -> Option<Worker> {
        let dispatcher = self.dispatcher.lock().await;
        let workers = dispatcher.list_workers().await.unwrap_or_default();

        for worker in workers {
            // Check if worker has required resources
            let matcher = ResourceMatcher::new(
                task.resources.clone(),
                worker.resources.available.clone(),
            );

            if matcher.can_fulfill() {
                return Some(worker.clone());
            }
        }

        None
    }

    // Helper method to assign task to worker
    async fn assign_task_to_worker(&self, task: &Task, worker: &Worker) -> Result<()> {
        let mut dispatcher = self.dispatcher.lock().await;
        
        // Update task status
        let mut task = task.clone();
        task.status = TaskStatus::Running;
        
        // Update task in queue
        self.queue.update(task.clone()).await?;
        
        // Assign task to worker
        dispatcher.ack_task(&worker.id, &task.id, true).await?;
        
        info!(
            "Assigned task {} to worker {} (resources: {:?})",
            task.id, worker.id, task.resources
        );
        
        Ok(())
    }

    // Helper method to handle task timeouts
    async fn handle_task_timeouts(&self) {
        loop {
            sleep(Duration::from_secs(30)).await; // Check every 30 seconds
            
            let mut dispatcher = self.dispatcher.lock().await;
            let workers = dispatcher.list_workers().await.unwrap_or_default();
            
            for worker in workers {
                for task_id in &worker.assigned_tasks {
                    if let Ok(Some(task)) = self.queue.get(&task_id).await {
                        // Check if task has been running for too long (e.g., 1 hour)
                        if task.status == TaskStatus::Running {
                            let running_time = chrono::Utc::now() - task.enqueue_time;
                            if running_time > chrono::Duration::hours(1) {
                                warn!(
                                    "Task {} has been running for too long ({}), marking as failed",
                                    task_id, running_time
                                );
                                
                                // Mark task as failed
                                let mut task = task.clone();
                                task.status = TaskStatus::Failed;
                                
                                if let Err(e) = self.queue.update(task).await {
                                    warn!("Failed to update task status: {}", e);
                                }
                                
                                // Release worker resources
                                if let Err(e) = dispatcher.ack_task(&worker.id, task_id, false).await {
                                    warn!("Failed to release task resources: {}", e);
                                }
                                
                                // Update stats
                                let mut failed = self.tasks_failed.lock().await;
                                *failed += 1;
                            }
                        }
                    }
                }
            }
        }
    }
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

        // Try to find available worker
        if let Some(worker) = self.find_available_worker(&task).await {
            // Assign task to worker
            self.assign_task_to_worker(&task, &worker)
                .await
                .map_err(|e| Status::internal(format!("Failed to assign task: {}", e)))?;
        } else {
            // No worker available, enqueue task
            self.queue
                .enqueue(task.clone())
                .await
                .map_err(|e| Status::internal(format!("Failed to enqueue task: {}", e)))?;

            info!(
                "No worker available for task {} (resources: {:?}), enqueued",
                task.id, task.resources
            );
        }

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
                    let workers = dispatcher.list_workers().await.unwrap_or_default();
                    for worker in workers {
                        if worker.assigned_tasks.contains(&task_id) {
                            // * NOTE (WIP):
                            // In the final system, we would notify the worker about the cancellation
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
        
        // Validate resources
        if req.resources.is_empty() {
            return Err(Status::invalid_argument("Worker must specify at least one resource"));
        }

        // Create worker with resources
        let worker = Worker {
            id: Uuid::new_v4(),
            address: req.address,
            tags: req.tags,
            resources: ResourceAvailability {
                available: req.resources.clone(),
                total: req.resources,
            },
            assigned_tasks: Vec::new(),
            last_heartbeat: chrono::Utc::now(),
        };

        // Register worker
        let mut dispatcher = self.dispatcher.lock().await;
        dispatcher.register_worker(worker.clone()).await
            .map_err(|e| Status::internal(format!("Failed to register worker: {}", e)))?;

        info!(
            "Registered worker {} at {} with resources: {:?}",
            worker.id, worker.address, worker.resources
        );

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

        // Get task
        let mut task = self.queue
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Task not found: {}", task_id)))?;

        // Update task status
        if req.success {
            task.status = TaskStatus::Completed;
            let mut completed = self.tasks_completed.lock().await;
            *completed += 1;
            info!("Task {} completed successfully by worker {}", task_id, worker_id);
        } else {
            task.status = TaskStatus::Failed;
            let mut failed = self.tasks_failed.lock().await;
            *failed += 1;
            warn!("Task {} failed: {}", task_id, req.result);
        }

        // Update task in queue
        self.queue
            .update(task)
            .await
            .map_err(|e| Status::internal(format!("Failed to update task: {}", e)))?;

        // Release worker resources
        let mut dispatcher = self.dispatcher.lock().await;
        dispatcher.ack_task(&worker_id, &task_id, false).await
            .map_err(|e| Status::internal(format!("Failed to release task: {}", e)))?;

        Ok(Response::new(AcknowledgeTaskResponse { success: true }))
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
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
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

async fn start_broker(_config_path: String, host: String, port: u16) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", host, port).parse()?;
    
    // Create queue
    let queue = Arc::new(InMemoryQueue::new());
    
    // Create service
    let service = ChopFlowBrokerService {
        queue: queue.clone(),
        dispatcher: Arc::new(tokio::sync::Mutex::new(InMemoryDispatcher::new(queue))),
        tasks_completed: Arc::new(tokio::sync::Mutex::new(0)),
        tasks_failed: Arc::new(tokio::sync::Mutex::new(0)),
    };
    
    // Start timeout handler
    let service_clone = service.clone();
    tokio::spawn(async move {
        service_clone.handle_task_timeouts().await;
    });
    
    info!("Starting ChopFlow broker on {}", addr);
    
    // Start server
    Server::builder()
        .add_service(ChopFlowBrokerServer::new(service))
        .serve(addr)
        .await?;
    
    Ok(())
}
