use tonic::{Request, Response, Status, Result};
use tokio_stream::wrappers::ReceiverStream;
use orka_proto::node_agent::{Empty, Workload, workload_service_server::WorkloadService, WorkloadSignal, WorkloadStatus};
use crate::workload_manager::container::client::ContainerClient;

pub struct WorkloadSvc {}

impl WorkloadSvc {
    /// Create a new `WorkloadService` gRPC service manager.
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl WorkloadService for WorkloadSvc {
    type CreateStream = ReceiverStream<Result<WorkloadStatus>>;

    async fn create(&self, request: Request<Workload>) -> Result<Response<Self::CreateStream>, Status> {
        let workload = request.into_inner();
        let mut client = match ContainerClient::new("/var/run/containerd/containerd.sock").await {
            Ok(x) => x,
            Err(e) => return Err(Status::new(
                tonic::Code::Internal,
                format!("Failed to create container client{:?}", e)
            )),
        };
        let container = match client.create(&workload).await {
            Ok(x) => x,
            Err(e) => return Err(Status::new(
                tonic::Code::Internal,
                format!("Failed to create container: {:?}", e)
            )),
        }.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                let info = match client.info(&container.container_id).await {
                    Ok(x) => x,
                    Err(e) => return Status::new(
                        tonic::Code::Internal,
                        format!("Failed to get container info: {:?}", e)
                    ),
                }.into_inner().container.unwrap();

                let workload_status = WorkloadStatus {
                    name: info.id,
                    status: 1,
                    resource: None,
                    message: "".to_string(),
                };

                match tx.send(Ok(workload_status)).await {
                    Ok(x) => x,
                    Err(e) => return Status::new(
                        tonic::Code::Internal,
                        format!("Failed to send workload status: {:?}", e)
                    ),
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn signal(&self, _: Request<WorkloadSignal>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }
}