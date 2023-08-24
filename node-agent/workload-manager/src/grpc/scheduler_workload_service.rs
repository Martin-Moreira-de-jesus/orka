use tonic::{Request, Response, Status, Result};
use tokio_stream::wrappers::ReceiverStream;
use orka_proto::node_agent::{Empty, Workload, workload_service_server::WorkloadService, WorkloadSignal, WorkloadStatus};
use crate::container::client::ContainerClient;

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
        // let image = request.into_inner().image;
        // let mut client = ContainerClient::new("/var/run/containerd/containerd.sock").await.unwrap();
        // let container = client.create(&image).await.unwrap().into_inner().container.unwrap();
        let status = WorkloadStatus {
            // name: container.id,
            name: "test".to_string(),
            status: 1,
            resource: None,
            message: "message".to_string(),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                tx.send(Ok(status.clone())).await.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn signal(&self, _: Request<WorkloadSignal>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }
}