use tonic::{Request, Response, Status, Result};
use tokio_stream::wrappers::ReceiverStream;
use orka_proto::node_agent::{Empty, Workload, workload_service_server::WorkloadService, WorkloadSignal, WorkloadStatus};

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
        todo!()
    }

    async fn signal(&self, request: Request<WorkloadSignal>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}