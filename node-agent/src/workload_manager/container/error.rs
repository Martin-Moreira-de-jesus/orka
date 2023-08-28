use anyhow::Error;
use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum ContainerClientError {
    #[error("Socket {sock_path:?} not found")]
    ContainerdSocketNotFound { sock_path: String },
    #[error("Workload {workload_id:?} is not a container")]
    NotAContainer { workload_id: String },
    #[error("Container {container_id:?} already exists")]
    AlreadyExists { container_id: String },
    #[error("Container {container_id:?} not found")]
    NotFound { container_id: String },
    #[error("Unknown error occured {error:?}")]
    Unknown { error: Error },
    #[error("gRPC with error code {status:?} occured")]
    GRPCError { status: Status },
}
