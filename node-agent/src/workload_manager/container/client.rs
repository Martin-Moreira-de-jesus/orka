use containerd_client::{
    connect,
    services::v1::{
        containers_client::ContainersClient, tasks_client::TasksClient, DeleteContainerRequest,
        GetContainerRequest, GetContainerResponse, ListContainersRequest, ListContainersResponse,
    },
    with_namespace,
};
use tokio::process::Command;
use tonic::Request;
use tonic::{transport::Channel, Code, Response};

use anyhow::{Error, Result};

use super::{error::ContainerClientError, oci};

use orka_proto::node_agent::{Type, Workload};

const NAMESPACE: &str = "default";

pub struct CreateContainerResponse {
    pub container_id: String,
}

pub struct ContainerClient {
    sock_path: String,
}

impl ContainerClient {
    async fn get_channel(&self) -> Result<Channel, ContainerClientError> {
        let channel = connect(self.sock_path.clone()).await.map_err(|_| {
            ContainerClientError::ContainerdSocketNotFound {
                sock_path: self.sock_path.clone(),
            }
        })?;
        Ok(channel)
    }

    async fn get_task_client(&self) -> Result<TasksClient<Channel>, ContainerClientError> {
        let channel = self.get_channel().await?;
        Ok(TasksClient::new(channel))
    }

    async fn get_client(&self) -> Result<ContainersClient<Channel>, ContainerClientError> {
        let channel = self.get_channel().await?;
        Ok(ContainersClient::new(channel))
    }

    pub async fn new(sock_path: &str) -> Result<Self> {
        let _ = connect(sock_path.clone()).await.map_err(|_| {
            ContainerClientError::ContainerdSocketNotFound {
                sock_path: sock_path.to_string(),
            }
        })?;

        Ok(Self {
            sock_path: sock_path.to_string(),
        })
    }

    pub async fn list(
        &mut self,
        filters: Vec<String>,
    ) -> Result<Response<ListContainersResponse>, ContainerClientError> {
        let request = ListContainersRequest { filters };

        let mut client = self.get_client().await?;

        let response = client
            .list(request)
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?;

        Ok(response)
    }

    pub async fn info(
        &mut self,
        container_id: &str,
    ) -> Result<Response<GetContainerResponse>, ContainerClientError> {
        let request = GetContainerRequest {
            id: container_id.to_string(),
        };

        let request = with_namespace!(request, NAMESPACE);

        let mut client = self.get_client().await?;

        let response = client.get(request).await.map_err(|status| {
            if status.code() == tonic::Code::NotFound {
                return ContainerClientError::NotFound {
                    container_id: container_id.to_string(),
                };
            }

            ContainerClientError::GRPCError { status }
        })?;

        Ok(response)
    }

    pub async fn create(
        &mut self,
        workload: &Workload,
    ) -> Result<Response<CreateContainerResponse>, ContainerClientError> {
        if workload.r#type != Type::Container as i32 {
            return Err(ContainerClientError::NotAContainer {
                workload_id: workload.name.to_string(),
            });
        }

        match self.info(&workload.name).await {
            Ok(_) => {
                return Err(ContainerClientError::AlreadyExists {
                    container_id: workload.name.to_string(),
                })
            }
            Err(ContainerClientError::NotFound { container_id: _ }) => {}
            Err(error) => return Err(error),
        }

        // TODO - use containerd library to create container instead of ctr
        let command = Command::new("ctr")
            .arg("run")
            .arg("--detach")
            .arg(&workload.image)
            .arg(&workload.name)
            .output()
            .await
            .map_err(|error| ContainerClientError::Unknown {
                error: error.into(),
            })?;

        if !command.status.success() {
            return Err(ContainerClientError::Unknown {
                error: Error::msg(String::from_utf8(command.stderr).unwrap_or_default()),
            });
        }

        Ok(Response::new(CreateContainerResponse {
            container_id: workload.name.to_string(),
        }))
    }

    pub async fn delete(&mut self, id: &str) -> Result<Response<()>> {
        let request = DeleteContainerRequest { id: id.to_string() };

        let mut client = self.get_client().await?;

        let response = client.delete(request).await.map_err(|status| {
            if status.code() == Code::NotFound {
                return ContainerClientError::NotFound {
                    container_id: id.to_string(),
                };
            }

            ContainerClientError::GRPCError { status }
        })?;

        Ok(response)
    }
}
