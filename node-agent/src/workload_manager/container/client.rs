use std::fs::{self, File};

use containerd_client::{
    connect,
    services::v1::{
        container::Runtime, containers_client::ContainersClient, tasks_client::TasksClient,
        Container, CreateContainerRequest, CreateContainerResponse, CreateTaskRequest,
        DeleteContainerRequest, ListContainersRequest, ListContainersResponse, StartRequest,
    },
    with_namespace,
};
use tonic::Request;
use tonic::{transport::Channel, Code, Response};

use anyhow::Result;

use super::{error::ContainerClientError, oci};

use orka_proto::node_agent::Workload;

const RUNTIME: &str = "io.containerd.runc.v2";
const NAMESPACE: &str = "default";

pub struct ContainerClient {
    sock_path: String,
}

impl ContainerClient {
    async fn get_channel(&self) -> Result<Channel> {
        let channel = connect(self.sock_path.clone()).await.map_err(|_| {
            ContainerClientError::ContainerdSocketNotFound {
                sock_path: self.sock_path.clone(),
            }
        })?;
        Ok(channel)
    }

    async fn get_task_client(&self) -> Result<TasksClient<Channel>> {
        let channel = self.get_channel().await?;
        Ok(TasksClient::new(channel))
    }

    async fn get_client(&self) -> Result<ContainersClient<Channel>> {
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

    pub async fn list(&mut self, filters: Vec<String>) -> Result<Response<ListContainersResponse>> {
        let request = ListContainersRequest { filters };

        let mut client = self.get_client().await?;

        let response = client
            .list(request)
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?;

        Ok(response)
    }

    pub async fn create(
        &mut self,
        workload: &Workload,
    ) -> Result<Response<CreateContainerResponse>> {
        let container = Container {
            id: workload.name.to_string(),
            image: workload.image.to_string(),
            runtime: Some(Runtime {
                name: RUNTIME.to_string(),
                options: None,
            }),
            spec: Some(oci::default_spec()),
            ..Default::default()
        };

        let req = CreateContainerRequest {
            container: Some(container),
        };
        let req = with_namespace!(req, NAMESPACE);

        let mut client = self.get_client().await?;

        let response = client.create(req).await.map_err(|status| {
            if status.code() == Code::AlreadyExists {
                return ContainerClientError::AlreadyExists {
                    container_id: workload.name.to_string(),
                };
            }

            ContainerClientError::GRPCError { status }
        })?;

        let mut client = self.get_task_client().await?;

        // create detached task
        let req = CreateTaskRequest {
            container_id: workload.name.to_string(),
            terminal: false,
            ..Default::default()
        };
        let req = with_namespace!(req, NAMESPACE);

        let _ = client.create(req).await?;

        let req = StartRequest {
            container_id: workload.name.to_string(),
            ..Default::default()
        };
        let req = with_namespace!(req, NAMESPACE);

        let _ = client.start(req).await?;

        Ok(response)
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
