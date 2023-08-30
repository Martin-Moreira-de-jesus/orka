use std::collections::HashMap;

use containerd_client::{
    connect,
    services::v1::{
        container::Runtime,
        containers_client::ContainersClient,
        content_client::ContentClient,
        images_client::ImagesClient,
        snapshots::{snapshots_client::SnapshotsClient, MountsRequest, PrepareSnapshotRequest},
        tasks_client::TasksClient,
        Container, CreateContainerRequest, CreateTaskRequest, DeleteContainerRequest,
        GetContainerRequest, GetContainerResponse, GetImageRequest, ListContainersRequest,
        ListContainersResponse, ReadContentRequest,
    },
    with_namespace,
};
use prost_types::Any;
use serde_json::json;
use tokio::process::Command;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::{transport::Channel, Code, Response};

use anyhow::{Error, Result};
use tracing::info;

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

    async fn get_snapshot_client(&self) -> Result<SnapshotsClient<Channel>, ContainerClientError> {
        let channel = self.get_channel().await?;
        Ok(SnapshotsClient::new(channel))
    }

    async fn get_image_client(&self) -> Result<ImagesClient<Channel>, ContainerClientError> {
        let channel = self.get_channel().await?;
        Ok(ImagesClient::new(channel))
    }

    async fn get_content_client(&self) -> Result<ContentClient<Channel>, ContainerClientError> {
        let channel = self.get_channel().await?;
        Ok(ContentClient::new(channel))
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
        info!("{:#?}", workload);

        // TODO: Pull the image

        // 1. Get the chain ID of the image to use
        let mut images_client = self.get_image_client().await?;

        let get_image_request = GetImageRequest {
            name: workload.image.to_owned(),
        };

        // TODO: Get namespace from somewhere
        let image = images_client
            .get(with_namespace!(get_image_request, "default"))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?;

        let image = image.into_inner();
        info!("{:#?}", image);

        // 2. Get the image index manifest from the target descriptor of the image
        // TODO: See https://github.com/opencontainers/image-spec/blob/main/image-index.md
        // TODO: Remove unwraps
        let image_index_manifest_digest = image.image.unwrap().target.unwrap().digest;

        let mut content_client = self.get_content_client().await?;

        let image_index_manifest_read_content_request = ReadContentRequest {
            digest: image_index_manifest_digest,
            ..Default::default()
        };

        // TODO: Get namespace from somewhere
        // TODO: Error management
        let mut image_index_manifest_raw = Vec::<u8>::new();
        let mut image_index_manifest_stream = content_client
            .read(with_namespace!(
                image_index_manifest_read_content_request,
                "default"
            ))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?
            .into_inner();

        while let Some(item) = image_index_manifest_stream.next().await {
            let item = item.map_err(|status| ContainerClientError::GRPCError { status })?;
            image_index_manifest_raw.extend(item.data.iter());
        }

        // TODO: Do a real struct
        let image_index_manifest: serde_json::Value =
            serde_json::from_slice(image_index_manifest_raw.as_slice())
                .map_err(|err| ContainerClientError::Unknown { error: err.into() })?;

        info!("{:?}", image_index_manifest);

        // 3. Extract the correct image manifest for the platform
        let mut image_manifest_digest: Option<String> = None;

        // TODO: Get current platform data dynamically
        // TODO: Remove unwraps
        for manifest in image_index_manifest["manifests"].as_array().unwrap().iter() {
            if manifest["platform"]["architecture"] == "arm64"
                && manifest["platform"]["os"] == "linux"
            {
                info!("{:?}", manifest);
                image_manifest_digest = Some(manifest["digest"].as_str().unwrap().to_owned());

                break;
            }
        }

        // TODO: Remove unwrap
        // TODO: Also check the `mediaType`?
        let image_manifest_digest = image_manifest_digest.unwrap();

        // 4. Get the image manifest
        // TODO: See https://github.com/opencontainers/image-spec/blob/main/manifest.md
        let image_manifest_read_content_request = ReadContentRequest {
            digest: image_manifest_digest,
            ..Default::default()
        };

        // TODO: Get namespace from somewhere
        let mut image_manifest_raw = Vec::<u8>::new();
        let mut image_manifest_stream = content_client
            .read(with_namespace!(
                image_manifest_read_content_request,
                "default"
            ))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?
            .into_inner();

        while let Some(item) = image_manifest_stream.next().await {
            let item = item.map_err(|status| ContainerClientError::GRPCError { status })?;
            image_manifest_raw.extend(item.data.iter());
        }

        // TODO: Do a real struct
        let image_manifest: serde_json::Value =
            serde_json::from_slice(image_manifest_raw.as_slice())
                .map_err(|err| ContainerClientError::Unknown { error: err.into() })?;

        info!("{:?}", image_manifest);

        // 5. Get the configuration manifest digest
        // TODO: Remove unwrap
        // TODO: Also check the `mediaType`?
        let config_manifest_digest = image_manifest["config"]["digest"]
            .as_str()
            .unwrap()
            .to_owned();

        // 6. Get the configuration manifest
        // TODO: See https://github.com/opencontainers/image-spec/blob/main/config.md
        let config_manifest_read_content_request = ReadContentRequest {
            digest: config_manifest_digest,
            ..Default::default()
        };

        // TODO: Get namespace from somewhere
        let mut config_manifest_raw = Vec::<u8>::new();
        let mut config_manifest_stream = content_client
            .read(with_namespace!(
                config_manifest_read_content_request,
                "default"
            ))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?
            .into_inner();

        while let Some(item) = config_manifest_stream.next().await {
            let item = item.map_err(|status| ContainerClientError::GRPCError { status })?;
            config_manifest_raw.extend(item.data.iter());
        }

        // TODO: Do a real struct
        let config_manifest: serde_json::Value =
            serde_json::from_slice(config_manifest_raw.as_slice())
                .map_err(|err| ContainerClientError::Unknown { error: err.into() })?;

        info!("{:?}", config_manifest);

        // 7. Get the diff IDs (See https://github.com/opencontainers/image-spec/blob/3131ecd26bfc92a41f3fbf0d13d4d9063f0f36c2/config.md#layer-diffid)
        // TODO: Remove unwraps
        // TODO: Do a real struct
        let diff_ids: Vec<String> = config_manifest["rootfs"]["diff_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();

        info!("{:#?}", diff_ids);

        // 8. Compute the chain ID (See https://github.com/opencontainers/image-spec/blob/3131ecd26bfc92a41f3fbf0d13d4d9063f0f36c2/config.md#layer-chainid)
        // TODO: What if the digest is not SHA256?
        // TODO: See https://github.com/opencontainers/go-digest
        let mut chain_id = diff_ids[0].clone();

        for diff_id in &diff_ids[1..] {
            let intermediate = format!("{} {}", chain_id, diff_id);
            chain_id = sha256::digest(intermediate);
            chain_id = format!("sha256:{}", chain_id);
        }

        info!("Chain ID: {}", chain_id);

        // 9. Create the rw snapshot
        // TODO: Try to get the default snapshotter from the label `containerd.io/defaults/snapshotter`
        // TODO: Hardcode the default snapshotter for Linux somewhere else
        let snapshot_key = format!("{}-snapshot", workload.name);

        let prepare_snapshot_request = PrepareSnapshotRequest {
            snapshotter: "overlayfs".to_string(),
            key: snapshot_key.clone(),
            parent: chain_id,
            labels: HashMap::new(),
        };

        let mut snapshot_client = self.get_snapshot_client().await?;

        // TODO: Delete this snapshot when the container is deleted
        // TODO: Get namespace from somewhere
        let snapshot = snapshot_client
            .prepare(with_namespace!(prepare_snapshot_request, "default"))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?
            .into_inner();

        info!("{:#?}", snapshot);

        // 10. Generate the container spec
        // TODO: What if we're not running on Linux?
        let mut spec = Self::default_container_spec("default", &workload.name);

        spec["process"]["env"] = config_manifest["config"]["Env"].clone();

        let cwd = config_manifest["config"]["WorkingDir"].clone();

        // TODO: Remove unwrap
        if cwd.is_string() && !cwd.as_str().unwrap().is_empty() {
            spec["process"]["cwd"] = cwd;
        }

        // Generate the process arguments
        let mut process_args: Vec<String> = Vec::new();

        // TODO: Remove unwraps
        let mut entrypoints: Vec<String> = config_manifest["config"]["Entrypoint"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();

        let mut cmds: Vec<String> = config_manifest["config"]["Cmd"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();

        process_args.append(&mut entrypoints);
        process_args.append(&mut cmds);

        spec["process"]["args"] = json!(process_args.as_slice());

        // 11. Create the container
        // TODO: Try to get the default snapshotter from the label `containerd.io/defaults/snapshotter`
        // TODO: Hardcode the default snapshotter for Linux somewhere else
        let runtime = Runtime {
            name: "io.containerd.runc.v2".to_string(),
            ..Default::default()
        };

        // TODO: Put the `type_url` of the `spec` somewhere else, break it down
        // TODO: Remove unwrap
        let container = Container {
            id: workload.name.to_owned(),
            image: workload.image.to_owned(),
            snapshotter: "overlayfs".to_string(),
            snapshot_key: snapshot_key.clone(),
            runtime: Some(runtime),
            spec: Some(Any {
                type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
                value: serde_json::to_vec(&spec).unwrap(),
            }),
            labels: HashMap::from([(
                "io.containerd.image.config.stop-signal".to_string(),
                "SIGTERM".to_string(),
            )]),
            ..Default::default()
        };

        info!("{:#?}", container);

        let create_container_request = CreateContainerRequest {
            container: Some(container),
        };

        let mut container_client = self.get_client().await?;

        // TODO: Get namespace from somewhere
        let create_container_response = container_client
            .create(with_namespace!(create_container_request, "default"))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?;

        info!("{:#?}", create_container_response);

        // 12. Get the snapshot mounts
        // TODO: Hardcode the default snapshotter for Linux somewhere else
        let mounts_request = MountsRequest {
            snapshotter: "overlayfs".to_string(),
            key: snapshot_key,
        };

        // TODO: Get namespace from somewhere
        let mounts = snapshot_client
            .mounts(with_namespace!(mounts_request, "default"))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?
            .into_inner();

        info!("{:#?}", mounts);

        // 13. Create the task
        let create_task_request = CreateTaskRequest {
            container_id: workload.name.to_owned(),
            rootfs: mounts.mounts,
            ..Default::default()
        };

        let mut task_client = self.get_task_client().await?;

        // TODO: Get namespace from somewhere
        let create_task_response = task_client
            .create(with_namespace!(create_task_request, "default"))
            .await
            .map_err(|status| ContainerClientError::GRPCError { status })?;

        info!("{:#?}", create_task_response);

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

    fn default_container_spec(namespace: &str, container_id: &str) -> serde_json::Value {
        let spec = json!({
            "ociVersion": "1.1.0-rc.1",
            "process": {
                "user": {
                    "uid": 0,
                    "gid": 0
                },
                "cwd": "/",
                "capabilities": {
                    "bounding": [
                        "CAP_CHOWN",
                        "CAP_DAC_OVERRIDE",
                        "CAP_FSETID",
                        "CAP_FOWNER",
                        "CAP_MKNOD",
                        "CAP_NET_RAW",
                        "CAP_SETGID",
                        "CAP_SETUID",
                        "CAP_SETFCAP",
                        "CAP_SETPCAP",
                        "CAP_NET_BIND_SERVICE",
                        "CAP_SYS_CHROOT",
                        "CAP_KILL",
                        "CAP_AUDIT_WRITE"
                    ],
                    "effective": [
                        "CAP_CHOWN",
                        "CAP_DAC_OVERRIDE",
                        "CAP_FSETID",
                        "CAP_FOWNER",
                        "CAP_MKNOD",
                        "CAP_NET_RAW",
                        "CAP_SETGID",
                        "CAP_SETUID",
                        "CAP_SETFCAP",
                        "CAP_SETPCAP",
                        "CAP_NET_BIND_SERVICE",
                        "CAP_SYS_CHROOT",
                        "CAP_KILL",
                        "CAP_AUDIT_WRITE"
                    ],
                    "permitted": [
                        "CAP_CHOWN",
                        "CAP_DAC_OVERRIDE",
                        "CAP_FSETID",
                        "CAP_FOWNER",
                        "CAP_MKNOD",
                        "CAP_NET_RAW",
                        "CAP_SETGID",
                        "CAP_SETUID",
                        "CAP_SETFCAP",
                        "CAP_SETPCAP",
                        "CAP_NET_BIND_SERVICE",
                        "CAP_SYS_CHROOT",
                        "CAP_KILL",
                        "CAP_AUDIT_WRITE"
                    ]
                },
                "rlimits": [
                {
                    "type": "RLIMIT_NOFILE",
                    "hard": 1024,
                    "soft": 1024
                }
                ],
                "noNewPrivileges": true
            },
            "root": {
                "path": "rootfs"
            },
            "mounts": [
            {
                "destination": "/proc",
                "type": "proc",
                "source": "proc",
                "options": [
                    "nosuid",
                    "noexec",
                    "nodev"
                ]
            },
            {
                "destination": "/dev",
                "type": "tmpfs",
                "source": "tmpfs",
                "options": [
                    "nosuid",
                    "strictatime",
                    "mode=755",
                    "size=65536k"
                ]
            },
            {
                "destination": "/dev/pts",
                "type": "devpts",
                "source": "devpts",
                "options": [
                    "nosuid",
                    "noexec",
                    "newinstance",
                    "ptmxmode=0666",
                    "mode=0620",
                    "gid=5"
                ]
            },
            {
                "destination": "/dev/shm",
                "type": "tmpfs",
                "source": "shm",
                "options": [
                    "nosuid",
                    "noexec",
                    "nodev",
                    "mode=1777",
                    "size=65536k"
                ]
            },
            {
                "destination": "/dev/mqueue",
                "type": "mqueue",
                "source": "mqueue",
                "options": [
                    "nosuid",
                    "noexec",
                    "nodev"
                ]
            },
            {
                "destination": "/sys",
                "type": "sysfs",
                "source": "sysfs",
                "options": [
                    "nosuid",
                    "noexec",
                    "nodev",
                    "ro"
                ]
            },
            {
                "destination": "/run",
                "type": "tmpfs",
                "source": "tmpfs",
                "options": [
                    "nosuid",
                    "strictatime",
                    "mode=755",
                    "size=65536k"
                ]
            }
            ],
            "linux": {
                "resources": {
                    "devices": [
                    {
                        "allow": false,
                        "access": "rwm"
                    }
                    ]
                },
                "cgroupsPath": format!("/{}/{}", namespace, container_id),
                "namespaces": [
                {
                    "type": "pid"
                },
                {
                    "type": "ipc"
                },
                {
                    "type": "uts"
                },
                {
                    "type": "mount"
                },
                {
                    "type": "network"
                }
                ],
                "maskedPaths": [
                    "/proc/acpi",
                    "/proc/asound",
                    "/proc/kcore",
                    "/proc/keys",
                    "/proc/latency_stats",
                    "/proc/timer_list",
                    "/proc/timer_stats",
                    "/proc/sched_debug",
                    "/sys/firmware",
                    "/proc/scsi"
                ],
                "readonlyPaths": [
                    "/proc/bus",
                    "/proc/fs",
                    "/proc/irq",
                    "/proc/sys",
                    "/proc/sysrq-trigger"
                ]
            }
        });

        spec
    }
}
