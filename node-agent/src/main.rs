mod args;

use args::CliArguments;
use clap::Parser;
use orka_proto::scheduler_agent::lifecycle_service_client::LifecycleServiceClient;
use tracing::{event, Level};
use tracing_log::AsTrace;
use uuid::Uuid;
use orka_proto::scheduler_agent::ConnectionRequest;

#[tokio::main]
async fn main() {
    let args = CliArguments::parse();

    tracing_subscriber::fmt()
        .with_max_level(args.verbose.log_level_filter().as_trace())
        .init();

    event!(
        Level::INFO,
        app_name = env!("CARGO_PKG_NAME"),
        app_version = env!("CARGO_PKG_VERSION"),
        "Starting",
    );

    event!(Level::INFO, "Arguments: {:?}", args);

    let mut lifecycle_client = LifecycleServiceClient::connect(format!(
        "http://{}:{}",
        args.scheduler_address, args.scheduler_port
    ))
    .await
    .expect("Failed to create lifecycle client");

    lifecycle_client.join_cluster(ConnectionRequest {
        id: Uuid::new_v4().to_string(),
    }).await.expect("Failed to join cluster");

    event!(Level::INFO, "Joined cluster");
}
