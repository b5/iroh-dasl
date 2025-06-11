use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use iroh_dasl::Node;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("gateway=debug".parse().unwrap());

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let node = Node::start().await?;
    println!("running with node_id: {}", node.node_id());
    node.gateway("127.0.0.1:8001").await?;
    tokio::signal::ctrl_c().await?;
    println!("bye bye");
    Ok(())
}
