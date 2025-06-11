use anyhow::Result;

use iroh_dasl::Node;

#[tokio::main]
async fn main() -> Result<()> {
    let node = Node::start().await?;
    println!("running with node_id: {}", node.node_id());
    node.gateway("127.0.0.1:8001").await?;
    tokio::signal::ctrl_c().await?;
    println!("bye bye");
    Ok(())
}
