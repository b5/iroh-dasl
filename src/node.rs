use anyhow::Result;
use iroh::{Endpoint, NodeId, protocol::Router};
use tokio::task::JoinHandle;

use crate::{
    echo::{ALPN, Echo},
    gateway::server,
};

pub struct Node {
    router: Router,
}

impl Node {
    pub async fn start() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        let node = Self::spawn_router(endpoint).await?;
        Ok(node)
    }

    pub async fn gateway(&self, serve_addr: &str) -> Result<JoinHandle<()>> {
        let addr = self.router.endpoint().node_addr().await?;
        let serve_addr = serve_addr.to_string();
        let handle = tokio::spawn(async move {
            server::run(addr, &serve_addr)
                .await
                .expect("gateway failed");
        });

        Ok(handle)
    }

    async fn spawn_router(endpoint: Endpoint) -> Result<Self> {
        let router = Router::builder(endpoint).accept(ALPN, Echo).spawn();
        Ok(Self { router })
    }

    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }
}

pub type N0de = Node;

impl iroh_n0des::N0de for Node {
    async fn spawn(endpoint: iroh::Endpoint) -> anyhow::Result<Self> {
        let node = Self::spawn_router(endpoint).await?;
        node.gateway("127.0.0.1:80").await?;
        Ok(node)
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
