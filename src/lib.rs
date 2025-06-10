use anyhow::Result;
use iroh::{Endpoint, NodeId, protocol::Router};

use echo::{ALPN, Echo};

pub mod echo;

pub struct Node {
    router: Router,
}

impl Node {
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
        Self::spawn_router(endpoint).await
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
