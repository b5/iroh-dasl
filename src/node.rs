use anyhow::Result;
use iroh::{Endpoint, NodeId, protocol::Router};
use iroh_blobs::{ALPN as BLOBS_ALPN, net_protocol::Blobs};
use sha2::Digest;
use tokio::task::JoinHandle;

use crate::{
    dasl::{DaslCodec, ShaMap, blake3_to_cid, sha2_to_cid},
    echo::{ALPN, Echo},
    gateway::server,
};

pub struct Node {
    shas: ShaMap,
    blobs: iroh_blobs::rpc::client::blobs::MemClient,
    router: Router,
}

impl Node {
    pub async fn start() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        let node = Self::spawn_router(endpoint).await?;
        Ok(node)
    }

    async fn spawn_router(endpoint: Endpoint) -> Result<Self> {
        let home_dir = dirs_next::data_dir().unwrap();
        let blobs_dir = home_dir.join("iroh_dasl/blobs");
        tokio::fs::create_dir_all(&blobs_dir).await?;

        let shas = ShaMap::new();
        let blobs = Blobs::persistent(blobs_dir).await?.build(&endpoint);
        let router = Router::builder(endpoint)
            .accept(ALPN, Echo)
            .accept(BLOBS_ALPN, blobs.clone())
            .spawn();

        let node = Self {
            shas,
            blobs: blobs.client().clone(),
            router,
        };

        node.add_test_data().await?;

        Ok(node)
    }

    pub async fn gateway(&self, serve_addr: &str) -> Result<JoinHandle<()>> {
        let hashes = self.shas.clone();
        let endpoint = self.router.endpoint().clone();
        let blobs_client = self.blobs.clone();

        let serve_addr = serve_addr.to_string();
        let handle = tokio::spawn(async move {
            server::run(endpoint, hashes, blobs_client, &serve_addr)
                .await
                .expect("gateway failed");
        });

        Ok(handle)
    }

    async fn add_test_data(&self) -> Result<()> {
        let data = "hello world";
        let res = self.blobs.add_bytes(data).await?;
        let sha2_hash: [u8; 32] = sha2::Sha256::digest(data).into();
        let b3cid = blake3_to_cid(res.hash, DaslCodec::Raw);
        let sha2cid = sha2_to_cid(&sha2_hash, DaslCodec::Raw);
        self.shas.set(sha2_hash, res.hash);

        println!(
            "Added test data with hash: {}\nBLAKE3 CID: {}\nSHA2 CID: {}",
            res.hash, b3cid, sha2cid
        );

        Ok(())
    }

    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }
}

pub type N0de = Node;

impl iroh_n0des::N0de for Node {
    async fn spawn(endpoint: iroh::Endpoint) -> anyhow::Result<Self> {
        let node = Self::spawn_router(endpoint).await?;
        node.gateway("0.0.0.0:80").await?;
        Ok(node)
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
