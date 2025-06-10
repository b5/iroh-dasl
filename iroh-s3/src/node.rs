use std::sync::Arc;

use anyhow::Result;
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{net_protocol::Blobs, ALPN as BLOBS_ALPN};
use iroh_docs::{protocol::Docs, ALPN as DOCS_ALPN};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};
use n0_future::{stream, BufferedStreamExt, StreamExt};

use crate::bucket::{Bucket, Config};

#[derive(Debug)]
pub struct Node {
    iroh: Iroh,
    buckets: Vec<Bucket>,
}

impl Node {
    pub async fn new(bucket_configs: Vec<Config>) -> Result<Node> {
        let iroh = Iroh::new().await?;

        let buckets = stream::iter(bucket_configs)
            .map(|cfg| async { Bucket::new(&iroh, cfg).await.unwrap() })
            .buffered_unordered(10)
            .collect::<Vec<_>>()
            .await;
        // let mut buckets = Vec::new();
        // for config in bucket_configs {
        //     let bucket = Bucket::new(router.clone(), config).await?;
        //     buckets.push(bucket);
        // }
        let node = Node { iroh, buckets };
        Ok(node)
    }

    pub fn iroh(&self) -> &Iroh {
        &self.iroh
    }

    pub fn list_buckets(&self) -> Vec<&Bucket> {
        self.buckets.iter().collect()
    }

    pub async fn create_bucket(&self, _name: &str) -> Result<Bucket> {
        todo!();
        // let config = Config {
        //     name: name.to_string(),
        //     region: s3::region::Region::UsEast1,
        //     credentials: s3::creds::Credentials::default()?,
        //     ticket: self.router.docs().create_ticket().await?,
        // };
        // let bucket = Bucket::new(self.router.clone(), config).await?;
        // Ok(bucket)
    }

    pub fn get_bucket(&self, name: &str) -> Option<&Bucket> {
        self.buckets.iter().find(|b| b.config().name == name)
    }
}

#[derive(Debug, Clone)]
pub struct Iroh(Arc<IrohInner>);

#[derive(Debug)]
struct IrohInner {
    blobs: iroh_blobs::rpc::client::blobs::MemClient,
    gossip: iroh_gossip::rpc::client::MemClient,
    docs: iroh_docs::rpc::client::docs::MemClient,
    router: Router,
}

impl Iroh {
    async fn new() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;

        let builder = Router::builder(endpoint);

        // build the blobs protocol
        let blobs = Blobs::memory().build(builder.endpoint());
        let gossip = Gossip::builder().spawn(builder.endpoint().clone()).await?;
        let docs = Docs::memory().spawn(&blobs, &gossip).await?;

        // setup router
        let router = builder
            .accept(BLOBS_ALPN, blobs.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(DOCS_ALPN, docs.clone())
            .spawn();

        let inner = IrohInner {
            blobs: blobs.client().clone(),
            gossip: gossip.client().clone(),
            docs: docs.client().clone(),
            router,
        };

        Ok(Self(Arc::new(inner)))
    }

    pub fn blobs(&self) -> &iroh_blobs::rpc::client::blobs::MemClient {
        &self.0.blobs
    }

    pub fn gossip(&self) -> &iroh_gossip::rpc::client::MemClient {
        &self.0.gossip
    }

    pub fn docs(&self) -> &iroh_docs::rpc::client::docs::MemClient {
        &self.0.docs
    }
}
