use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use iroh::{NodeAddr, PublicKey};
use iroh_blobs::util::SetTagOption;
use iroh_blobs::Hash;
use iroh_docs::{AuthorId, DocTicket};
use n0_future::{stream::Stream, StreamExt};
use s3::creds::Credentials;
use s3::serde_types::ListBucketResult;
use s3::{Bucket as BackingBucket, Region};
use s3s::dto::Range as s3sRange;
use s3s::stream::{ByteStream, RemainingLength};
use s3s::StdError;
use tracing::warn;

use crate::node::Iroh;

/// Bucket configuration
#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub region: Region,
    pub credentials: Credentials,
    pub ticket: iroh_docs::DocTicket,
}

#[derive(Debug)]
pub struct Bucket {
    config: Config,
    proxy: BucketProxy,
    backing: Box<BackingBucket>,
}

impl Bucket {
    pub async fn new(node: &Iroh, config: Config) -> Result<Bucket> {
        let backing = BackingBucket::new(
            &config.name,
            config.region.clone(),
            config.credentials.clone(),
        )?;

        let iroh = BucketProxy::open(node.clone(), config.ticket.clone()).await?;

        let bucket = Bucket {
            config,
            proxy: iroh,
            backing,
        };
        Ok(bucket)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn list_objects(&self) -> Result<Vec<ListBucketResult>> {
        let res = self
            .backing
            .list("/".to_string(), Some("/".to_string()))
            .await?;
        Ok(res)
    }

    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        // write the object to iroh
        let hash = self
            .proxy
            .put_blob(key.to_owned(), data.to_owned().clone())
            .await?;

        println!("put_object: key={}, hash={}", key, hash);

        let res = self.backing.put_object(key, data).await?;
        println!("put_object: res={:?}", res);
        Ok(())
    }

    pub async fn get_object<S: AsRef<str>>(
        &self,
        key: S,
        range: impl Into<Range>,
    ) -> Result<ObjectReader> {
        let key = key.as_ref().to_string();
        let range = range.into();
        match range.into() {
            Range::Int { first, last } => {
                return Err(anyhow!(
                    "int range {}-{} not supported",
                    first,
                    last.unwrap_or(0)
                ));
            }
            Range::Suffix { length } => {
                return Err(anyhow!("suffix range {} not supported", length));
            }
            Range::All => {}
        }

        let fake_key = "nope";
        if let Ok(res) = self.proxy.get_blob(fake_key).await {
            return Ok(ObjectReader::Iroh { reader: res });
        }

        let (header, status) = self.backing.head_object(&key).await?;
        // dbg!(&header, status);

        let size = header.content_length;

        let (reader, mut writer) = tokio::io::duplex(1024);
        let backing = self.backing.clone();
        tokio::task::spawn(async move {
            let res = backing.get_object_to_writer(&key, &mut writer).await;
            if let Err(err) = res {
                warn!("failed to get object: {}: {:?}", key, err);
            }
        });
        let stream_reader = tokio_util::io::ReaderStream::new(reader);

        Ok(ObjectReader::S3 {
            reader: stream_reader,
            status_code: status,
            size,
        })
    }

    pub async fn delete_object<S: AsRef<str>>(&self, key: S) -> Result<()> {
        let res = self.backing.delete_object(key).await?;
        println!("delete_object: res={:?}", res);
        Ok(())
    }
}

pub enum ObjectReader {
    S3 {
        reader: tokio_util::io::ReaderStream<tokio::io::DuplexStream>,
        status_code: u16,
        size: Option<i64>,
    },
    Iroh {
        reader: iroh_blobs::rpc::client::blobs::Reader,
    },
}

impl ObjectReader {
    pub async fn to_bytes(mut self) -> Result<Vec<u8>> {
        let mut stream_contents = Vec::new();
        match self {
            Self::S3 { ref mut reader, .. } => {
                while let Some(chunk) = reader.next().await {
                    let chunk = chunk?;
                    stream_contents.extend_from_slice(&chunk);
                }
            }
            Self::Iroh { ref mut reader } => {
                while let Some(chunk) = reader.next().await {
                    stream_contents.extend_from_slice(&chunk?);
                }
            }
        }

        Ok(stream_contents)
    }

    pub async fn to_string(self) -> Result<String> {
        let bytes = self.to_bytes().await?;
        let s = std::string::String::from_utf8(bytes)?;
        Ok(s)
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::S3 { status_code, .. } => *status_code,
            Self::Iroh { .. } => 200,
        }
    }
}

impl Stream for ObjectReader {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::S3 { ref mut reader, .. } => Pin::new(reader)
                .poll_next(cx)
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync + 'static>),
            Self::Iroh { ref mut reader } => Pin::new(reader)
                .poll_next(cx)
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync + 'static>),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::S3 { size, .. } => match size {
                Some(i) => {
                    let size = usize::try_from(*i).unwrap();
                    (size, Some(size))
                }
                None => (0, None),
            },
            Self::Iroh { reader } => reader.size_hint(),
        }
    }
}

impl ByteStream for ObjectReader {
    fn remaining_length(&self) -> RemainingLength {
        let (lower, upper) = self.size_hint();
        RemainingLength::new(lower, upper)
    }
}

type Doc = iroh_docs::rpc::client::docs::Doc<
    quic_rpc::transport::flume::FlumeConnector<
        iroh_docs::rpc::proto::Response,
        iroh_docs::rpc::proto::Request,
    >,
>;

#[derive(Debug)]
struct BucketProxy {
    node: Iroh,
    author: AuthorId,
    doc: Doc,
}

impl BucketProxy {
    async fn open(node: Iroh, ticket: DocTicket) -> Result<BucketProxy> {
        let author = node.docs().authors().default().await?;
        let doc = node.docs().import(ticket).await?;
        let download_nothing_policy = iroh_docs::store::DownloadPolicy::NothingExcept(vec![]);
        doc.set_download_policy(download_nothing_policy).await?;
        Ok(BucketProxy { node, author, doc })
    }

    // pub async fn put_blob<B: impl Into<Bytes>>(&self, key: B, value: B) -> Result<Hash> {
    pub async fn put_blob(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<Hash> {
        self.doc
            .set_bytes(self.author, key.into(), value.into())
            .await
    }

    pub async fn get_blob(
        &self,
        key: impl Into<Bytes>,
    ) -> Result<iroh_blobs::rpc::client::blobs::Reader> {
        let hash = self.blob_hash(key.into()).await?;

        let status = self.node.blobs().status(hash).await?;
        let reader = match status {
            iroh_blobs::rpc::client::blobs::BlobStatus::Complete { .. } => {
                self.node.blobs().read(hash).await?
            }
            // TODO(b5): handle partial blobs, checking for overlap?
            _ => {
                self.download_blob(hash).await?;
                self.node.blobs().read(hash).await?
            }
        };

        Ok(reader)
    }

    async fn blob_hash(&self, key: Bytes) -> Result<Hash> {
        let query = iroh_docs::store::Query::key_exact(key);
        match self.doc.get_one(query).await? {
            Some(entry) => Ok(entry.content_hash()),
            None => Err(anyhow!("object not found")),
        }
    }

    async fn download_blob(
        &self,
        hash: Hash,
    ) -> Result<iroh_blobs::rpc::client::blobs::DownloadOutcome> {
        let nodes = self.connected_peers().await?;
        if nodes.is_empty() {
            return Err(anyhow!("no connected peers"));
        }

        let opts = iroh_blobs::rpc::client::blobs::DownloadOptions {
            nodes,
            format: iroh_blobs::BlobFormat::Raw,
            mode: iroh_blobs::net_protocol::DownloadMode::Queued,
            tag: SetTagOption::Auto,
        };

        let outcome = self
            .node
            .blobs()
            .download_with_opts(hash, opts)
            .await?
            .finish()
            .await?;

        Ok(outcome)
    }

    async fn connected_peers(&self) -> Result<Vec<NodeAddr>> {
        let peers = self.doc.get_sync_peers().await?;
        match peers {
            None => Ok(vec![]),
            Some(peers) => {
                let peers = peers
                    .iter()
                    .map(|b| NodeAddr::from(PublicKey::from_bytes(b).unwrap()))
                    .collect();
                Ok(peers)
            }
        }
    }
}

pub enum Range {
    All,
    /// Int range in bytes. This range is **inclusive**.
    ///
    /// See <https://www.rfc-editor.org/rfc/rfc9110.html#rule.int-range>
    Int {
        /// first position
        first: u64,
        /// last position
        last: Option<u64>,
    },
    /// Suffix range in bytes.
    ///
    /// See <https://www.rfc-editor.org/rfc/rfc9110.html#rule.suffix-range>
    Suffix {
        /// suffix length
        length: u64,
    },
}

impl From<Option<s3sRange>> for Range {
    fn from(value: Option<s3sRange>) -> Self {
        match value {
            Some(s3sRange::Int { first, last }) => Range::Int { first, last },
            Some(s3sRange::Suffix { length }) => Range::Suffix { length },
            None => Range::All,
        }
    }
}
