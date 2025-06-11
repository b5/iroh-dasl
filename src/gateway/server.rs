use std::ops::Deref;
use std::{
    result,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Extension, Router,
    body::Body,
    extract::Path,
    http::{Method, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
// use derive_more::Deref;
use ipld_core::cid::Cid;
use iroh::{Endpoint, NodeAddr, endpoint::Connection};
use iroh_blobs::{
    BlobFormat, Hash,
    format::collection::Collection,
    get::fsm::{BlobContentNext, ConnectedNext, DecodeError, EndBlobNext},
    protocol::{ALPN, RangeSpecSeq},
    store::bao_tree::{ChunkNum, io::fsm::BaoContentItem},
    ticket::BlobTicket,
};
use lru::LruCache;
use mime::Mime;
use mime_classifier::MimeClassifier;
use range_collections::RangeSet2;
use tower_http::cors::{AllowHeaders, AllowOrigin, CorsLayer};
use url::Url;

use super::ranges::{parse_byte_range, slice, to_byte_range, to_chunk_range};

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Debug, Clone)]
struct Gateway(Arc<Inner>);

impl Deref for Gateway {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

type MimeCache = LruCache<(Hash, Option<String>), (u64, Mime)>;

#[derive(derive_more::Debug)]
struct Inner {
    /// Endpoint to connect to nodes
    endpoint: Endpoint,
    /// Default node to connect to when not specified in the url
    default_node: Option<NodeAddr>,
    /// This will replace default_node
    client: iroh_blobs::rpc::client::blobs::MemClient,
    /// Mime classifier
    #[debug("MimeClassifier")]
    mime_classifier: MimeClassifier,
    /// Cache of hashes to mime types
    mime_cache: Mutex<MimeCache>,
    /// Cache of hashes to collections
    collection_cache: Mutex<LruCache<Hash, Collection>>,
}

impl Inner {
    /// Get the default node to connect to when not specified in the url
    fn default_node(&self) -> anyhow::Result<NodeAddr> {
        let node_addr = self
            .default_node
            .clone()
            .context("default node not configured")?;
        Ok(node_addr)
    }

    /// Get the mime type for a hash from the remote node.
    async fn get_default_connection(&self) -> anyhow::Result<Connection> {
        let connection = self.endpoint.connect(self.default_node()?, ALPN).await?;
        Ok(connection)
    }
}

async fn get_collection_inner(
    hash: &Hash,
    connection: &iroh::endpoint::Connection,
    headers: bool,
) -> anyhow::Result<(Collection, Vec<(Hash, u64, Vec<u8>)>)> {
    let spec = if headers {
        RangeSpecSeq::from_ranges_infinite(vec![
            RangeSet2::all(),
            RangeSet2::all(),
            RangeSet2::from(..ChunkNum::chunks(2048)),
        ])
    } else {
        RangeSpecSeq::from_ranges(vec![RangeSet2::all(), RangeSet2::all()])
    };
    let request = iroh_blobs::protocol::GetRequest::new(*hash, spec);
    let req = iroh_blobs::get::fsm::start(connection.clone(), request);
    let connected = req.next().await?;
    let ConnectedNext::StartRoot(at_start_root) = connected.next().await? else {
        anyhow::bail!("unexpected response");
    };
    let (mut curr, hash_seq, collection) = Collection::read_fsm(at_start_root).await?;

    let mut headers = Vec::new();
    let at_closing = loop {
        match curr {
            EndBlobNext::Closing(at_closing) => {
                break at_closing;
            }
            EndBlobNext::MoreChildren(at_start_child) => {
                let Some(hash) = hash_seq.get(at_start_child.child_offset() as usize) else {
                    break at_start_child.finish();
                };
                let at_blob_header = at_start_child.next(hash);
                let (at_blob_content, size) = at_blob_header.next().await?;
                let (at_end_blob, data) = at_blob_content.concatenate_into_vec().await?;
                curr = at_end_blob.next();
                headers.push((hash, size, data));
            }
        }
    };
    let _stats = at_closing.next().await?;

    Ok((collection, headers))
}

/// Get the collection. This will also fill the mime cache.
async fn get_collection(
    gateway: &Gateway,
    hash: &Hash,
    connection: &Connection,
) -> anyhow::Result<Collection> {
    if let Some(res) = gateway.collection_cache.lock().unwrap().get(hash) {
        return Ok(res.clone());
    }
    let (collection, headers) = get_collection_inner(hash, connection, true).await?;

    let mut cache = gateway.mime_cache.lock().unwrap();
    for (name, hash) in collection.iter() {
        let ext = get_extension(name);
        let Some((hash, size, data)) = headers.iter().find(|(h, _, _)| h == hash) else {
            tracing::debug!("hash {hash:?} for name {name:?} not found in headers");
            continue;
        };
        let mime = get_mime_from_ext_and_data(ext.as_deref(), data, &gateway.mime_classifier);
        let key = (*hash, ext);
        cache.put(key, (*size, mime));
    }
    drop(cache);

    gateway
        .collection_cache
        .lock()
        .unwrap()
        .put(*hash, collection.clone());
    Ok(collection)
}

fn get_extension(name: &str) -> Option<String> {
    std::path::Path::new(name)
        .extension()
        .map(|s| s.to_string_lossy().to_string())
}

/// Get the mime type for a hash from the remote node.
async fn get_mime_type_inner(
    hash: &Hash,
    ext: Option<&str>,
    connection: &Connection,
    mime_classifier: &MimeClassifier,
) -> anyhow::Result<(u64, Mime)> {
    // read 2 KiB.
    let range = RangeSpecSeq::from_ranges(Some(RangeSet2::from(..ChunkNum::chunks(2048))));
    let request = iroh_blobs::protocol::GetRequest::new(*hash, range);
    let req = iroh_blobs::get::fsm::start(connection.clone(), request);
    let connected = req.next().await?;
    let ConnectedNext::StartRoot(x) = connected.next().await? else {
        anyhow::bail!("unexpected response");
    };
    let (at_blob_content, size) = x.next().next().await?;
    let (at_end, data) = at_blob_content.concatenate_into_vec().await?;
    let EndBlobNext::Closing(at_closing) = at_end.next() else {
        anyhow::bail!("unexpected response");
    };
    let _stats = at_closing.next().await?;
    let mime = get_mime_from_ext_and_data(ext, &data, mime_classifier);
    Ok((size, mime))
}

fn get_mime_from_ext_and_data(
    ext: Option<&str>,
    data: &[u8],
    mime_classifier: &MimeClassifier,
) -> Mime {
    let context = mime_classifier::LoadContext::Browsing;
    let no_sniff_flag = mime_classifier::NoSniffFlag::On;
    let apache_bug_flag = mime_classifier::ApacheBugFlag::On;
    let supplied_type = match ext {
        None => None,
        Some(ext) => mime_guess::from_ext(ext).first(),
    };
    mime_classifier.classify(
        context,
        no_sniff_flag,
        apache_bug_flag,
        &supplied_type,
        data,
    )
}

/// Get the mime type for a hash, either from the cache or by requesting it from the node.
async fn get_mime_type(
    gateway: &Gateway,
    hash: &Hash,
    name: Option<&str>,
    connection: &Connection,
) -> anyhow::Result<(u64, Mime)> {
    let ext = name.and_then(get_extension);
    let key = (*hash, ext.clone());
    if let Some(sm) = gateway.mime_cache.lock().unwrap().get(&key) {
        return Ok(sm.clone());
    }
    let sm =
        get_mime_type_inner(hash, ext.as_deref(), connection, &gateway.mime_classifier).await?;
    gateway.mime_cache.lock().unwrap().put(key, sm.clone());
    Ok(sm)
}

async fn handle_index() -> std::result::Result<String, AppError> {
    tracing::info!("handle_index");
    Ok("oh hai fren".to_string())
}

/// Handle a request for a range of bytes from the default node.
async fn handle_local_blob_request(
    // gateway: Extension<Gateway>,
    Path(cid): Path<Cid>,
    // req: Request<Body>,
) -> std::result::Result<String, AppError> {
    println!("{:?}", cid);
    // cid.hash().code() != 0x55 {
    // }
    // let connection = gateway.get_default_connection().await?;
    // let byte_range = parse_byte_range(req).await?;
    // let res = forward_range(&gateway, connection, &blake3_hash, None, byte_range).await?;
    // Ok(res)
    Ok(cid.to_string())
}

async fn handle_local_collection_index(
    gateway: Extension<Gateway>,
    Path(hash): Path<Hash>,
) -> std::result::Result<impl IntoResponse, AppError> {
    let connection = gateway.get_default_connection().await?;
    let link_prefix = format!("/collection/{}", hash);
    let res = collection_index(&gateway, connection, &hash, &link_prefix).await?;
    Ok(res)
}

/// Handle a request for a range of bytes from the default node.
async fn handle_local_collection_request(
    gateway: Extension<Gateway>,
    Path((hash, suffix)): Path<(Hash, String)>,
    req: Request<Body>,
) -> std::result::Result<impl IntoResponse, AppError> {
    let connection = gateway.get_default_connection().await?;
    let byte_range = parse_byte_range(req).await?;
    let res = forward_collection_range(&gateway, connection, &hash, &suffix, byte_range).await?;
    Ok(res)
}

async fn handle_ticket_index(
    gateway: Extension<Gateway>,
    Path(ticket): Path<BlobTicket>,
    req: Request<Body>,
) -> std::result::Result<impl IntoResponse, AppError> {
    tracing::info!("handle_ticket_index");
    let byte_range = parse_byte_range(req).await?;
    let connection = gateway
        .endpoint
        .connect(ticket.node_addr().clone(), ALPN)
        .await?;
    let hash = ticket.hash();
    let prefix = format!("/ticket/{}", ticket);
    let res = match ticket.format() {
        BlobFormat::Raw => forward_range(&gateway, connection, &hash, None, byte_range)
            .await?
            .into_response(),
        BlobFormat::HashSeq => collection_index(&gateway, connection, &hash, &prefix)
            .await?
            .into_response(),
    };
    Ok(res)
}

async fn handle_ticket_request(
    gateway: Extension<Gateway>,
    Path((ticket, suffix)): Path<(BlobTicket, String)>,
    req: Request<Body>,
) -> std::result::Result<impl IntoResponse, AppError> {
    tracing::info!("handle_ticket_request");
    let byte_range = parse_byte_range(req).await?;
    let connection = gateway
        .endpoint
        .connect(ticket.node_addr().clone(), ALPN)
        .await?;
    let hash = ticket.hash();
    let res = forward_collection_range(&gateway, connection, &hash, &suffix, byte_range).await?;
    Ok(res)
}

async fn collection_index(
    gateway: &Gateway,
    connection: Connection,
    hash: &Hash,
    link_prefix: &str,
) -> anyhow::Result<impl IntoResponse + use<>> {
    fn encode_relative_url(relative_url: &str) -> anyhow::Result<String> {
        let base = Url::parse("http://example.com")?;
        let joined_url = base.join(relative_url)?;

        Ok(joined_url[url::Position::BeforePath..].to_string())
    }

    let collection = get_collection(gateway, hash, &connection).await?;
    let mut res = String::new();
    res.push_str("<html>\n<head></head>\n");

    for (name, child_hash) in collection.iter() {
        let url = format!("{}/{}", link_prefix, name);
        let url = encode_relative_url(&url)?;
        let key = (*child_hash, get_extension(name));
        let smo = gateway.mime_cache.lock().unwrap().get(&key).cloned();
        res.push_str(&format!("<a href=\"{}\">{}</a>", url, name,));
        if let Some((size, mime)) = smo {
            res.push_str(&format!(" ({}, {})", mime, indicatif::HumanBytes(size)));
        }
        res.push_str("<br>\n");
    }
    res.push_str("</body>\n</html>\n");
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html")
        .header(header::CACHE_CONTROL, "max-age=3600")
        .body(res)?;
    Ok((StatusCode::OK, response).into_response())
}

async fn forward_collection_range(
    gateway: &Gateway,
    connection: Connection,
    hash: &Hash,
    suffix: &str,
    range: (Option<u64>, Option<u64>),
) -> anyhow::Result<impl IntoResponse + use<>> {
    let suffix = suffix.strip_prefix('/').unwrap_or(suffix);
    tracing::trace!("suffix {}", suffix);
    let collection = get_collection(gateway, hash, &connection).await?;
    for (name, hash) in collection.iter() {
        if name == suffix {
            let res = forward_range(gateway, connection, hash, Some(suffix), range).await?;
            return Ok(res.into_response());
        } else {
            tracing::trace!("'{}' != '{}'", name, suffix);
        }
    }
    Ok((
        StatusCode::NOT_FOUND,
        format!("entry '{}' not found in collection '{}'", suffix, hash),
    )
        .into_response())
}

fn format_content_range(start: Option<u64>, end: Option<u64>, size: u64) -> String {
    format!(
        "bytes {}-{}/{}",
        start.map(|x| x.to_string()).unwrap_or_default(),
        end.map(|x| (x + 1).to_string())
            .unwrap_or_else(|| size.to_string()),
        size
    )
}

async fn forward_range(
    gateway: &Gateway,
    connection: Connection,
    hash: &Hash,
    name: Option<&str>,
    (start, end): (Option<u64>, Option<u64>),
) -> anyhow::Result<Response<Body>> {
    // we need both byte ranges and chunk ranges.
    // chunk ranges to request data, and byte ranges to return the data.
    tracing::debug!("forward_range {:?} {:?} (name {name:?})", start, end);

    let byte_ranges = to_byte_range(start, end);
    let chunk_ranges = to_chunk_range(start, end);
    tracing::debug!("got connection");
    let (_size, mime) = get_mime_type(gateway, hash, name, &connection).await?;
    tracing::debug!("mime: {}", mime);
    let chunk_ranges = RangeSpecSeq::from_ranges(vec![chunk_ranges]);
    let request = iroh_blobs::protocol::GetRequest::new(*hash, chunk_ranges.clone());
    let status_code = if byte_ranges.is_all() {
        StatusCode::OK
    } else {
        StatusCode::PARTIAL_CONTENT
    };
    tracing::debug!("status_code {}", status_code);
    let (send, recv) = flume::bounded::<result::Result<Bytes, DecodeError>>(2);

    tracing::trace!("requesting {:?}", request);
    let req = iroh_blobs::get::fsm::start(connection.clone(), request);
    let connected = req.next().await?;
    let ConnectedNext::StartRoot(x) = connected.next().await? else {
        anyhow::bail!("unexpected response");
    };
    tracing::trace!("connected");
    let (mut current, size) = x.next().next().await?;
    tokio::spawn(async move {
        let end = loop {
            match current.next().await {
                BlobContentNext::More((next, Ok(item))) => {
                    match item {
                        BaoContentItem::Leaf(leaf) => {
                            tracing::trace!("got leaf {} {}", leaf.offset, leaf.data.len());
                            for item in slice(leaf.offset, leaf.data, byte_ranges.clone()) {
                                send.send_async(Ok(item)).await?;
                            }
                        }
                        BaoContentItem::Parent(parent) => {
                            tracing::trace!("got parent {:?}", parent);
                        }
                    }
                    current = next;
                }
                BlobContentNext::More((_, Err(err))) => {
                    send.send_async(Err(err)).await?;
                    anyhow::bail!("error");
                }
                BlobContentNext::Done(end) => break end,
            }
        };
        let EndBlobNext::Closing(at_closing) = end.next() else {
            anyhow::bail!("unexpected response");
        };
        let _stats = at_closing.next().await?;
        Ok(())
    });
    let body = Body::from_stream(recv.into_stream());
    let builder = Response::builder()
        .status(status_code)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "public,max-age=31536000,immutable")
        .header(header::CONTENT_TYPE, mime.to_string());
    // content-length needs to be the actual repsonse size
    let transfer_size = match (start, end) {
        (Some(start), Some(end)) => end - start,
        (Some(start), None) => size - start,
        (None, Some(end)) => end,
        (None, None) => size,
    };
    let builder = builder.header(header::CONTENT_LENGTH, transfer_size);

    let builder = if start.is_some() || end.is_some() {
        builder
            .header(
                header::CONTENT_RANGE,
                format_content_range(start, end, size),
            )
            .status(StatusCode::PARTIAL_CONTENT)
    } else {
        builder
    };
    let response = builder.body(body).unwrap();
    Ok(response)
}

pub async fn run(
    endpoint: Endpoint,
    client: iroh_blobs::rpc::client::blobs::MemClient,
    serve_addr: &str,
) -> anyhow::Result<()> {
    let node_addr = endpoint.node_addr().await?;
    let gateway = Gateway(Arc::new(Inner {
        endpoint,
        default_node: Some(node_addr),
        client,
        mime_classifier: MimeClassifier::new(),
        mime_cache: Mutex::new(LruCache::new(100000.try_into().unwrap())),
        collection_cache: Mutex::new(LruCache::new(1000.try_into().unwrap())),
    }));

    let cors = CorsLayer::new()
        .allow_headers(AllowHeaders::mirror_request())
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_origin(AllowOrigin::mirror_request());

    #[rustfmt::skip]
    let app = Router::new()
        .without_v07_checks()
        .route("/", get(handle_index))
        .route("/collection/:blake3_hash", get(handle_local_collection_index))
        .route("/collection/:blake3_hash/*path", get(handle_local_collection_request))
        .route("/blob/:cid", get(handle_local_blob_request))
        .route("/ticket/:ticket", get(handle_ticket_index))
        .route("/ticket/:ticket/*path", get(handle_ticket_request))
        .layer(cors)
        .layer(Extension(gateway));
    // Run our application as just http
    println!("listening on {}, http", serve_addr);

    let listener = tokio::net::TcpListener::bind(serve_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
