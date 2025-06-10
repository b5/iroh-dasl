use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::ops::Not;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::pin_mut;
use hyper::server::Server;
use md5::{Digest, Md5};
use n0_future::{Stream, StreamExt, TryStreamExt};
use path_absolutize::Absolutize;
use rust_utils::default::default;
use s3s::auth::{Credentials, S3Auth, SecretKey};
use s3s::dto::*;
use s3s::service::{MakeService, S3ServiceBuilder, SharedS3Service};
use s3s::stream::ByteStream;
use s3s::S3ErrorCode;
use s3s::{s3_error, S3Error, S3Request, S3Response, S3Result, StdError, S3};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info};
use transform_stream::AsyncTryStream;
use uuid::Uuid;

use crate::bucket::Bucket as CrateBucket;
use crate::node::Node;

pub type S3Server = Server<hyper::server::conn::AddrIncoming, MakeService<SharedS3Service>>;

pub async fn serve_s3_api(
    addr: SocketAddr,
    data_dir: &Path,
    node: Node,
) -> anyhow::Result<S3Server> {
    // Setup S3 service
    let service = {
        let s3_dir = data_dir.join("s3");
        tokio::fs::create_dir_all(&s3_dir).await?;
        let s3_api = S3Api::new(s3_dir, node);
        let mut b = S3ServiceBuilder::new(s3_api);

        b.set_auth(Auth::from_single("access", "secret"));
        // // Enable parsing virtual-hosted-style requests
        // if let Some(domain_name) = cfg.s3_domain_name {
        //     b.set_base_domain(domain_name);
        //     info!("S3 API virtual-hosted-style requests are enabled");
        // }

        b.build()
    };

    // Run S3 HTTP API server
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let server = Server::from_tcp(listener)?.serve(service.into_shared().into_make_service());
    info!("S3 API is running at http://{local_addr}");
    Ok(server)
}

macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                $crate::http::log(&err);
                return Err(::s3s::S3Error::internal_error(err));
            }
        }
    };
}

#[derive(Debug)]
pub struct S3Api {
    root: PathBuf,
    node: Node,
    tmp_file_counter: AtomicU64,
}

impl S3Api {
    pub fn new(root: PathBuf, node: Node) -> Self {
        let tmp_file_counter = AtomicU64::new(0);
        Self {
            root,
            node,
            tmp_file_counter,
        }
    }

    async fn has_crate_bucket(&self, bucket: &str) -> bool {
        self.node.get_bucket(bucket).is_some()
    }

    fn get_bucket(&self, bucket: &str) -> Result<&CrateBucket, S3Error> {
        let bucket = self
            .node
            .get_bucket(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        Ok(bucket)
    }

    // Everything below here in this implementation is to support multipart uploads, much of which
    // writes to a directory within the iroh-s3 directory & isn't used.
    // TODO - refactor, integrating support for metadata & whatnot.

    /// resolve object path under the virtual root
    fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        self.resolve_abs_path(dir.join(file_path))
    }

    fn get_upload_info_path(&self, upload_id: &Uuid) -> Result<PathBuf> {
        self.resolve_abs_path(format!(".upload-{upload_id}.json"))
    }

    fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    fn resolve_upload_part_path(
        &self,
        upload_id: Uuid,
        part_number: PartNumber,
    ) -> Result<PathBuf> {
        self.resolve_abs_path(format!(".upload_id-{upload_id}.part-{part_number}"))
    }

    async fn create_upload_id(&self, cred: Option<&Credentials>) -> Result<Uuid> {
        let upload_id = Uuid::new_v4();
        let upload_info_path = self.get_upload_info_path(&upload_id)?;

        let ak: Option<&str> = cred.map(|c| c.access_key.as_str());

        let content = serde_json::to_vec(&ak)?;
        fs::write(&upload_info_path, &content).await?;

        Ok(upload_id)
    }

    async fn verify_upload_id(&self, cred: Option<&Credentials>, upload_id: &Uuid) -> Result<bool> {
        let upload_info_path = self.get_upload_info_path(upload_id)?;
        if upload_info_path.exists().not() {
            return Ok(false);
        }

        let content = fs::read(&upload_info_path).await?;
        let ak: Option<String> = serde_json::from_slice(&content)?;

        Ok(ak.as_deref() == cred.map(|c| c.access_key.as_str()))
    }

    async fn delete_upload_id(&self, upload_id: &Uuid) -> Result<()> {
        let upload_info_path = self.get_upload_info_path(upload_id)?;
        if upload_info_path.exists() {
            fs::remove_file(&upload_info_path).await?;
        }
        Ok(())
    }

    /// Write to the filesystem atomically.
    /// This is done by first writing to a temporary location and then moving the file.
    pub(crate) async fn prepare_file_write<'a>(&self, path: &'a Path) -> Result<FileWriter<'a>> {
        let tmp_name = format!(
            ".tmp.{}.internal.part",
            self.tmp_file_counter.fetch_add(1, Ordering::SeqCst)
        );
        let tmp_path = self.resolve_abs_path(tmp_name)?;
        let file = File::create(&tmp_path).await?;
        let writer = BufWriter::new(file);
        Ok(FileWriter {
            tmp_path,
            dest_path: path,
            writer,
            clean_tmp: true,
        })
    }

    pub(crate) async fn get_md5_sum(&self, bucket: &str, key: &str) -> Result<String> {
        let object_path = self.get_object_path(bucket, key)?;
        let mut file = File::open(&object_path).await?;
        let mut buf = vec![0; 65536];
        let mut md5_hash = Md5::new();
        loop {
            let nread = file.read(&mut buf).await?;
            if nread == 0 {
                break;
            }
            md5_hash.update(&buf[..nread]);
        }
        Ok(hex(md5_hash.finalize()))
    }

    /// resolve metadata path under the virtual root (custom format)
    pub(crate) fn get_metadata_path(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<PathBuf> {
        let encode = |s: &str| base64_simd::URL_SAFE_NO_PAD.encode_to_string(s);
        let u_ext = upload_id
            .map(|u| format!(".upload-{u}"))
            .unwrap_or_default();
        let file_path = format!(
            ".bucket-{}.object-{}{u_ext}.metadata.json",
            encode(bucket),
            encode(key)
        );
        self.resolve_abs_path(file_path)
    }

    /// load metadata from fs
    pub(crate) async fn load_metadata(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<Option<s3s::dto::Metadata>> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        if path.exists().not() {
            return Ok(None);
        }
        let content = fs::read(&path).await?;
        let map = serde_json::from_slice(&content)?;
        Ok(Some(map))
    }

    /// save metadata to fs
    pub(crate) async fn save_metadata(
        &self,
        bucket: &str,
        key: &str,
        metadata: &s3s::dto::Metadata,
        upload_id: Option<Uuid>,
    ) -> Result<()> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        let content = serde_json::to_vec(metadata)?;
        fs::write(&path, &content).await?;
        Ok(())
    }

    /// remove metadata from fs
    pub(crate) fn delete_metadata(
        &self,
        bucket: &str,
        key: &str,
        upload_id: Option<Uuid>,
    ) -> Result<()> {
        let path = self.get_metadata_path(bucket, key, upload_id)?;
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl S3 for S3Api {
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        Err(s3_error!(NotImplemented, "create_bucket"))
    }

    async fn delete_bucket(
        &self,
        _req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        Err(s3_error!(NotImplemented, "delete_bucket"))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = self.get_bucket(&req.input.bucket)?;
        let region = bucket.config().region.to_string();
        Ok(S3Response::new(HeadBucketOutput {
            // TODO(b5) - open question about how we want to represent this
            // we could claim the regin is "local", but for now we're just passing through
            // the backing bucket's details
            bucket_region: Some(region),
            ..Default::default()
        }))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;

        // check if bucket exists
        if !self.has_crate_bucket(&input.bucket).await {
            return Err(s3_error!(NoSuchBucket));
        };
        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        debug!("list_buckets");
        let buckets: Vec<Bucket> = self
            .node
            .list_buckets()
            .into_iter()
            .map(|b: &CrateBucket| Bucket {
                creation_date: None,
                name: Some(b.config().name.clone()),
            })
            .collect();

        debug!("list_buckets: {:?}", buckets);

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        _req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        Err(s3_error!(NotImplemented, "copy_object"))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let bucket = self.get_bucket(&input.bucket)?; // check if bucket exists
        bucket.delete_object(&input.key).await.map_err(|e| {
            debug!("Failed to delete object: {}", e);
            s3_error!(NoSuchKey)
        })?;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    async fn delete_objects(
        &self,
        _req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        Err(s3_error!(NotImplemented, "delete_objects"))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let bucket = self
            .node
            .get_bucket(&input.bucket)
            .ok_or(s3_error!(NoSuchBucket))?;

        if input.range.is_some() {
            return Err(s3_error!(NotImplemented, "range"));
        }

        let res = bucket
            .get_object(input.key, input.range)
            .await
            .map_err(|e| {
                tracing::debug!("{:?}", e);
                s3_error!(NoSuchKey)
            })?;

        // let object_metadata = self.load_metadata(&input.bucket, &input.key).await?;
        // let md5_sum = self.get_md5_sum(&input.bucket, &input.key).await?;
        // let e_tag = format!("\"{md5_sum}\"");
        // let e_tag = hash.to_string();
        let e_tag = None;

        // let info = self.load_internal_info(&input.bucket, &input.key).await?;
        // let checksum = match &info {
        //     Some(info) => crate::checksum::from_internal_info(info),
        //     None => default(),
        // };

        // TODO
        let checksum: Checksum = default();

        let content_length = res.remaining_length().exact().map(|i| i as i64);
        let output = GetObjectOutput {
            body: Some(StreamingBlob::new(res)),
            // content_length: Some(content_length_i64),
            // last_modified: Some(last_modified),
            content_length,
            last_modified: None,
            metadata: None,
            e_tag,
            checksum_crc32: checksum.checksum_crc32,
            checksum_crc32c: checksum.checksum_crc32c,
            checksum_sha1: checksum.checksum_sha1,
            checksum_sha256: checksum.checksum_sha256,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        debug!("head_object {:?}", req);
        todo!();
        // let input = req.input;
        // let bucket = self.get_bucket(&input.bucket)?;
        // let e = bucket.get_object_info(&input.key).await.map_err(|e| {
        //     debug!("Failed to get object: {}", e);
        //     s3_error!(NoSuchKey)
        // })?;
        // let hash = e.content_hash();

        // // let file_metadata = try_!(fs::metadata(path).await);
        // let last_modified =
        //     Timestamp::from(SystemTime::UNIX_EPOCH + Duration::from_nanos(e.timestamp()));
        // // let object_metadata = self.load_metadata(&input.bucket, &input.key).await?;

        // let status = self.node.iroh().blobs().status(hash).await.map_err(|e| {
        //     debug!("Failed to get blob status: {}", e);
        //     s3_error!(NoSuchKey)
        // })?;
        // let file_len = size_from_status(status)?;
        // // // TODO: detect content type
        // let content_type = mime::APPLICATION_OCTET_STREAM;

        // let output = HeadObjectOutput {
        //     content_length: Some(i64::try_from(file_len).unwrap()),
        //     content_type: Some(content_type),
        //     last_modified: Some(last_modified),
        //     metadata: None,
        //     e_tag: Some(hash.to_string()),
        //     accept_ranges: Some("bytes".to_string()),
        //     ..Default::default()
        // };
        // Ok(S3Response::new(output))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = self.get_bucket(&input.bucket)?;
        let objects: Vec<_> = bucket
            .list_objects()
            .await
            .map_err(|e| {
                debug!("Failed to list objects: {}", e);
                s3_error!(InternalError)
            })?
            .into_iter()
            .map(|e| Object {
                key: Some(e.prefix.unwrap()),
                ..Default::default()
            })
            .collect();

        let key_count = Some(i32::try_from(objects.len()).unwrap());

        let output = ListObjectsV2Output {
            key_count,
            max_keys: key_count,
            contents: Some(objects),
            delimiter: input.delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            prefix: input.prefix,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            // metadata,
            // content_length,
            ..
        } = input;

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let bucket = self.get_bucket(&bucket)?;

        let data = body
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|_e| s3_error!(InternalError))?;

        bucket
            .put_object(&key, &data)
            .await
            .map_err(|_e| s3_error!(InternalError))?;

        // debug!(path = %object_path.display(), ?size, %md5_sum, ?checksum, "write file");

        let output = PutObjectOutput {
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = self.create_upload_id(req.credentials.as_ref()).await?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };
        debug!(?output, "create_multipart_upload");

        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            upload_id,
            part_number,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let file_path = self.resolve_upload_part_path(upload_id, part_number)?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let mut file_writer = self.prepare_file_write(&file_path).await?;
        let size = copy_bytes(stream, file_writer.writer()).await?;
        file_writer.done().await?;

        let md5_sum = hex(md5_hash.finalize());

        debug!(path = %file_path.display(), ?size, %md5_sum, "write file");

        let output = UploadPartOutput {
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;

        let upload_id = Uuid::parse_str(&input.upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        let part_number = input.part_number;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let (src_bucket, src_key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                ..
            } => (bucket, key),
        };
        let src_path = self.get_object_path(src_bucket, src_key)?;
        let dst_path = self.resolve_upload_part_path(upload_id, part_number)?;

        let mut src_file = fs::File::open(&src_path)
            .await
            .map_err(|e| s3_error!(e, NoSuchKey))?;
        let file_len = try_!(src_file.metadata().await).len();

        let (start, end) = if let Some(copy_range) = &input.copy_source_range {
            if !copy_range.starts_with("bytes=") {
                return Err(s3_error!(InvalidArgument));
            }
            let range = &copy_range["bytes=".len()..];
            let parts: Vec<&str> = range.split('-').collect();
            if parts.len() != 2 {
                return Err(s3_error!(InvalidArgument));
            }

            let start: u64 = parts[0].parse().map_err(|_| s3_error!(InvalidArgument))?;
            let mut end = file_len - 1;
            if parts[1].is_empty().not() {
                end = parts[1].parse().map_err(|_| s3_error!(InvalidArgument))?;
            }
            (start, end)
        } else {
            (0, file_len - 1)
        };

        let content_length = end - start + 1;
        let content_length_usize = try_!(usize::try_from(content_length));

        let _ = try_!(src_file.seek(io::SeekFrom::Start(start)).await);
        let body = StreamingBlob::wrap(bytes_stream(
            ReaderStream::with_capacity(src_file, 4096),
            content_length_usize,
        ));

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));

        let mut file_writer = self.prepare_file_write(&dst_path).await?;
        let size = copy_bytes(stream, file_writer.writer()).await?;
        file_writer.done().await?;

        let md5_sum = hex(md5_hash.finalize());

        debug!(path = %dst_path.display(), ?size, %md5_sum, "write file");

        let output = UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(format!("\"{md5_sum}\"")),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let mut parts: Vec<Part> = Vec::new();
        let mut iter = try_!(fs::read_dir(&self.root).await);

        let prefix = format!(".upload_id-{upload_id}");

        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            let Some(part_segment) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(part_number) = part_segment.strip_prefix(".part-") else {
                continue;
            };
            let part_number = part_number.parse::<i32>().unwrap();

            let file_meta = try_!(entry.metadata().await);
            let last_modified = Timestamp::from(try_!(file_meta.modified()));
            let size = try_!(i64::try_from(file_meta.len()));

            let part = Part {
                last_modified: Some(last_modified),
                part_number: Some(part_number),
                size: Some(size),
                ..Default::default()
            };
            parts.push(part);
        }

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        self.delete_upload_id(&upload_id).await?;

        if let Ok(Some(metadata)) = self.load_metadata(&bucket, &key, Some(upload_id)).await {
            self.save_metadata(&bucket, &key, &metadata, None).await?;
            let _ = self.delete_metadata(&bucket, &key, Some(upload_id));
        }

        let object_path = self.get_object_path(&bucket, &key)?;
        let mut file_writer = self.prepare_file_write(&object_path).await?;

        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part
                .part_number
                .ok_or_else(|| s3_error!(InvalidRequest, "missing part number"))?;
            cnt += 1;
            if part_number != cnt {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }

            let part_path = self.resolve_upload_part_path(upload_id, part_number)?;

            let mut reader = try_!(fs::File::open(&part_path).await);
            let size = try_!(tokio::io::copy(&mut reader, &mut file_writer.writer()).await);

            debug!(from = %part_path.display(), tmp = %file_writer.tmp_path().display(), to = %file_writer.dest_path().display(), ?size, "write file");
            try_!(fs::remove_file(&part_path).await);
        }
        file_writer.done().await?;

        let b = self.get_bucket(&bucket)?;

        let data = fs::read(&object_path).await.map_err(|e| {
            debug!("Failed to read file: {}", e);
            s3_error!(InternalError)
        })?;

        // TODO(b5) - uh, like, streams. lower level API should be changed to accept streams
        b.put_object(&key, &data).await.map_err(|e| {
            debug!("Failed to put object: {}", e);
            s3_error!(InternalError)
        })?;

        let file_size = try_!(fs::metadata(&object_path).await).len();
        let md5_sum = self.get_md5_sum(&bucket, &key).await?;

        debug!(?md5_sum, path = %object_path.display(), size = ?file_size, "file md5 sum");

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        if self
            .verify_upload_id(req.credentials.as_ref(), &upload_id)
            .await?
            .not()
        {
            return Err(s3_error!(AccessDenied));
        }

        let prefix = format!(".upload_id-{upload_id}");
        let mut iter = try_!(fs::read_dir(&self.root).await);
        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            if name.starts_with(&prefix) {
                try_!(fs::remove_file(entry.path()).await);
            }
        }

        self.delete_upload_id(&upload_id).await?;

        debug!(bucket = %bucket, key = %key, upload_id = %upload_id, "multipart upload aborted");

        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }
}

// fn size_from_status(status: BlobStatus) -> Result<u64, S3Error> {
//     match status {
//         BlobStatus::Complete { size, .. } => Ok(size),
//         BlobStatus::Partial { .. } | BlobStatus::NotFound => return Err(s3_error!(InternalError)),
//     }
// }

async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
where
    S: Stream<Item = Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = match result {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e)),
        };
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

fn bytes_stream<S, E>(
    stream: S,
    content_length: usize,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    AsyncTryStream::<Bytes, E, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while let Some(result) = stream.next().await {
            let mut bytes = result?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}

fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}

pub(crate) struct FileWriter<'a> {
    tmp_path: PathBuf,
    dest_path: &'a Path,
    writer: BufWriter<File>,
    clean_tmp: bool,
}

impl<'a> FileWriter<'a> {
    pub(crate) fn tmp_path(&self) -> &Path {
        &self.tmp_path
    }

    pub(crate) fn dest_path(&self) -> &'a Path {
        self.dest_path
    }

    pub(crate) fn writer(&mut self) -> &mut BufWriter<File> {
        &mut self.writer
    }

    pub(crate) async fn done(mut self) -> Result<()> {
        if let Some(final_dir_path) = self.dest_path().parent() {
            fs::create_dir_all(&final_dir_path).await?;
        }

        fs::rename(&self.tmp_path, self.dest_path()).await?;
        self.clean_tmp = false;
        Ok(())
    }
}

impl<'a> Drop for FileWriter<'a> {
    fn drop(&mut self) {
        if self.clean_tmp {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

#[derive(Debug)]
pub struct Error {
    source: StdError,
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

impl Error {
    #[must_use]
    #[track_caller]
    pub fn new(source: StdError) -> Self {
        log(&*source);
        Self { source }
    }

    #[must_use]
    #[track_caller]
    pub fn from_string(s: impl Into<String>) -> Self {
        Self::new(s.into().into())
    }
}

impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    #[track_caller]
    fn from(source: E) -> Self {
        Self::new(Box::new(source))
    }
}

impl From<Error> for S3Error {
    fn from(e: Error) -> Self {
        S3Error::with_source(S3ErrorCode::InternalError, e.source)
    }
}

#[inline]
#[track_caller]
pub(crate) fn log(source: &dyn std::error::Error) {
    let location = Location::caller();
    let span_trace = tracing_error::SpanTrace::capture();

    error!(
        target: "s3s_fs_internal_error",
        %location,
        error=%source,
        "span trace:\n{span_trace}"
    );
}

/// A simple authentication provider
#[derive(Debug, Default)]
struct Auth {
    /// key map
    map: HashMap<String, SecretKey>,
}

impl Auth {
    pub fn from_single(access_key: impl Into<String>, secret_key: impl Into<SecretKey>) -> Self {
        let access_key = access_key.into();
        let secret_key = secret_key.into();
        let map = [(access_key, secret_key)].into_iter().collect();
        Self { map }
    }

    /// lookup a secret key
    pub fn lookup(&self, access_key: &str) -> Option<&SecretKey> {
        self.map.get(access_key)
    }
}

#[async_trait::async_trait]
impl S3Auth for Auth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.lookup(access_key) {
            // None => Err(s3_error!(NotSignedUp, "Your account is not signed up")),
            None => Ok(SecretKey::from("nommnom")),
            Some(s) => Ok(s.clone()),
        }
    }
}
