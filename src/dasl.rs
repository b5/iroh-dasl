use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use ipld_core::cid::Cid;
use iroh_blobs::Hash as IrohHash;

// Multicodecs DASL supports
const RAW_CODE_POINT: u64 = 0x55;
const DAG_CBOR_CODE_POINT: u64 = 0x71;

// Multihashes DASL supports
const SHA_2_CODE_POINT: u64 = 0x12;
const BLAKE_3_CODE_POINT: u64 = 0x1e;

pub(crate) enum DaslCodec {
    Raw,
    DagCbor,
}

impl DaslCodec {
    fn multicodec_code(self) -> u64 {
        match self {
            DaslCodec::Raw => RAW_CODE_POINT,
            DaslCodec::DagCbor => DAG_CBOR_CODE_POINT,
        }
    }
}

impl TryFrom<u64> for DaslCodec {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            RAW_CODE_POINT => Ok(DaslCodec::Raw),
            DAG_CBOR_CODE_POINT => Ok(DaslCodec::DagCbor),
            _ => Err(anyhow!("Unsupported DASL multicodec")),
        }
    }
}

enum DaslHash {
    Sha2,
    Blake3,
}

impl DaslHash {
    fn multihash_code(self) -> u64 {
        match self {
            DaslHash::Sha2 => SHA_2_CODE_POINT,
            DaslHash::Blake3 => BLAKE_3_CODE_POINT,
        }
    }
}

impl TryFrom<u64> for DaslHash {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            SHA_2_CODE_POINT => Ok(DaslHash::Sha2),
            BLAKE_3_CODE_POINT => Ok(DaslHash::Blake3),
            _ => Err(anyhow!("Unsupported DASL multihash")),
        }
    }
}

pub fn blake3_to_cid(hash: IrohHash, codec: DaslCodec) -> Cid {
    let mh =
        multihash::Multihash::wrap(DaslHash::Blake3.multihash_code(), hash.as_bytes()).unwrap();
    Cid::new_v1(codec.multicodec_code(), mh)
}

pub fn sha2_to_cid(hash: &[u8], codec: DaslCodec) -> Cid {
    let mh = multihash::Multihash::wrap(DaslHash::Sha2.multihash_code(), hash).unwrap();
    Cid::new_v1(codec.multicodec_code(), mh)
}

/// Map of SHA2-256 <-> BLAKE3 hashes. Because iroh blobs only speaks BLAKE3, we
/// keep a map that hashes the same bytes with both hash functions so we can
/// lookup the corresponding BLAKE3 hash for a given SHA2, and feed that to
/// iroh.
///
/// This is a silly in-memory-only proof of concept that could be extended
/// with lookups from an external service. Given DASL has a block limit for SHA2
/// data, it's totally fine to maintain this mapping as local knowledge.
#[derive(Debug, Clone)]
pub(crate) struct ShaMap(Arc<Mutex<HashMap<[u8; 32], IrohHash>>>);

impl ShaMap {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    pub(crate) fn set(&self, sha2: [u8; 32], blake3: IrohHash) {
        self.0.lock().unwrap().insert(sha2, blake3);
    }

    pub(crate) fn get(&self, sha2: [u8; 32]) -> Option<IrohHash> {
        self.0.lock().unwrap().get(&sha2).cloned()
    }

    pub(crate) fn cid_to_blake3_hash(&self, cid: Cid) -> Result<(DaslCodec, IrohHash)> {
        let codec = DaslCodec::try_from(cid.codec())?;
        let hash_type = DaslHash::try_from(cid.hash().code())?;

        let digest = cid.hash().digest();
        if digest.len() != 32 {
            return Err(anyhow!("Invalid hash length"));
        }
        // Convert to fixed-size array
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(digest);

        match hash_type {
            DaslHash::Sha2 => {
                let hash = self
                    .get(hash_bytes)
                    .ok_or_else(|| anyhow!("SHA2 hash not found"))?;
                Ok((codec, hash))
            }
            DaslHash::Blake3 => {
                let hash = IrohHash::from(hash_bytes);
                Ok((codec, hash))
            }
        }
    }
}
