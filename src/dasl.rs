use anyhow::{Result, anyhow};
use ipld_core::cid::Cid;
use iroh_blobs::Hash;

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

pub fn iroh_hash_to_cid(hash: Hash, codec: DaslCodec) -> Cid {
    let mh =
        multihash::Multihash::wrap(DaslHash::Blake3.multihash_code(), hash.as_bytes()).unwrap();
    Cid::new_v1(codec.multicodec_code(), mh)
}

pub fn cid_to_iroh_hash(cid: Cid) -> Result<(DaslCodec, Hash)> {
    let codec = DaslCodec::try_from(cid.codec())?;
    let hash_type = DaslHash::try_from(cid.hash().code())?;
    match hash_type {
        DaslHash::Sha2 => {
            todo!("support working with sha2 hashes");
        }
        DaslHash::Blake3 => {
            let digest = cid.hash().digest();
            if digest.len() != 32 {
                return Err(anyhow!("Invalid BLAKE3 hash length"));
            }

            // Convert to fixed-size array
            let mut hash_bytes = [0u8; 32];
            hash_bytes.copy_from_slice(digest);

            let hash = Hash::from(hash_bytes);
            Ok((codec, hash))
        }
    }
}
