use std::time::SystemTime;

use p2panda_core::cbor::{DecodeError, EncodeError, decode_cbor, encode_cbor};
use p2panda_core::{Body, Extensions, Header, PrivateKey};
use p2panda_store::{LocalLogStore, LogId, MemoryStore};

pub async fn create_operation<L, E>(
    store: &mut MemoryStore<L, E>,
    private_key: &PrivateKey,
    log_id: Option<&L>,
    extensions: Option<E>,
    body: Option<&[u8]>,
) -> (Header<E>, Option<Body>)
where
    L: LogId + Send + Sync,
    E: Extensions + Send + Sync,
{
    let body = body.map(Body::new);
    let public_key = private_key.public_key();

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("time from operation system")
        .as_secs();

    let (seq_num, backlink) = match log_id {
        Some(log_id) => {
            let Ok(latest_operation) = store.latest_operation(&public_key, log_id).await;
            match latest_operation {
                Some((header, _)) => (header.seq_num + 1, Some(header.hash())),
                None => (0, None),
            }
        }
        None => (0, None),
    };

    // @TODO(adz): Memory stores are infallible right now but we'll switch to a SQLite-based one
    // soon and then we need to handle this error here:
    let mut header = Header {
        version: 1,
        public_key,
        signature: None,
        payload_size: body.as_ref().map_or(0, |body| body.size()),
        payload_hash: body.as_ref().map(|body| body.hash()),
        timestamp,
        seq_num,
        backlink,
        previous: vec![],
        extensions,
    };
    header.sign(private_key);

    (header, body)
}

pub fn encode_gossip_message<E>(
    header: &Header<E>,
    body: Option<&Body>,
) -> Result<Vec<u8>, EncodeError>
where
    E: Extensions,
{
    let bytes = encode_cbor(&(header.to_bytes(), body.map(|body| body.to_bytes())))?;
    Ok(bytes)
}

pub fn decode_gossip_message(bytes: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>), DecodeError> {
    let raw_operation = decode_cbor(bytes)?;
    Ok(raw_operation)
}
