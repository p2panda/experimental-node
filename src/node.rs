use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use iroh_io::AsyncSliceReader;
use p2panda_blobs::{Blobs, DownloadBlobEvent, FilesystemStore as BlobsStore, ImportBlobEvent};
use p2panda_core::{Body, Extension, Extensions, Hash, Header, PrivateKey, PruneFlag, PublicKey};
use p2panda_discovery::mdns::LocalDiscovery;
use p2panda_net::{
    Network, NetworkBuilder, NetworkId, RelayUrl, SyncConfiguration, SystemEvent, TopicId,
};
use p2panda_store::{LogId, MemoryStore};
use p2panda_sync::TopicQuery;
use p2panda_sync::log_sync::{LogSyncProtocol, TopicLogMap};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::pin;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use super::{
    actor::{NodeActor, ToNodeActor},
    operation::encode_gossip_message,
    stream::{StreamController, StreamControllerError, StreamEvent, ToStreamController},
};

pub struct Node<T, L, E> {
    pub private_key: PrivateKey,
    pub store: MemoryStore<L, E>,
    pub network: Network<T>,
    blobs: Blobs<T, BlobsStore>,
    #[allow(dead_code)]
    stream: StreamController<L, E>,
    stream_tx: mpsc::Sender<ToStreamController<L, E>>,
    network_actor_tx: mpsc::Sender<ToNodeActor<T>>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl<T, L, E> Node<T, L, E>
where
    T: TopicId + TopicQuery + 'static,
    L: LogId + Send + Sync + Serialize + for<'a> Deserialize<'a> + 'static,
    E: Extensions + Extension<L> + Extension<PruneFlag> + Send + Sync + 'static,
{
    pub async fn new<TM: TopicLogMap<T, L> + 'static>(
        network_name: String,
        private_key: PrivateKey,
        bootstrap_node_id: Option<PublicKey>,
        relay_url: Option<RelayUrl>,
        store: MemoryStore<L, E>,
        blobs_root_dir: PathBuf,
        topic_map: TM,
    ) -> Result<(
        Self,
        mpsc::Receiver<StreamEvent<E>>,
        broadcast::Receiver<SystemEvent<T>>,
    )> {
        let network_id: NetworkId = Hash::new(network_name).into();

        let rt = tokio::runtime::Handle::current();

        let (stream, stream_tx, stream_rx) = StreamController::new(store.clone());
        let (ephemeral_tx, mut ephemeral_rx) = mpsc::channel(1024);

        let (network_tx, mut network_rx) = mpsc::channel(1024);

        {
            let stream_tx = stream_tx.clone();
            rt.spawn(async move {
                while let Some((header, body, header_bytes)) = network_rx.recv().await {
                    stream_tx
                        .send(ToStreamController::Ingest {
                            header,
                            body,
                            header_bytes,
                        })
                        .await
                        .expect("send stream_tx");
                }
            });
        }

        {
            let stream_tx = stream_tx.clone();
            rt.spawn(async move {
                while let Some(bytes) = ephemeral_rx.recv().await {
                    stream_tx
                        .send(ToStreamController::Ephemeral { bytes })
                        .await
                        .expect("send stream_tx");
                }
            });
        }

        let mdns = LocalDiscovery::new();

        let sync_protocol = LogSyncProtocol::new(topic_map, store.clone());
        let sync_config = SyncConfiguration::new(sync_protocol);

        let mut network_builder = NetworkBuilder::new(network_id)
            .discovery(mdns)
            .sync(sync_config)
            .private_key(private_key.clone());

        if let Some(bootstrap_node_id) = bootstrap_node_id {
            println!(
                "P2Panda: Direct address provided for peer: {}",
                bootstrap_node_id
            );
            network_builder = network_builder.direct_address(bootstrap_node_id, vec![], relay_url);
        } else {
            // I am probably the bootstrap node since I know of no others
            println!("P2Panda: No direct address provided, starting as bootstrap node");
            network_builder = network_builder.bootstrap();
        }

        let blobs_store = BlobsStore::load(blobs_root_dir).await?;
        let (network, blobs) = Blobs::from_builder(network_builder, blobs_store).await?;

        let system_events_rx = network.events().await?;

        let (network_actor_tx, network_actor_rx) = mpsc::channel(64);
        let actor = NodeActor::new(network.clone(), network_tx, ephemeral_tx, network_actor_rx);

        let actor_handle = rt.spawn(async {
            if let Err(err) = actor.run().await {
                error!("node actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|err: JoinError| err.to_string()) as JoinErrToStr)
            .shared();

        Ok((
            Self {
                private_key,
                store,
                network,
                blobs,
                stream,
                stream_tx,
                network_actor_tx,
                actor_handle: actor_drop_handle,
            },
            stream_rx,
            system_events_rx,
        ))
    }

    /// Acknowledge operations to mark them as successfully processed in the stream controller.
    pub async fn ack(&mut self, operation_id: Hash) -> Result<(), StreamControllerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.stream_tx
            .send(ToStreamController::Ack {
                operation_id,
                reply: reply_tx,
            })
            .await
            .expect("send stream_tx");
        reply_rx.await.expect("receive reply_rx")
    }

    /// Send all unacknowledged operations again on the stream which belong to these logs.
    pub async fn replay(
        &mut self,
        logs: HashMap<PublicKey, Vec<L>>,
    ) -> Result<(), StreamControllerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.stream_tx
            .send(ToStreamController::Replay {
                logs,
                reply: reply_tx,
            })
            .await
            .expect("send stream_tx");
        reply_rx.await.expect("receive reply_rx")
    }

    /// Ingest an operation without publishing it.
    pub async fn ingest(
        &mut self,
        header: &Header<E>,
        body: Option<&Body>,
    ) -> Result<(), PublishError> {
        let header_bytes = header.to_bytes();

        self.stream_tx
            .send(ToStreamController::Ingest {
                header: header.to_owned(),
                body: body.cloned(),
                header_bytes,
            })
            .await
            // @TODO: Handle error.
            .unwrap();

        Ok(())
    }

    pub async fn publish_ephemeral(
        &mut self,
        topic: &T,
        payload: &[u8],
    ) -> Result<(), PublishError> {
        self.network_actor_tx
            .send(ToNodeActor::Broadcast {
                topic_id: topic.id(),
                bytes: payload.to_vec(),
            })
            .await
            // @TODO: Handle error.
            .unwrap();
        Ok(())
    }

    pub async fn publish_persisted(
        &mut self,
        topic: &T,
        header: &Header<E>,
        body: Option<&Body>,
    ) -> Result<Hash, PublishError> {
        let operation_id = header.hash();

        let bytes = encode_gossip_message(header, body)?;

        self.ingest(header, body)
            .await
            // @TODO: Handle error.
            .unwrap();

        self.network_actor_tx
            .send(ToNodeActor::Broadcast {
                topic_id: topic.id(),
                bytes,
            })
            .await
            // @TODO: Handle error.
            .unwrap();

        Ok(operation_id)
    }

    pub async fn subscribe_persisted(&self, topic: &T) -> Result<()> {
        self.network_actor_tx
            .send(ToNodeActor::SubscribePersisted {
                topic: topic.clone(),
            })
            .await?;
        Ok(())
    }

    pub async fn subscribe_ephemeral(&self, topic: &T) -> Result<()> {
        self.network_actor_tx
            .send(ToNodeActor::SubscribeEphemeral {
                topic: topic.clone(),
            })
            .await?;
        Ok(())
    }

    pub async fn upload_file(&self, path: PathBuf) -> Result<Hash, BlobError> {
        let progress = self.blobs.import_blob(path).await;
        pin!(progress);
        match progress.next().await {
            Some(ImportBlobEvent::Done(hash)) => Ok(hash),
            Some(ImportBlobEvent::Abort(err)) => Err(BlobError::Upload(anyhow!(err))),
            None => Err(BlobError::AbruptlyEnded),
        }
    }

    pub async fn read_file(&self, hash: Hash) -> Result<Option<impl AsyncSliceReader>, BlobError> {
        let file_handle = self
            .blobs
            .get(hash)
            .await
            .map_err(BlobError::InvalidFileHandle)?;
        match file_handle {
            Some(handle) => {
                if !handle.is_complete() {
                    return Ok(None);
                }
                let reader = handle.data_reader();
                Ok(Some(reader))
            }
            None => Ok(None),
        }
    }

    pub async fn sync_remote_file(&self, hash: Hash) -> Result<(), BlobError> {
        let progress = self.blobs.download_blob(hash).await;
        pin!(progress);
        match progress.next().await {
            Some(DownloadBlobEvent::Done) => Ok(()),
            Some(DownloadBlobEvent::Abort(err)) => Err(BlobError::Download(anyhow!(err))),
            None => Err(BlobError::AbruptlyEnded),
        }
    }
}

#[derive(Debug, Error)]
pub enum PublishError {
    #[error(transparent)]
    EncodeError(#[from] p2panda_core::cbor::EncodeError),
}

#[derive(Debug, Error)]
pub enum BlobError {
    #[error(transparent)]
    EncodeError(#[from] p2panda_core::cbor::EncodeError),

    #[error("could not get local blob: {0}")]
    InvalidFileHandle(anyhow::Error),

    #[error("upload or download ended abruptly due to unknown error")]
    AbruptlyEnded,

    #[error("blob download aborted due to error: {0}")]
    Download(anyhow::Error),

    #[error("blob upload aborted due to error: {0}")]
    Upload(anyhow::Error),
}

impl Serialize for PublishError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper to construct shared `AbortOnDropHandle` coming from tokio crate.
type JoinErrToStr = Box<dyn Fn(tokio::task::JoinError) -> String + Send + Sync + 'static>;
