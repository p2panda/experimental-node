use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use p2panda_core::{Body, Extension, Extensions, Hash, Header, PruneFlag, PublicKey};
use p2panda_store::{LogStore, MemoryStore, OperationStore};
use p2panda_stream::IngestExt;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

// use super::extensions::{Extensions, LogId, LogPath, Stream, StreamOwner, StreamRootHash};

#[allow(clippy::large_enum_variant, dead_code)]
pub enum ToStreamController<L, E> {
    Ephemeral {
        bytes: Vec<u8>,
    },
    Ingest {
        header: Header<E>,
        body: Option<Body>,
        header_bytes: Vec<u8>,
    },
    Ack {
        operation_id: Hash,
        reply: oneshot::Sender<Result<(), StreamControllerError>>,
    },
    Replay {
        logs: HashMap<PublicKey, Vec<L>>,
        reply: oneshot::Sender<Result<(), StreamControllerError>>,
    },
}

#[allow(dead_code)]
pub struct StreamController<L, E> {
    controller_store: StreamMemoryStore<L, E>,
    operation_store: MemoryStore<L, E>,
    processor_handle: JoinHandle<()>,
}

pub type StreamReturn<L, E> = (
    StreamController<L, E>,
    mpsc::Sender<ToStreamController<L, E>>,
    mpsc::Receiver<StreamEvent<E>>,
);

impl<L, E> StreamController<L, E>
where
    L: p2panda_store::LogId + Send + Sync + 'static,
    E: Extensions + Extension<L> + Extension<PruneFlag> + Send + Sync + 'static,
{
    pub fn new(operation_store: MemoryStore<L, E>) -> StreamReturn<L, E> {
        let rt = tokio::runtime::Handle::current();

        let controller_store = StreamMemoryStore::new(operation_store.clone());

        let (app_tx, app_rx) = mpsc::channel(1024);

        let (processor_tx, processor_rx) = mpsc::channel(1024);
        let processor_rx = ReceiverStream::new(processor_rx);

        let (stream_tx, mut stream_rx) = mpsc::channel(1024);

        {
            let controller_store = controller_store.clone();
            let app_tx = app_tx.clone();

            rt.spawn(async move {
                loop {
                    match stream_rx.recv().await {
                        Some(ToStreamController::Ephemeral { bytes }) => {
                            app_tx
                                .send(StreamEvent::from_bytes(bytes))
                                .await
                                .expect("send on app_tx");
                        }
                        Some(ToStreamController::Ingest {
                            header,
                            body,
                            header_bytes,
                        }) => processor_tx
                            .send((header, body, header_bytes))
                            .await
                            .expect("send processor_tx"),
                        Some(ToStreamController::Ack {
                            operation_id,
                            reply,
                        }) => {
                            let result = controller_store.ack(operation_id).await;
                            reply.send(result).ok();
                        }
                        Some(ToStreamController::Replay { logs, reply }) => {
                            match controller_store.unacked(logs).await {
                                Ok(operations) => {
                                    for operation in operations {
                                        debug!("send operation: {}", &operation.0.hash());
                                        processor_tx
                                            .send(operation)
                                            .await
                                            .expect("send processor_tx");
                                    }
                                    reply.send(Ok(())).ok();
                                }
                                Err(err) => {
                                    reply.send(Err(err)).ok();
                                }
                            }
                        }
                        None => break,
                    }
                }
            });
        }

        let processor_handle = {
            let operation_store = operation_store.clone();

            rt.spawn(async move {
                let mut processor =
                    processor_rx
                        .ingest(operation_store, 512)
                        .filter_map(|result| match result {
                            Ok(operation) => Some(operation),
                            Err(_err) => {
                                // @TODO(adz): Which errors do we want to report to the application and
                                // which not? It might become pretty spammy in some cases and I'm not sure
                                // if the frontend can do anything about it?

                                // app_tx
                                //     .blocking_send(StreamEvent::from_error(
                                //         StreamError::IngestError(err),
                                //         // @TODO: We should be able to get the operation causing the error.
                                //         result.header,
                                //     ))
                                //     .expect("app_tx send");
                                None
                            }
                        });

                loop {
                    match processor.next().await {
                        Some(operation) => {
                            // If the operation has a body we want to forward it to the application
                            // layer as it might contain more information relevant for it.
                            if let Some(body) = operation.body {
                                app_tx
                                    .send(StreamEvent::from_operation(operation.header, body))
                                    .await
                                    .expect("app_tx send");
                            }
                        }
                        None => {
                            // @TODO(adz): Panicking here probably doesn't make any sense, I'll keep it
                            // here until I understand the error handling better.
                            panic!("processor stream ended");
                        }
                    }
                }
            })
        };

        (
            Self {
                controller_store,
                operation_store,
                processor_handle,
            },
            stream_tx,
            app_rx,
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamEvent<E> {
    pub header: Option<Header<E>>,
    pub data: EventData,
}

impl<E> StreamEvent<E> {
    pub fn from_operation(header: Header<E>, body: Body) -> Self {
        Self {
            header: Some(header),
            data: EventData::Application(body.to_bytes()),
        }
    }

    pub fn from_bytes(payload: Vec<u8>) -> Self {
        Self {
            header: None,
            data: EventData::Ephemeral(payload),
        }
    }

    #[allow(dead_code)]
    pub fn from_error(error: StreamError, header: Header<E>) -> Self {
        Self {
            header: Some(header),
            data: EventData::Error(error),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum EventData {
    Application(Vec<u8>),
    Ephemeral(Vec<u8>),
    Error(StreamError),
}

#[allow(dead_code)]
#[derive(Clone, Debug, Error)]
pub enum StreamError {
    #[error(transparent)]
    IngestError(p2panda_stream::operation::IngestError),
}

impl PartialEq for StreamError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Serialize for StreamError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, Error)]
pub enum StreamControllerError {
    #[error("tried do ack unknown operation {0}")]
    AckedUnknownOperation(Hash),

    #[error("can't extract log id from operation {0}")]
    MissingLogId(Hash),
}

impl Serialize for StreamControllerError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

type Operation<E> = (Header<E>, Option<Body>, Vec<u8>);

trait StreamControllerStore<L, E>
where
    E: p2panda_core::Extensions,
{
    type Error;

    /// Mark operation as acknowledged.
    fn ack(&self, operation_id: Hash) -> impl Future<Output = Result<(), Self::Error>>;

    /// Return all operations from given logs which have not yet been acknowledged.
    fn unacked(
        &self,
        logs: HashMap<PublicKey, Vec<L>>,
    ) -> impl Future<Output = Result<Vec<Operation<E>>, Self::Error>>;
}

#[derive(Clone, Debug)]
struct StreamMemoryStore<L, E = ()> {
    operation_store: MemoryStore<L, E>,

    /// Log-height of latest ack per log.
    acked: Arc<RwLock<HashMap<(PublicKey, L), u64>>>,
}

impl<L, E> StreamMemoryStore<L, E> {
    pub fn new(operation_store: MemoryStore<L, E>) -> Self {
        Self {
            operation_store,
            acked: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<L, E> StreamControllerStore<L, E> for StreamMemoryStore<L, E>
where
    L: p2panda_store::LogId + Send + Sync,
    E: p2panda_core::Extensions + Extension<L> + Send + Sync,
{
    type Error = StreamControllerError;

    async fn ack(&self, operation_id: Hash) -> Result<(), Self::Error> {
        let Ok(Some((header, _))) = self.operation_store.get_operation(operation_id).await else {
            return Err(StreamControllerError::AckedUnknownOperation(operation_id));
        };

        let mut acked = self.acked.write().await;

        let log_id: Option<L> = header.extension();
        let Some(log_id) = log_id else {
            return Err(StreamControllerError::MissingLogId(operation_id));
        };

        // Remember the "acknowledged" log-height for this log.
        acked.insert((header.public_key, log_id), header.seq_num);

        Ok(())
    }

    async fn unacked(
        &self,
        logs: HashMap<PublicKey, Vec<L>>,
    ) -> Result<Vec<Operation<E>>, Self::Error> {
        let acked = self.acked.read().await;

        let mut result = Vec::new();
        for (public_key, log_ids) in logs {
            for log_id in log_ids {
                match acked.get(&(public_key, log_id.clone())) {
                    Some(ack_log_height) => {
                        let Ok(operations) = self
                            .operation_store
                            // Get all operations from > ack_log_height
                            .get_log(&public_key, &log_id, Some(*ack_log_height + 1))
                            .await;

                        if let Some(operations) = operations {
                            for (header, body) in operations {
                                // @TODO(adz): Getting the encoded header bytes through encoding
                                // like this feels redundant and should be possible to retreive
                                // just from calling "get_log".
                                let header_bytes = header.to_bytes();
                                result.push((header, body, header_bytes));
                            }
                        }
                    }
                    None => {
                        let Ok(operations) = self
                            .operation_store
                            // Get all operations from > ack_log_height
                            .get_log(&public_key, &log_id, Some(0))
                            .await;

                        if let Some(operations) = operations {
                            for (header, body) in operations {
                                // @TODO(adz): Getting the encoded header bytes through encoding
                                // like this feels redundant and should be possible to retreive
                                // just from calling "get_log".
                                let header_bytes = header.to_bytes();
                                result.push((header, body, header_bytes));
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures_util::FutureExt;
    use p2panda_core::{Body, Hash, Header, PrivateKey, PruneFlag};
    use p2panda_store::MemoryStore;
    use tokio::sync::oneshot;

    use crate::extensions::{LogId, NodeExtensions};
    use crate::operation::{self};
    use crate::stream::StreamEvent;

    use super::{StreamController, ToStreamController};

    async fn create_operation(
        operation_store: &mut MemoryStore<LogId, NodeExtensions>,
        private_key: &PrivateKey,
        log_id: Option<LogId>,
    ) -> (Header<NodeExtensions>, Body, Vec<u8>, Hash) {
        let extensions = NodeExtensions {
            log_id: log_id.clone(),
            prune_flag: PruneFlag::default(),
        };

        let (header, body) = operation::create_operation(
            operation_store,
            private_key,
            log_id.as_ref(),
            extensions.clone(),
            Some(&[0, 1, 2, 3]),
        )
        .await;
        let header_bytes = header.to_bytes();
        let operation_id = header.hash();

        (header, body.unwrap(), header_bytes, operation_id)
    }

    #[tokio::test]
    async fn replay_unacked_operations() {
        let mut operation_store = MemoryStore::new();

        let (_controller, tx, mut rx) =
            StreamController::<LogId, NodeExtensions>::new(operation_store.clone());

        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();

        // Create and ingest operation 0.
        let (header_0, body_0, header_bytes_0, operation_id_0) =
            create_operation(&mut operation_store, &private_key, None).await;

        tx.send(ToStreamController::Ingest {
            header: header_0.clone(),
            body: Some(body_0.clone()),
            header_bytes: header_bytes_0,
        })
        .await
        .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            StreamEvent::from_operation(header_0.clone(), body_0)
        );

        // Acknowledge operation 0.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Ack {
            operation_id: operation_id_0,
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());

        // Ask to replay log, but don't expect anything to be sent.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Replay {
            logs: HashMap::from([(public_key, vec![header_0.extension().unwrap()])]),
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());
        assert_eq!(rx.recv().now_or_never(), None);

        // Create and ingest operation 1.
        let (header_1, body_1, header_bytes_1, operation_id_1) =
            create_operation(&mut operation_store, &private_key, header_0.extension()).await;

        tx.send(ToStreamController::Ingest {
            header: header_1.clone(),
            body: Some(body_1.clone()),
            header_bytes: header_bytes_1,
        })
        .await
        .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            StreamEvent::from_operation(header_1.clone(), body_1.clone())
        );

        // Create and ingest operation 2.
        let (header_2, body_2, header_bytes_2, operation_id_2) =
            create_operation(&mut operation_store, &private_key, header_0.extension()).await;

        tx.send(ToStreamController::Ingest {
            header: header_2.clone(),
            body: Some(body_2.clone()),
            header_bytes: header_bytes_2,
        })
        .await
        .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            StreamEvent::from_operation(header_2.clone(), body_2.clone())
        );

        // Ask to replay log, expect operation 1 and 2 to be sent again.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Replay {
            logs: HashMap::from([(public_key, vec![header_0.extension().unwrap()])]),
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());
        assert_eq!(
            rx.recv().await.unwrap(),
            StreamEvent::from_operation(header_1, body_1)
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            StreamEvent::from_operation(header_2, body_2)
        );

        // Acknowledge operation 1 and 2.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Ack {
            operation_id: operation_id_1,
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Ack {
            operation_id: operation_id_2,
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());

        // Ask to replay log, but don't expect anything to be sent.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ToStreamController::Replay {
            logs: HashMap::from([(public_key, vec![header_0.extension().unwrap()])]),
            reply,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.is_ok());
        assert_eq!(rx.recv().now_or_never(), None);
    }
}
