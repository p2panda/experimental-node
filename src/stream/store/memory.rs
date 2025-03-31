use std::collections::HashMap;
use std::sync::Arc;

use p2panda_core::{Extension, Hash, PublicKey};
use p2panda_store::{LogStore, MemoryStore, OperationStore};
use tokio::sync::RwLock;

use crate::stream::StreamControllerError;

use super::{Operation, StreamControllerStore};

#[derive(Clone, Debug)]
pub struct StreamMemoryStore<L, E = ()> {
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
