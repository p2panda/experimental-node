use std::{collections::HashMap, path::PathBuf};

use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::log_sync::TopicLogMap;
use serde::Serialize;
use thiserror::Error;

use crate::{
    extensions::{LogId, NodeExtensions},
    node::Node,
    operation::create_operation,
    topic::{Topic, TopicMap},
};

pub struct NodeApi {
    pub node: Node<Topic, LogId, NodeExtensions>,
    pub topic_map: TopicMap,
    pub subscriptions: HashMap<[u8; 32], Topic>,
}

impl NodeApi {
    pub fn new(node: Node<Topic, LogId, NodeExtensions>) -> Self {
        Self {
            node,
            topic_map: TopicMap::new(),
            subscriptions: HashMap::default(),
        }
    }

    /// The public key of the local node.
    pub async fn public_key(&self) -> Result<PublicKey, ApiError> {
        let public_key = self.node.private_key.public_key();
        Ok(public_key)
    }

    /// Acknowledge operations to mark them as successfully processed in the stream controller.
    pub async fn ack(&mut self, operation_id: Hash) -> Result<(), ApiError> {
        self.node.ack(operation_id).await?;
        Ok(())
    }

    /// Replay any un-ack'd messages on a persisted topic.
    pub async fn replay(&mut self, topic: &str) -> Result<(), ApiError> {
        let topic = Topic::Persisted(topic.to_string());
        if let Some(logs) = self.topic_map.get(&topic).await {
            self.node.replay(logs).await?;
        };
        Ok(())
    }

    /// Add a persisted topic to the topic log map.
    pub async fn add_topic_log(
        &self,
        public_key: &PublicKey,
        topic: &str,
        log_id: &LogId,
    ) -> Result<(), ApiError> {
        let topic = Topic::Persisted(topic.to_string());
        self.topic_map.add_log(&topic, public_key, log_id).await;
        Ok(())
    }

    /// Subscribe to a persisted topic.
    pub async fn subscribe_persisted(&mut self, topic: &str) -> Result<(), ApiError> {
        let topic = Topic::Persisted(topic.to_string());
        return self.subscribe(&topic).await;
    }

    /// Subscribe to a ephemeral topic.
    pub async fn subscribe_ephemeral(&mut self, topic: &str) -> Result<(), ApiError> {
        let topic = Topic::Ephemeral(topic.to_string());
        return self.subscribe(&topic).await;
    }

    async fn subscribe(&mut self, topic: &Topic) -> Result<(), ApiError> {
        if self
            .subscriptions
            .insert(topic.id(), topic.clone())
            .is_some()
        {
            return Ok(());
        };

        match topic {
            Topic::Ephemeral(_) => {
                self.node
                    .subscribe_ephemeral(topic)
                    .await
                    .expect("can subscribe to topic");
            }
            Topic::Persisted(_) => {
                self.node
                    .subscribe_persisted(topic)
                    .await
                    .expect("can subscribe to topic");
            }
        }

        Ok(())
    }

    /// Publish to a persisted topic.
    pub async fn publish_persisted(
        &mut self,
        payload: &[u8],
        topic: Option<&str>,
        log_id: Option<&str>,
        prune_flag: bool,
    ) -> Result<Hash, ApiError> {
        let private_key = self.node.private_key.clone();

        let log_id = log_id.map(|log_id| LogId(log_id.to_string()));
        let extensions = NodeExtensions {
            log_id: log_id.clone(),
            prune_flag: prune_flag.into(),
        };

        let (header, body) = create_operation(
            &mut self.node.store,
            &private_key,
            log_id.as_ref(),
            extensions,
            Some(payload),
        )
        .await;

        match topic {
            Some(topic) => {
                let topic = Topic::Persisted(topic.to_string());
                self.node
                    .publish_persisted(&topic, &header, body.as_ref())
                    .await?;
            }
            None => {
                self.node.ingest(&header, body.as_ref()).await?;
            }
        }

        Ok(header.hash())
    }

    /// Publish to an ephemeral topic.
    pub async fn publish_ephemeral(&mut self, topic: &str, payload: &[u8]) -> Result<(), ApiError> {
        let topic = Topic::Ephemeral(topic.to_string());
        self.node.publish_ephemeral(&topic, payload).await?;
        Ok(())
    }

    /// Upload a file.
    pub async fn upload_file(&self, path: PathBuf) -> Result<Hash, ApiError> {
        let file_hash = self.node.upload_file(path).await?;
        Ok(file_hash)
    }
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    StreamController(#[from] crate::stream::StreamControllerError),

    #[error(transparent)]
    Publish(#[from] crate::node::PublishError),

    #[error(transparent)]
    Blob(#[from] crate::node::BlobError),
}

impl Serialize for ApiError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use p2panda_store::MemoryStore;

    use crate::api::NodeApi;

    use crate::extensions::{LogId, NodeExtensions};
    use crate::stream::EventData;
    use crate::topic::TopicMap;

    use super::Node;

    #[tokio::test]
    async fn publish() {
        let node_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node, mut stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map,
        )
        .await
        .unwrap();

        let mut node_api = NodeApi::new(node);

        let topic = "some_topic";
        let result = node_api.subscribe_persisted(&topic).await;
        assert!(result.is_ok());

        let log_id = "my_log";
        let payload = [0, 1, 2, 3];
        let result = node_api
            .publish_persisted(&payload, Some(&topic), Some(&log_id), false)
            .await;

        assert!(result.is_ok());
        let operation_hash = result.unwrap();

        let expected_log_id = LogId(log_id.to_string());
        let event = stream_rx.recv().await.unwrap();
        let header = event.header.unwrap();

        assert_eq!(header.public_key, node_private_key.public_key());
        assert_eq!(header.hash(), operation_hash);
        assert_eq!(header.extension(), Some(expected_log_id));

        let EventData::Application(value) = event.data else {
            panic!();
        };

        assert_eq!(value, payload);
    }

    // @TODO: bring back all tests.
    //
    //     #[tokio::test]
    //     async fn two_peers_subscribe() {
    //         let peer_a = Rpc {
    //             context: Service::run().await,
    //         };
    //         let peer_b = Rpc {
    //             context: Service::run().await,
    //         };
    //
    //         let (peer_a_tx, _peer_a_rx) = broadcast::channel(100);
    //         let (peer_b_tx, mut peer_b_rx) = broadcast::channel(100);
    //
    //         let result = peer_a.init(peer_a_tx).await;
    //         assert!(result.is_ok());
    //
    //         let result = peer_b.init(peer_b_tx).await;
    //         assert!(result.is_ok());
    //
    //         let topic = "some_topic";
    //         let result = peer_a.subscribe_ephemeral(&topic).await;
    //         assert!(result.is_ok());
    //
    //         let result = peer_b.subscribe_ephemeral(&topic).await;
    //         assert!(result.is_ok());
    //
    //         let send_payload = json!({
    //             "message": "organize!"
    //         });
    //
    //         {
    //             let send_payload = send_payload.clone();
    //             tokio::spawn(async move {
    //                 loop {
    //                     tokio::time::sleep(Duration::from_secs(1)).await;
    //                     let result = peer_a
    //                         .publish_ephemeral(&topic, &serde_json::to_vec(&send_payload).unwrap())
    //                         .await;
    //                     assert!(result.is_ok());
    //                 }
    //             });
    //         }
    //
    //         let mut message_received = false;
    //         while let Ok(event) = peer_b_rx.recv().await {
    //             if let ChannelEvent::Stream(StreamEvent {
    //                 data: EventData::Ephemeral(payload),
    //                 ..
    //             }) = event
    //             {
    //                 assert_eq!(send_payload, payload);
    //                 message_received = true;
    //                 break;
    //             }
    //         }
    //
    //         assert!(message_received);
    //     }
    //
    //     #[tokio::test]
    //     async fn two_peers_sync() {
    //         let peer_a = Rpc {
    //             context: Service::run().await,
    //         };
    //         let peer_b = Rpc {
    //             context: Service::run().await,
    //         };
    //
    //         let peer_a_public_key = peer_a.public_key().await.unwrap();
    //         let peer_b_public_key = peer_b.public_key().await.unwrap();
    //
    //         let (peer_a_tx, mut peer_a_rx) = broadcast::channel(100);
    //         let (peer_b_tx, mut peer_b_rx) = broadcast::channel(100);
    //
    //         let result = peer_a.init(peer_a_tx).await;
    //         assert!(result.is_ok());
    //
    //         let result = peer_b.init(peer_b_tx).await;
    //         assert!(result.is_ok());
    //
    //         let topic = "messages";
    //         let log_path = json!("messages").into();
    //         let stream_args = StreamArgs::default();
    //
    //         let peer_a_payload = json!({
    //             "message": "organize!"
    //         });
    //
    //         // Peer A publishes the first message to a new stream.
    //         let result = peer_a
    //             .publish_persisted(
    //                 &serde_json::to_vec(&peer_a_payload).unwrap(),
    //                 &stream_args,
    //                 Some(&log_path),
    //                 Some(&topic),
    //             )
    //             .await;
    //         assert!(result.is_ok());
    //
    //         // We need these values so Peer B can subscribe and publish to the correct stream.
    //         let (operation_id, stream_id) = result.unwrap();
    //
    //         let stream_args = StreamArgs {
    //             id: Some(stream_id),
    //             root_hash: Some(operation_id.clone()),
    //             owner: Some(peer_a_public_key.clone()),
    //         };
    //
    //         let peer_b_payload = json!({
    //             "message": "Hell yeah!"
    //         });
    //
    //         // Peer B publishes it's own message to the stream.
    //         let result = peer_b
    //             .publish_persisted(
    //                 &serde_json::to_vec(&peer_b_payload).unwrap(),
    //                 &stream_args,
    //                 Some(&log_path),
    //                 Some(&topic),
    //             )
    //             .await;
    //         assert!(result.is_ok());
    //
    //         // Both peers add themselves and each other to their topic map.
    //         let stream = Stream {
    //             root_hash: operation_id.into(),
    //             owner: peer_a_public_key.into(),
    //         };
    //         let log_id = LogId {
    //             stream,
    //             log_path: Some(log_path),
    //         };
    //
    //         peer_a
    //             .add_topic_log(&peer_a_public_key, &topic, &log_id)
    //             .await
    //             .unwrap();
    //         peer_a
    //             .add_topic_log(&peer_b_public_key, &topic, &log_id)
    //             .await
    //             .unwrap();
    //
    //         peer_b
    //             .add_topic_log(&peer_a_public_key, &topic, &log_id)
    //             .await
    //             .unwrap();
    //         peer_b
    //             .add_topic_log(&peer_b_public_key, &topic, &log_id)
    //             .await
    //             .unwrap();
    //
    //         // Finally they both subscribe to the topic.
    //         let result = peer_a.subscribe_persisted(&topic).await;
    //         assert!(result.is_ok());
    //         let result = peer_b.subscribe_persisted(&topic).await;
    //         assert!(result.is_ok());
    //
    //         // Peer A should receive Peer B's message via sync.
    //         let mut message_received = false;
    //         while let Ok(event) = peer_a_rx.recv().await {
    //             if let ChannelEvent::Stream(StreamEvent {
    //                 data: EventData::Application(payload),
    //                 meta: Some(EventMeta { author, .. }),
    //             }) = event
    //             {
    //                 if author == peer_b_public_key {
    //                     assert_eq!(peer_b_payload, payload);
    //                     message_received = true;
    //                     break;
    //                 }
    //             }
    //         }
    //
    //         assert!(message_received);
    //
    //         // Peer B should receive Peer A's message via sync.
    //         let mut message_received = false;
    //         while let Ok(event) = peer_b_rx.recv().await {
    //             if let ChannelEvent::Stream(StreamEvent {
    //                 data: EventData::Application(payload),
    //                 meta: Some(EventMeta { author, .. }),
    //             }) = event
    //             {
    //                 if author == peer_a_public_key {
    //                     assert_eq!(peer_a_payload, payload);
    //                     message_received = true;
    //                     break;
    //                 }
    //             }
    //         }
    //
    //         assert!(message_received);
    //     }
}
