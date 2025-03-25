use std::{collections::HashMap, path::PathBuf};

use p2panda_core::{Extension, Extensions, Hash, PruneFlag, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::log_sync::TopicLogMap;
use serde::Serialize;
use thiserror::Error;

use crate::{
    extensions::LogId,
    node::Node,
    operation::create_operation,
    topic::{Topic, TopicMap},
};

pub struct NodeApi<E>
where
    E:,
{
    pub node: Node<Topic, LogId, E>,
    pub topic_map: TopicMap,
    pub subscriptions: HashMap<[u8; 32], Topic>,
}

impl<E> NodeApi<E>
where
    E: Extensions + Extension<LogId> + Extension<PruneFlag> + Send + Sync + 'static,
{
    pub fn new(node: Node<Topic, LogId, E>, topic_map: TopicMap) -> Self {
        Self {
            node,
            topic_map,
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
        log_id: &str,
    ) -> Result<(), ApiError> {
        let topic = Topic::Persisted(topic.to_string());
        let log_id = LogId(log_id.to_string());
        self.topic_map.add_log(&topic, public_key, &log_id).await;
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
        topic: Option<&str>,
        payload: &[u8],
        log_id: Option<&str>,
        extensions: Option<E>,
    ) -> Result<Hash, ApiError> {
        let private_key = self.node.private_key.clone();

        let log_id = log_id.map(|log_id| LogId(log_id.to_string()));
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
    use crate::stream::{EventData, StreamEvent};
    use crate::topic::TopicMap;

    use super::Node;

    #[tokio::test]
    async fn subscribe_publish_persisted() {
        let private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node, mut stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();

        let mut node_api = NodeApi::new(node, topic_map);

        let topic = "some_topic";
        let result = node_api.subscribe_persisted(&topic).await;
        assert!(result.is_ok());

        let payload = [0, 1, 2, 3];
        let extensions = NodeExtensions::default();
        let result = node_api
            .publish_persisted(
                Some(&topic),
                &payload,
                Some(&private_key.public_key().to_hex()),
                Some(extensions),
            )
            .await;

        assert!(result.is_ok());
        let operation_hash = result.unwrap();

        let expected_log_id = LogId(private_key.public_key().to_hex());
        let event = stream_rx.recv().await.unwrap();
        let header = event.header.unwrap();

        assert_eq!(header.public_key, private_key.public_key());
        assert_eq!(header.hash(), operation_hash);
        assert_eq!(header.extension(), Some(expected_log_id));

        let EventData::Application(value) = event.data else {
            panic!();
        };

        assert_eq!(value, payload);
    }

    #[tokio::test]
    async fn subscribe_publish_ephemeral() {
        let node_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node, _stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();
        let mut node_api = NodeApi::new(node, topic_map);

        let topic = "some_topic";
        let result = node_api.subscribe_ephemeral(&topic).await;
        assert!(result.is_ok());

        let payload = [0, 1, 2, 3];
        let result = node_api.publish_ephemeral(&topic, &payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn two_peers_subscribe() {
        let node_a_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node_a, _node_a_stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_a_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();
        let mut node_a_api = NodeApi::new(node_a, topic_map);

        let node_b_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node_b, mut node_b_stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_b_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();
        let mut node_b_api = NodeApi::new(node_b, topic_map);

        let topic = "some_topic";
        let result = node_a_api.subscribe_ephemeral(&topic).await;
        assert!(result.is_ok());

        let result = node_b_api.subscribe_ephemeral(&topic).await;
        assert!(result.is_ok());

        let send_payload = [0, 1, 2, 3];

        {
            let send_payload = send_payload.clone();
            tokio::spawn(async move {
                loop {
                    let result = node_a_api.publish_ephemeral(&topic, &send_payload).await;
                    assert!(result.is_ok());
                }
            });
        }

        let mut message_received = false;
        while let Some(event) = node_b_stream_rx.recv().await {
            let StreamEvent {
                data: EventData::Ephemeral(payload),
                ..
            } = event
            else {
                panic!()
            };
            {
                assert_eq!(send_payload.to_vec(), payload);
                message_received = true;
                break;
            }
        }

        assert!(message_received);
    }

    #[tokio::test]
    async fn two_peers_sync() {
        let node_a_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node_a, mut node_a_stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_a_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();
        let mut node_a_api = NodeApi::new(node_a, topic_map);

        let node_b_private_key = PrivateKey::new();
        let store = MemoryStore::<LogId, NodeExtensions>::new();
        let blobs_root_dir = tempfile::tempdir().unwrap().into_path();
        let topic_map = TopicMap::new();
        let (node_b, mut node_b_stream_rx, _system_rx) = Node::new(
            "my_network".to_string(),
            node_b_private_key.clone(),
            None,
            None,
            store,
            blobs_root_dir,
            topic_map.clone(),
        )
        .await
        .unwrap();
        let mut node_b_api = NodeApi::new(node_b, topic_map);

        let topic = "messages";
        let log_id = "messages";
        let node_a_payload = [0, 1, 2, 3];

        // Peer A publishes the first message to a new topic.
        let extensions = NodeExtensions {
            log_id: Some(LogId(log_id.to_string())),
            ..Default::default()
        };
        let result: Result<p2panda_core::Hash, crate::api::ApiError> = node_a_api
            .publish_persisted(
                Some(&topic),
                &node_a_payload,
                Some(&log_id),
                Some(extensions.clone()),
            )
            .await;
        assert!(result.is_ok());

        // Peer B publishes it's own message to the topic.
        let node_b_payload = [5, 6, 7, 8];
        let result = node_b_api
            .publish_persisted(
                Some(&topic),
                &node_b_payload,
                Some(&log_id),
                Some(extensions),
            )
            .await;
        assert!(result.is_ok());

        // Both peers add themselves and each other to their topic map.
        node_a_api
            .add_topic_log(&node_a_private_key.public_key(), &topic, &log_id)
            .await
            .unwrap();
        node_a_api
            .add_topic_log(&node_b_private_key.public_key(), &topic, &log_id)
            .await
            .unwrap();
        node_b_api
            .add_topic_log(&node_a_private_key.public_key(), &topic, &log_id)
            .await
            .unwrap();
        node_b_api
            .add_topic_log(&node_b_private_key.public_key(), &topic, &log_id)
            .await
            .unwrap();

        let result = node_a_api.subscribe_persisted(&topic).await;
        assert!(result.is_ok());
        let result = node_b_api.subscribe_persisted(&topic).await;
        assert!(result.is_ok());

        // Peer A should receive Peer B's message via sync.
        let mut message_received = false;
        while let Some(event) = node_a_stream_rx.recv().await {
            println!("{event:?}");
            let StreamEvent {
                data: EventData::Application(payload),
                header: Some(header),
            } = event
            else {
                panic!()
            };
            {
                if header.public_key == node_b_private_key.public_key() {
                    assert_eq!(node_b_payload.to_vec(), payload);
                    message_received = true;
                    break;
                }
            }
        }

        assert!(message_received);

        // Peer B should receive Peer A's message via sync.
        let mut message_received = false;
        while let Some(event) = node_b_stream_rx.recv().await {
            println!("{event:?}");
            let StreamEvent {
                data: EventData::Application(payload),
                header: Some(header),
            } = event
            else {
                panic!()
            };
            {
                if header.public_key == node_a_private_key.public_key() {
                    assert_eq!(node_a_payload.to_vec(), payload);
                    message_received = true;
                    break;
                }
            }
        }

        assert!(message_received);
    }
}
