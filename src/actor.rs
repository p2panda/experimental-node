use std::collections::HashMap;

use anyhow::{Result, anyhow};
use futures_util::stream::SelectAll;
use p2panda_core::{Body, Extensions, Header, cbor::decode_cbor};
use p2panda_net::{FromNetwork, Network, ToNetwork, TopicId};
use p2panda_sync::TopicQuery;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

use super::operation::decode_gossip_message;

pub enum ToNodeActor<T> {
    SubscribePersisted {
        topic: T,
    },
    SubscribeEphemeral {
        topic: T,
    },
    Broadcast {
        topic_id: [u8; 32],
        bytes: Vec<u8>,
    },
    #[allow(dead_code)]
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NodeActor<T, E> {
    pub network: Network<T>,
    inbox: mpsc::Receiver<ToNodeActor<T>>,
    topic_persisted_rx: SelectAll<ReceiverStream<FromNetwork>>,
    topic_ephemeral_rx: SelectAll<ReceiverStream<FromNetwork>>,
    topic_tx: HashMap<[u8; 32], mpsc::Sender<ToNetwork>>,
    stream_processor_tx: mpsc::Sender<(Header<E>, Option<Body>, Vec<u8>)>,
    ephemeral_tx: mpsc::Sender<Vec<u8>>,
}

impl<T, E> NodeActor<T, E>
where
    T: TopicId + TopicQuery + 'static,
    E: Extensions + Send + Sync + 'static,
{
    pub fn new(
        network: Network<T>,
        stream_processor_tx: mpsc::Sender<(Header<E>, Option<Body>, Vec<u8>)>,
        ephemeral_tx: mpsc::Sender<Vec<u8>>,
        inbox: mpsc::Receiver<ToNodeActor<T>>,
    ) -> Self {
        Self {
            network,
            inbox,
            topic_persisted_rx: SelectAll::new(),
            topic_ephemeral_rx: SelectAll::new(),
            topic_tx: HashMap::new(),
            stream_processor_tx,
            ephemeral_tx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal.
        let shutdown_completed_signal = self.run_inner().await;

        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                Some(event) = self.topic_persisted_rx.next() => {
                    self.on_network_event_persisted(event).await?;
                },
                Some(event) = self.topic_ephemeral_rx.next() => {
                    self.on_network_event_ephemeral(event).await?;
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor<T>) -> Result<()> {
        match msg {
            ToNodeActor::SubscribePersisted { topic } => {
                let topic_id = topic.id();
                let (topic_tx, topic_rx, _ready) = self.network.subscribe(topic).await?;
                self.topic_tx.insert(topic_id, topic_tx);
                self.topic_persisted_rx.push(ReceiverStream::new(topic_rx));
            }
            ToNodeActor::SubscribeEphemeral { topic } => {
                let topic_id = topic.id();
                let (topic_tx, topic_rx, _ready) = self.network.subscribe(topic).await?;
                self.topic_tx.insert(topic_id, topic_tx.clone());
                self.topic_ephemeral_rx.push(ReceiverStream::new(topic_rx));
            }
            ToNodeActor::Broadcast { topic_id, bytes } => match self.topic_tx.get(&topic_id) {
                Some(tx) => {
                    if let Err(err) = tx.send(ToNetwork::Message { bytes }).await {
                        error!("error sending on topic channel: {}", err)
                        // @TODO: Handle error
                    }
                }
                None => {
                    debug!("attempted to broadcast on unknown topic: {:?}", topic_id);
                    // @TODO: Can we ignore this?
                }
            },
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_network_event_persisted(&mut self, event: FromNetwork) -> Result<()> {
        let (header_bytes, body_bytes) = match event {
            FromNetwork::GossipMessage { bytes, .. } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                match decode_gossip_message(&bytes) {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Error decoding gossip message: {err}");
                        return Ok(());
                    }
                }
            }
            FromNetwork::SyncMessage {
                header: header_bytes,
                payload: body_bytes,
                ..
            } => {
                trace!(
                    source = "sync",
                    bytes = header_bytes.len() + body_bytes.as_ref().map_or(0, |b| b.len()),
                    "received network message"
                );
                (header_bytes, body_bytes)
            }
        };

        let header = match decode_cbor(&header_bytes[..]) {
            Ok(header) => header,
            Err(err) => {
                warn!("failed decoding operation header: {err}");
                return Ok(());
            }
        };

        let body = body_bytes.map(Body::from);
        self.stream_processor_tx
            .send((header, body, header_bytes))
            .await?;

        Ok(())
    }

    async fn on_network_event_ephemeral(&mut self, event: FromNetwork) -> Result<()> {
        let bytes = match event {
            FromNetwork::GossipMessage { bytes, .. } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                bytes
            }
            FromNetwork::SyncMessage {
                header: header_bytes,
                payload: body_bytes,
                ..
            } => {
                trace!(
                    source = "sync",
                    bytes = header_bytes.len() + body_bytes.as_ref().map_or(0, |b| b.len()),
                    "received network message"
                );
                error!("unexpected ephemeral message received via sync");
                return Ok(());
            }
        };

        self.ephemeral_tx.send(bytes).await?;

        Ok(())
    }

    // @TODO: Send shutdown command.
    async fn shutdown(self) -> Result<()> {
        self.network.shutdown().await?;
        Ok(())
    }
}
