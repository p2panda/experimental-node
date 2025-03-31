mod controller;
mod store;

pub use controller::*;
pub use store::memory::StreamMemoryStore;

use p2panda_core::{Body, Header};
use serde::Serialize;
use thiserror::Error;

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
