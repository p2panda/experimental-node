use std::hash::Hash as StdHash;

use p2panda_core::{Extension, Header, PruneFlag};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, StdHash, Serialize, Deserialize)]
pub struct LogId(pub String);

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeExtensions {
    #[serde(rename = "l")]
    pub log_id: Option<LogId>,

    #[serde(
        rename = "p",
        skip_serializing_if = "PruneFlag::is_not_set",
        default = "PruneFlag::default"
    )]
    pub prune_flag: PruneFlag,
}

impl Extension<LogId> for NodeExtensions {
    fn extract(header: &Header<Self>) -> Option<LogId> {
        if let Some(extensions) = header.extensions.as_ref() {
            if let Some(ref log_id) = extensions.log_id {
                return Some(log_id.to_owned());
            }
        };
        Some(LogId(header.public_key.to_hex()))
    }
}

impl Extension<PruneFlag> for NodeExtensions {
    fn extract(header: &Header<Self>) -> Option<PruneFlag> {
        header
            .extensions
            .as_ref()
            .map(|extensions| extensions.prune_flag.clone())
    }
}
