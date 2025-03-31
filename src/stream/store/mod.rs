use std::collections::HashMap;
use std::future::Future;

pub mod memory;

use p2panda_core::{Body, Hash, Header, PublicKey};

type Operation<E> = (Header<E>, Option<Body>, Vec<u8>);

pub trait StreamControllerStore<L, E>
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
