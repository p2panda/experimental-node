// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite persistent storage.
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash as StdHash, Hasher};

use p2panda_store::OperationStore;
use sqlx::migrate;
use sqlx::migrate::{MigrateDatabase, MigrateError};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::{Error as SqlxError, Sqlite};
use thiserror::Error;

use p2panda_core::cbor::{DecodeError, EncodeError};
use p2panda_core::{Extension, Hash, PublicKey};

use crate::stream::StreamControllerError;

use super::{Operation, StreamControllerStore};

#[derive(Debug, Error)]
pub enum SqliteStoreError {
    #[error("an error occurred with the sqlite database: {0}")]
    Database(#[from] SqlxError),
}

impl From<MigrateError> for SqliteStoreError {
    fn from(error: MigrateError) -> Self {
        Self::Database(SqlxError::Migrate(Box::new(error)))
    }
}

/// Re-export of SQLite connection pool type.
pub type Pool = SqlitePool;

/// SQLite-based persistent store.
#[derive(Clone, Debug)]
pub struct StreamSqliteStore<L, E = ()> {
    operation_store: p2panda_store::SqliteStore<L, E>,

    /// Log-height of latest ack per log.
    pool: Pool,
}

impl<L, E> StreamSqliteStore<L, E> {
    /// Create a new `SqliteStore` using the provided db `Pool`.
    pub fn new(pool: Pool, operation_store: p2panda_store::SqliteStore<L, E>) -> Self {
        Self {
            pool,
            operation_store,
        }
    }
}

/// Create the database if it doesn't already exist.
pub async fn create_database(url: &str) -> Result<(), SqliteStoreError> {
    if !Sqlite::database_exists(url).await? {
        Sqlite::create_database(url).await?
    }

    Ok(())
}

/// Drop the database if it exists.
pub async fn drop_database(url: &str) -> Result<(), SqliteStoreError> {
    if Sqlite::database_exists(url).await? {
        Sqlite::drop_database(url).await?
    }

    Ok(())
}

/// Create a connection pool.
pub async fn connection_pool(url: &str, max_connections: u32) -> Result<Pool, SqliteStoreError> {
    let pool: Pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;

    Ok(pool)
}

/// Run any pending database migrations from inside the application.
pub async fn run_pending_migrations(pool: &Pool) -> Result<(), SqliteStoreError> {
    migrate!().run(pool).await?;

    Ok(())
}

fn calculate_hash<T: StdHash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl<L, E> StreamControllerStore<L, E> for StreamSqliteStore<L, E>
where
    L: p2panda_store::LogId + Send + Sync,
    E: p2panda_core::Extensions + Extension<L> + Send + Sync,
{
    type Error = StreamControllerError;

    async fn ack(&self, operation_id: Hash) -> Result<(), Self::Error> {
        let Ok(Some((header, _))) = self.operation_store.get_operation(operation_id).await else {
            return Err(StreamControllerError::AckedUnknownOperation(operation_id));
        };

        todo!();
        //         let mut acked = self.acked.write().await;
        //
        //         let log_id: Option<L> = header.extension();
        //         let Some(log_id) = log_id else {
        //             return Err(StreamControllerError::MissingLogId(operation_id));
        //         };
        //
        //         // Remember the "acknowledged" log-height for this log.
        //         acked.insert((header.public_key, log_id), header.seq_num);

        Ok(())
    }

    async fn unacked(
        &self,
        logs: HashMap<PublicKey, Vec<L>>,
    ) -> Result<Vec<Operation<E>>, Self::Error> {
        todo!();

        //         let acked = self.acked.read().await;
        //
        //         let mut result = Vec::new();
        //         for (public_key, log_ids) in logs {
        //             for log_id in log_ids {
        //                 match acked.get(&(public_key, log_id.clone())) {
        //                     Some(ack_log_height) => {
        //                         let Ok(operations) = self
        //                             .operation_store
        //                             // Get all operations from > ack_log_height
        //                             .get_log(&public_key, &log_id, Some(*ack_log_height + 1))
        //                             .await;
        //
        //                         if let Some(operations) = operations {
        //                             for (header, body) in operations {
        //                                 // @TODO(adz): Getting the encoded header bytes through encoding
        //                                 // like this feels redundant and should be possible to retreive
        //                                 // just from calling "get_log".
        //                                 let header_bytes = header.to_bytes();
        //                                 result.push((header, body, header_bytes));
        //                             }
        //                         }
        //                     }
        //                     None => {
        //                         let Ok(operations) = self
        //                             .operation_store
        //                             // Get all operations from > ack_log_height
        //                             .get_log(&public_key, &log_id, Some(0))
        //                             .await;
        //
        //                         if let Some(operations) = operations {
        //                             for (header, body) in operations {
        //                                 // @TODO(adz): Getting the encoded header bytes through encoding
        //                                 // like this feels redundant and should be possible to retreive
        //                                 // just from calling "get_log".
        //                                 let header_bytes = header.to_bytes();
        //                                 result.push((header, body, header_bytes));
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //
        //         Ok(result)
    }
}
