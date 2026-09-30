//! SQLite transactions shared by the persistent examples.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::{Fault, Result};
pub use rusqlite;
use rusqlite::{Connection, Transaction, TransactionBehavior};

/// A connection shared within a process; SQLite coordinates other processes.
#[derive(Clone, Debug)]
pub struct Database(Arc<Mutex<Connection>>);

/// Keep storage diagnostics in the fault's source, not its public message.
pub fn sql(error: rusqlite::Error) -> Fault {
    Fault::internal("the store could not complete its transaction").with_source(error)
}

impl Database {
    /// Open a shared database file. Use a local filesystem supporting SQLite locks.
    /// New files are private to the current user on Unix; existing permissions stay.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        #[cfg(unix)]
        if path != Path::new(":memory:") {
            use std::fs::OpenOptions;
            use std::os::unix::fs::OpenOptionsExt;
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
            {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(Fault::internal("the store could not create its database")
                        .with_source(error));
                }
            }
        }
        Self::configure(Connection::open(path).map_err(sql)?)
    }

    /// An isolated database, shared by clones for tests and single-process use.
    pub fn memory() -> Result<Self> {
        Self::configure(Connection::open_in_memory().map_err(sql)?)
    }

    fn configure(connection: Connection) -> Result<Self> {
        connection
            .busy_timeout(Duration::from_millis(250))
            .map_err(sql)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sql)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(sql)?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    /// Run and commit an immediate transaction off the async runtime's threads.
    /// An error rolls back. Cancellation cannot interrupt an already running
    /// SQLite operation; callers use leases or durable states for recovery.
    pub async fn transact<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let database = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = database
                .lock()
                .map_err(|_| Fault::internal("store lock poisoned"))?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            let value = operation(&transaction)?;
            transaction.commit().map_err(sql)?;
            Ok(value)
        })
        .await
        .map_err(|error| Fault::internal("store task failed").with_source(error))?
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[tokio::test]
    async fn new_databases_are_private_and_existing_permissions_are_preserved() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "respan-private-store-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let io = |error| Fault::internal("store permission test failed").with_source(error);
        let database = Database::open(&path)?;
        database
            .transact(|tx| {
                tx.execute_batch("CREATE TABLE private(value TEXT)")
                    .map_err(sql)
            })
            .await?;
        assert_eq!(std::fs::metadata(&path).map_err(io)?.mode() & 0o777, 0o600);
        let wal = path.with_extension("db-wal");
        assert_eq!(std::fs::metadata(&wal).map_err(io)?.mode() & 0o777, 0o600);
        drop(database);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).map_err(io)?;
        drop(Database::open(&path)?);
        assert_eq!(std::fs::metadata(&path).map_err(io)?.mode() & 0o777, 0o640);
        std::fs::remove_file(path).map_err(io)?;
        Ok(())
    }
}
