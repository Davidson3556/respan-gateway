//! SQLite transactions shared by the persistent examples.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use respan_gateway_sdk::{Fault, Result};
pub use rusqlite;
use rusqlite::{Connection, ErrorCode, Transaction, TransactionBehavior};

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
        // Competing first opens can return BUSY immediately while one connection
        // switches the file to WAL, without using SQLite's busy timeout. Retry
        // only that startup operation, for no longer than the transaction wait.
        let until = Instant::now() + Duration::from_millis(250);
        loop {
            let remaining = until.saturating_duration_since(Instant::now());
            connection
                .busy_timeout(remaining.min(Duration::from_millis(25)))
                .map_err(sql)?;
            match connection.pragma_update(None, "journal_mode", "WAL") {
                Ok(()) => break,
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if matches!(
                        error.code,
                        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
                    ) && Instant::now() < until =>
                {
                    std::thread::sleep(
                        until
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(5)),
                    );
                }
                Err(error) => return Err(sql(error)),
            }
        }
        connection
            .busy_timeout(Duration::from_millis(250))
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn startup_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "respan-startup-store-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ))
    }

    fn assert_configured(database: &Database) -> Result<()> {
        let connection = database
            .0
            .lock()
            .map_err(|_| Fault::internal("store lock poisoned"))?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(sql)?;
        let wait: u64 = connection
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .map_err(sql)?;
        assert_eq!(mode, "wal");
        assert_eq!(wait, 250);
        Ok(())
    }

    #[test]
    fn concurrent_first_opens_all_initialize_wal() -> Result<()> {
        for _ in 0..60 {
            let path = startup_path();
            let barrier = std::sync::Barrier::new(8);
            let databases = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            Database::open(&path)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| Fault::internal("startup opener panicked"))?
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            for database in &databases {
                assert_configured(database)?;
            }
            drop(databases);
            std::fs::remove_file(path)
                .map_err(|error| Fault::internal("startup cleanup failed").with_source(error))?;
        }
        Ok(())
    }

    #[test]
    fn first_open_retries_a_transient_lock() -> Result<()> {
        let path = startup_path();
        let blocker = Connection::open(&path).map_err(sql)?;
        blocker
            .execute_batch("BEGIN EXCLUSIVE; CREATE TABLE startup(value INTEGER)")
            .map_err(sql)?;
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(70));
            blocker.execute_batch("COMMIT")
        });
        let database = Database::open(&path)?;
        release
            .join()
            .map_err(|_| Fault::internal("startup blocker panicked"))?
            .map_err(sql)?;
        assert_configured(&database)?;
        drop(database);
        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn first_open_fails_within_one_startup_budget_under_a_held_lock() -> Result<()> {
        let path = startup_path();
        let blocker = Connection::open(&path).map_err(sql)?;
        blocker
            .execute_batch("BEGIN EXCLUSIVE; CREATE TABLE startup(value INTEGER)")
            .map_err(sql)?;
        let start = Instant::now();
        let error = Database::open(&path).expect_err("the exclusive lock must block WAL startup");
        let source = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<rusqlite::Error>());
        assert!(matches!(
            source,
            Some(rusqlite::Error::SqliteFailure(error, _))
                if matches!(error.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        ));
        assert!(
            start.elapsed() >= Duration::from_millis(200)
                && start.elapsed() < Duration::from_millis(750),
            "startup retries exceeded the shared 250 ms budget: {error}"
        );
        blocker.execute_batch("ROLLBACK").map_err(sql)?;
        drop(blocker);
        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[cfg(unix)]
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
