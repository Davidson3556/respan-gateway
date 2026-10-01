//! Weighted admission shared by processes using one SQLite database file.
//!
//! A lease expires after a crashed worker stops renewing. A live worker must
//! stop its work when `Slot::lost` resolves; this happens before the database
//! reuses its capacity. This bounds local work, not computation already sent
//! to an upstream. Processes need a responsive runtime and the same host clock.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use respan_gateway_sdk::{Fault, Result};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::store::{Database, rusqlite::OptionalExtension, sql};

const POLL: Duration = Duration::from_millis(10);
const CLEAN_USERS: &str = "DELETE FROM admission_users WHERE NOT EXISTS (
    SELECT 1 FROM admission_waiters w WHERE w.priority=admission_users.priority
    AND w.user=admission_users.user AND w.expires IS NULL)";

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS admission_config (
    id INTEGER PRIMARY KEY CHECK(id=1), capacity INTEGER NOT NULL, lease_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS admission_waiters (
    id INTEGER PRIMARY KEY AUTOINCREMENT, priority INTEGER NOT NULL,
    user TEXT NOT NULL, deadline INTEGER NOT NULL, expires INTEGER);
CREATE TABLE IF NOT EXISTS admission_users (
    priority INTEGER NOT NULL, user TEXT NOT NULL, weight INTEGER NOT NULL,
    remaining INTEGER NOT NULL, turn INTEGER NOT NULL, PRIMARY KEY(priority,user));
";

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn milliseconds(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

/// A weighted queue. Clones share a connection; `open` shares across processes.
#[derive(Clone, Debug)]
pub struct Slots {
    database: Database,
    capacity: usize,
    lease: Duration,
    weights: Arc<BTreeMap<String, u32>>,
}

impl Slots {
    /// An in-memory queue, with a three-second renewable lease.
    pub fn new(capacity: usize) -> Result<Self> {
        Self::of(Database::memory()?, capacity, Duration::from_secs(3))
    }

    /// Open a queue shared with other processes on this host. All callers
    /// must configure the same capacity and lease, and the same user weights.
    pub fn open(path: impl AsRef<Path>, capacity: usize, lease: Duration) -> Result<Self> {
        Self::of(Database::open(path)?, capacity, lease)
    }

    fn of(database: Database, capacity: usize, lease: Duration) -> Result<Self> {
        if capacity == 0 || capacity > i32::MAX as usize || lease < Duration::from_millis(300) {
            return Err(Fault::config(
                "admission needs positive capacity and a lease of at least 300 ms",
            ));
        }
        Ok(Self {
            database,
            capacity,
            lease,
            weights: Arc::default(),
        })
    }

    /// Give a user this many consecutive turns before the next queued user.
    /// Priority still wins over weight. Configure this from trusted policy.
    pub fn weight(mut self, user: impl Into<String>, weight: u32) -> Result<Self> {
        if !(1..=1000).contains(&weight) {
            return Err(Fault::config(
                "admission weights must be between 1 and 1000",
            ));
        }
        Arc::make_mut(&mut self.weights).insert(user.into(), weight);
        Ok(self)
    }

    /// Wait for capacity until `deadline`. Dropping this future cancels its
    /// ticket; a process crash also removes it at the persisted deadline.
    pub async fn acquire(
        &self,
        priority: u8,
        user: &str,
        deadline: Instant,
    ) -> Result<Option<Slot>> {
        if deadline <= Instant::now() {
            return Ok(None);
        }
        let user = user.to_owned();
        let weight = self.weights.get(&user).copied().unwrap_or(1);
        let capacity = self.capacity;
        let lease = milliseconds(self.lease);
        let expires = now().saturating_add(milliseconds(
            deadline.saturating_duration_since(Instant::now()),
        ));
        let database = self.database.clone();
        let queued = self.database.transact(move |tx| {
            tx.execute_batch(SCHEMA).map_err(sql)?;
            tx.execute(
                "INSERT OR IGNORE INTO admission_config VALUES(1,?1,?2)",
                (capacity, lease),
            )
            .map_err(sql)?;
            let settings: (usize, i64) = tx
                .query_row(
                    "SELECT capacity,lease_ms FROM admission_config WHERE id=1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(sql)?;
            if settings != (capacity, lease) {
                return Err(Fault::config("shared admission settings differ"));
            }
            tx.execute(
                "INSERT INTO admission_waiters(priority,user,deadline) VALUES(?1,?2,?3)",
                (priority, &user, expires),
            )
            .map_err(sql)?;
            let id = tx.last_insert_rowid();
            tx.execute(
                "INSERT OR IGNORE INTO admission_users
                 SELECT ?1,?2,?3,?3,COALESCE(MAX(turn),0)+1 FROM admission_users",
                (priority, user, weight),
            )
            .map_err(sql)?;
            // Own cleanup inside the blocking operation: its result may be
            // dropped without being received if the caller was canceled.
            Ok(Ticket {
                database,
                id,
                released: false,
            })
        });
        let ticket = match tokio::time::timeout_at(deadline, queued).await {
            Ok(queued) => queued?,
            // Dropping a late result releases its ticket; the persisted
            // deadline also bounds it if this process exits before cleanup.
            Err(_) => return Ok(None),
        };
        let id = ticket.id;
        let admitted = tokio::time::timeout_at(deadline, async {
            loop {
                let expires = self.claim(id).await?;
                match expires {
                    Some(expires) if expires > 0 => return Ok::<_, Fault>(Some(expires)),
                    None => return Ok(None),
                    _ => tokio::time::sleep(POLL).await,
                }
            }
        })
        .await;
        match admitted {
            Ok(Ok(Some(expires))) => {
                let valid = expires
                    .saturating_sub(now())
                    .saturating_sub(milliseconds(self.lease / 3));
                if valid <= 0 {
                    return Ok(None);
                }
                let (renewed, deadline) =
                    watch::channel(Instant::now() + Duration::from_millis(valid as u64));
                let database = self.database.clone();
                let period = self.lease / 4;
                let margin = milliseconds(self.lease / 3);
                let heartbeat = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(period).await;
                        let previous = *renewed.borrow();
                        if previous <= Instant::now() {
                            break;
                        }
                        let renewed_until = database.transact(move |tx| {
                            let at = now();
                            let expires = at.saturating_add(lease);
                            let changed = tx.execute("UPDATE admission_waiters SET expires=?1 WHERE id=?2 AND expires>?3", (expires,id,at)).map_err(sql)?;
                            Ok((changed == 1).then_some(expires))
                        }).await;
                        match renewed_until {
                            Ok(Some(expires)) if previous > Instant::now() => {
                                let left = expires.saturating_sub(now()).saturating_sub(margin);
                                if left <= 0 {
                                    break;
                                }
                                renewed.send_replace(
                                    Instant::now() + Duration::from_millis(left as u64),
                                );
                            }
                            _ => break,
                        }
                    }
                    renewed.send_replace(Instant::now());
                });
                Ok(Some(Slot {
                    ticket,
                    heartbeat,
                    deadline,
                }))
            }
            Ok(Err(error)) => Err(error),
            _ => Ok(None),
        }
    }

    async fn claim(&self, id: i64) -> Result<Option<i64>> {
        let capacity = self.capacity;
        let lease = milliseconds(self.lease);
        self.database
            .transact(move |tx| {
                let at = now();
                tx.execute(
                    "DELETE FROM admission_waiters WHERE COALESCE(expires,deadline)<=?1",
                    [at],
                )
                .map_err(sql)?;
                tx.execute(CLEAN_USERS, []).map_err(sql)?;
                let active: usize = tx
                    .query_row(
                        "SELECT COUNT(*) FROM admission_waiters WHERE expires IS NOT NULL",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(sql)?;
                for _ in active..capacity {
                    let next: Option<(i64, u8, String)> = tx
                        .query_row(
                            "SELECT w.id,w.priority,w.user FROM admission_waiters w
                         JOIN admission_users u ON u.priority=w.priority AND u.user=w.user
                         WHERE w.expires IS NULL
                         ORDER BY w.priority DESC,u.turn,w.id LIMIT 1",
                            [],
                            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                        )
                        .optional()
                        .map_err(sql)?;
                    let Some((next, priority, user)) = next else {
                        break;
                    };
                    tx.execute(
                        "UPDATE admission_waiters SET expires=?1 WHERE id=?2",
                        (at.saturating_add(lease), next),
                    )
                    .map_err(sql)?;
                    // Spend one turn; when its weight is exhausted, move
                    // this user's remaining queue behind every other user.
                    tx.execute(
                        "UPDATE admission_users SET
                         turn=CASE WHEN remaining=1
                            THEN (SELECT MAX(turn)+1 FROM admission_users) ELSE turn END,
                         remaining=CASE WHEN remaining=1 THEN weight ELSE remaining-1 END
                         WHERE priority=?1 AND user=?2",
                        (priority, &user),
                    )
                    .map_err(sql)?;
                    tx.execute(
                        "DELETE FROM admission_users WHERE priority=?1 AND user=?2
                         AND NOT EXISTS(SELECT 1 FROM admission_waiters
                         WHERE priority=?1 AND user=?2 AND expires IS NULL)",
                        (priority, user),
                    )
                    .map_err(sql)?;
                }
                tx.query_row(
                    "SELECT COALESCE(expires,0) FROM admission_waiters WHERE id=?1",
                    [id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql)
            })
            .await
    }

    /// Queue size and available capacity, for observing this example.
    pub async fn counts(&self) -> Result<(usize, usize)> {
        let capacity = self.capacity;
        self.database
            .transact(move |tx| {
                tx.execute_batch(SCHEMA).map_err(sql)?;
                let waiting = tx
                    .query_row(
                        "SELECT COUNT(*) FROM admission_waiters
                 WHERE expires IS NULL AND deadline>?1",
                        [now()],
                        |r| r.get(0),
                    )
                    .map_err(sql)?;
                let active: usize = tx
                    .query_row(
                        "SELECT COUNT(*) FROM admission_waiters
                 WHERE expires IS NOT NULL AND expires>?1",
                        [now()],
                        |r| r.get(0),
                    )
                    .map_err(sql)?;
                Ok((waiting, capacity.saturating_sub(active)))
            })
            .await
    }
}

#[derive(Debug)]
struct Ticket {
    database: Database,
    id: i64,
    released: bool,
}

impl Ticket {
    async fn release(&mut self) -> Result<()> {
        let id = self.id;
        self.database
            .transact(move |tx| {
                tx.execute("DELETE FROM admission_waiters WHERE id=?1", [id])
                    .map_err(sql)?;
                tx.execute(CLEAN_USERS, []).map_err(sql)?;
                Ok(())
            })
            .await?;
        self.released = true;
        Ok(())
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let database = self.database.clone();
        let id = self.id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = database
                    .transact(move |tx| {
                        tx.execute("DELETE FROM admission_waiters WHERE id=?1", [id])
                            .map_err(sql)?;
                        tx.execute(CLEAN_USERS, []).map_err(sql)?;
                        Ok(())
                    })
                    .await;
            });
        }
    }
}

/// Capacity held until explicit release, drop, or lease expiry after a crash.
#[derive(Debug)]
pub struct Slot {
    ticket: Ticket,
    heartbeat: JoinHandle<()>,
    deadline: watch::Receiver<Instant>,
}
impl Slot {
    /// Resolves when renewing fails or the owner approaches lease expiry.
    /// Wrap admitted work with this signal; a lease is not permission to
    /// continue working after losing contact with its store.
    pub fn lost(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut deadline = self.deadline.clone();
        async move {
            loop {
                let until = *deadline.borrow_and_update();
                tokio::select! {
                    _ = tokio::time::sleep_until(until) => return,
                    changed = deadline.changed() => if changed.is_err() { return; },
                }
            }
        }
    }

    /// Release before returning from the request's end hook.
    pub async fn release(mut self) -> Result<()> {
        self.heartbeat.abort();
        self.ticket.release().await
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}
