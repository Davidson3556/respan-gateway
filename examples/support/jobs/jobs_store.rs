//! SQLite idempotency claims. Completed replies survive restart; expiring
//! leases recover interrupted claims. Live requests renew their lease.

use super::*;
use std::hash::{BuildHasher, RandomState};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use respan_gateway_conversation_http::Schema;
use respan_gateway_examples::store::{Database, rusqlite::OptionalExtension, sql};
use respan_gateway_http::bytes::Bytes;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY, owner TEXT NOT NULL, lease INTEGER NOT NULL, result BLOB
);";
const LEASE: Duration = Duration::from_secs(30);

pub(super) struct Sqlite(Database, Duration);

impl Sqlite {
    pub(super) fn open(path: impl AsRef<Path>) -> Result<Self, Fault> {
        Database::open(path).map(|db| Self(db, LEASE))
    }

    #[cfg(test)]
    pub(super) fn with_lease(path: impl AsRef<Path>, lease: Duration) -> Result<Self, Fault> {
        Database::open(path).map(|db| Self(db, lease))
    }
}

fn deadline(expires: i64, lease: Duration) -> Instant {
    let left = expires
        .saturating_sub(now())
        .saturating_sub((lease / 3).as_millis() as i64);
    Instant::now() + Duration::from_millis(left.max(0) as u64)
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn key(key: &Key) -> Result<String, Fault> {
    serde_json::to_string(key).map_err(|e| Fault::internal(e.to_string()))
}

// Persist the format's encoding rather than teaching canonical events a
// second serialization schema. Replay uses the same format to decode it.
fn encode(replies: &[Output]) -> Result<Vec<u8>, Fault> {
    let head = respan_gateway_http::http::Request::new(()).into_parts().0;
    let request = Bytes::from_static(br#"{"stream":true}"#);
    let request = http::Request::from_parts(head, request);
    Ok(OpenAiChat
        .encode_response(&request, replies)?
        .into_body()
        .to_vec())
}

fn decode(bytes: Vec<u8>) -> Result<Replies, Fault> {
    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
    head.headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    let response = http::Response::from_parts(head, Bytes::from(bytes));
    Ok(Arc::new(OpenAiChat.decode_response(
        response,
        &respan_gateway_http::http::Request::new(()).into_parts().0,
    )?))
}

#[async_trait]
impl Store for Sqlite {
    async fn claim(&self, key_value: &Key) -> Result<Claim, Fault> {
        let key = key(key_value)?;
        let lease = self.1;
        let owner = RandomState::new().hash_one(&key);
        loop {
            let key = key.clone();
            let result = self.0.transact(move |tx| {
                tx.execute_batch(SCHEMA).map_err(sql)?;
                let row: Option<(Option<Vec<u8>>, i64)> = tx.query_row(
                    "SELECT result,lease FROM idempotency WHERE key=?1", [&key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).optional().map_err(sql)?;
                match row {
                    Some((Some(bytes), _)) => Ok(Some(Claim::Done(decode(bytes)?))),
                    Some((None, lease)) if lease > now() => Ok(None),
                    _ => {
                        let expires = now() + lease.as_millis() as i64;
                        tx.execute("INSERT INTO idempotency(key,owner,lease) VALUES(?1,?2,?3)
                            ON CONFLICT(key) DO UPDATE SET owner=excluded.owner,lease=excluded.lease,result=NULL",
                            (key, owner.to_string(), expires)).map_err(sql)?;
                        Ok(Some(Claim::Run(owner, Some(deadline(expires, lease)))))
                    }
                }
            }).await?;
            if let Some(result) = result {
                return Ok(result);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn finish(
        &self,
        key_value: &Key,
        owner: u64,
        result: Option<Replies>,
    ) -> Result<(), Fault> {
        let key = key(key_value)?;
        let result = result
            .as_deref()
            .map(|replies| encode(replies))
            .transpose()?;
        self.0
            .transact(move |tx| {
                match result {
                    Some(bytes) => tx.execute(
                        "UPDATE idempotency SET result=?1 WHERE key=?2 AND owner=?3 AND lease>?4",
                        (bytes, key, owner.to_string(), now()),
                    ),
                    None => tx.execute(
                        "DELETE FROM idempotency WHERE key=?1 AND owner=?2",
                        (key, owner.to_string()),
                    ),
                }
                .map_err(sql)?;
                Ok(())
            })
            .await
    }

    async fn renew(&self, key_value: &Key, owner: u64) -> Result<Instant, Fault> {
        let key = key(key_value)?;
        let lease = self.1;
        self.0.transact(move |tx| {
            let at = now();
            let expires = at + lease.as_millis() as i64;
            let updated = tx.execute("UPDATE idempotency SET lease=?1 WHERE key=?2 AND owner=?3 AND result IS NULL AND lease>?4",
                (expires, key, owner.to_string(), at)).map_err(sql)?;
            if updated == 0 { return Err(Fault::abandoned()); }
            Ok(deadline(expires, lease))
        }).await
    }

    fn heartbeat(&self) -> Option<Duration> {
        Some(self.1 / 4)
    }
}
