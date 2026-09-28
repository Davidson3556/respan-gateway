//! `MemoryLedger`: nested accounts, all-or-nothing holds, idempotence, TTL.

use std::time::Duration;

use respan_gateway_ledger::{HoldId, INSUFFICIENT_FUNDS, Ledger, MemoryLedger};

const TTL: Duration = Duration::from_secs(60);

fn ledger() -> MemoryLedger {
    let ledger = MemoryLedger::new();
    ledger
        .open("acme", None, 1000)
        .open("team", Some("acme"), 300)
        .open("key-1", Some("team"), 500)
        .open("key-2", Some("team"), 500);
    ledger
}

fn names(accounts: &[&str]) -> Vec<String> {
    accounts.iter().map(|a| a.to_string()).collect()
}

#[tokio::test]
async fn nested_accounts_reserve_all_or_nothing() {
    let ledger = ledger();
    // The team is short, so nothing is held anywhere.
    let refused = ledger
        .reserve(&HoldId::new(), &names(&["key-1"]), 400, TTL)
        .await;
    let fault = refused
        .err()
        .map(|f| (f.status().as_u16(), f.code().to_owned()));
    assert_eq!(fault, Some((402, INSUFFICIENT_FUNDS.to_owned())));
    for (account, left) in [("key-1", 500), ("team", 300), ("acme", 1000)] {
        assert_eq!(ledger.available(account), left, "{account}");
    }
    // Two keys of one team hold the team once.
    let both = names(&["key-1", "key-2"]);
    assert!(
        ledger
            .reserve(&HoldId::new(), &both, 200, TTL)
            .await
            .is_ok()
    );
    for (account, left) in [("key-1", 300), ("key-2", 300), ("team", 100), ("acme", 800)] {
        assert_eq!(ledger.available(account), left, "{account}");
    }
}

#[tokio::test]
async fn reserve_and_settle_are_idempotent_by_hold_id() {
    let ledger = ledger();
    let (id, key) = (HoldId::new(), names(&["key-1"]));
    for _ in 0..2 {
        assert!(ledger.reserve(&id, &key, 100, TTL).await.is_ok());
    }
    assert_eq!(ledger.available("team"), 200);
    for _ in 0..2 {
        assert!(ledger.settle(&id, 60).await.is_ok());
    }
    assert_eq!((ledger.spent("acme"), ledger.available("team")), (60, 240));
}

#[tokio::test]
async fn an_unknown_account_is_refused() {
    let refused = ledger()
        .reserve(&HoldId::new(), &names(&["nobody"]), 1, TTL)
        .await;
    assert_eq!(refused.err().map(|f| f.status().as_u16()), Some(401));
}

#[tokio::test(start_paused = true)]
async fn a_hold_never_settled_is_released_after_its_ttl() {
    let ledger = ledger();
    let id = HoldId::new();
    let ttl = Duration::from_secs(1);
    assert!(
        ledger
            .reserve(&id, &names(&["key-1"]), 100, ttl)
            .await
            .is_ok()
    );
    assert_eq!(ledger.available("acme"), 900);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(ledger.available("acme"), 1000);
}

/// Expiry releases the hold, not the debt: a call that outlives its TTL,
/// such as an hour-long session, is still charged on the hold's accounts.
#[tokio::test(start_paused = true)]
async fn a_call_that_outlives_its_ttl_is_still_charged() {
    let ledger = ledger();
    let id = HoldId::new();
    let ttl = Duration::from_secs(900);
    let key = names(&["key-1"]);
    assert!(ledger.reserve(&id, &key, 100, ttl).await.is_ok());
    tokio::time::advance(Duration::from_secs(901)).await;
    assert_eq!(ledger.available("team"), 300, "released");
    assert!(ledger.settle(&id, 250).await.is_ok());
    for account in ["key-1", "team", "acme"] {
        assert_eq!(ledger.spent(account), 250, "{account}");
    }
    assert_eq!(ledger.available("team"), 50);
    // Settled once: a repeat is a no-op.
    assert!(ledger.settle(&id, 250).await.is_ok());
    assert_eq!(ledger.spent("acme"), 250);
}

/// A hold never settled is forgotten one TTL after it expires, so lost
/// settlements do not pile up; a settlement after that is a no-op.
#[tokio::test(start_paused = true)]
async fn a_hold_never_settled_is_forgotten_after_a_grace_period() {
    let ledger = ledger();
    let id = HoldId::new();
    let ttl = Duration::from_secs(1);
    assert!(
        ledger
            .reserve(&id, &names(&["key-1"]), 100, ttl)
            .await
            .is_ok()
    );
    tokio::time::advance(Duration::from_millis(2001)).await;
    assert!(ledger.settle(&id, 250).await.is_ok());
    assert_eq!((ledger.spent("acme"), ledger.available("acme")), (0, 1000));
}
