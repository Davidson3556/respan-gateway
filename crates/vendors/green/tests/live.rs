//! A green vendor is verified end to end: each has a live test of its own in
//! the validation package, named `real_<id>_...`.

use respan_gateway_vendors_green::ALL;

/// The live tests, against the vendors' own APIs.
const LIVE: &[&str] = &[
    include_str!("../../../../validation/tests/live_providers.rs"),
    include_str!("../../../../validation/tests/live_ws.rs"),
];

#[test]
fn every_green_vendor_has_a_live_test() {
    for vendor in ALL {
        let named = format!("fn real_{}_", vendor.id);
        let tested = LIVE.iter().any(|tests| tests.contains(&named));
        assert!(tested, "{}: no live test ({named}...)", vendor.id);
    }
}
