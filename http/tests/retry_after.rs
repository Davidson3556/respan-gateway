use std::time::Duration;

use h::http::{Response, header::RETRY_AFTER};
use respan_gateway_http as h;
use respan_gateway_sdk::{RetryAfter, Upstream};

#[test]
fn retry_after_rounding_handles_maximum_duration() {
    for (wait, seconds) in [
        (Duration::ZERO, 0),
        (Duration::from_nanos(1), 1),
        (Duration::from_secs(1), 1),
        (Duration::from_millis(1500), 2),
        (Duration::new(u64::MAX - 1, 1), u64::MAX),
        (Duration::from_secs(u64::MAX), u64::MAX),
        (Duration::MAX, u64::MAX),
    ] {
        let fault = Upstream::fault("retry later").with(RetryAfter(wait));
        let mut head = Response::new(()).into_parts().0;
        h::error_head(&mut head, &fault);
        assert_eq!(
            head.headers[RETRY_AFTER].to_str().expect("ASCII delay"),
            seconds.to_string(),
            "{wait:?}"
        );
        assert_eq!(
            h::retry_after(&head.headers),
            Some(Duration::from_secs(seconds))
        );
    }
}
