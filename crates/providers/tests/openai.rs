//! OpenAI's quirks.

use respan_gateway_providers::openai;

#[test]
fn protocol() {
    assert_eq!(openai::REALTIME.subprotocol(), Some("realtime"));
    assert_eq!(openai::RESPONSES_WS.subprotocol(), None);
}
