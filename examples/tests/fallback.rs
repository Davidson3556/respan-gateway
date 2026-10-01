use tokio::time::Instant;

use super::*;

const LIMITED_0: &str = "openai gpt-4o #0 Loser requests";
const LIMITED_1: &str = "openai gpt-4o #1 Loser requests";
const LOST_0: &str = "openai gpt-4o #0 Loser upstream";
const LOST_1: &str = "openai gpt-4o #1 Loser upstream";
/// The fallback asked Anthropic for its own model name (`on_provider`).
const CLAUDE: &str = "anthropic claude-sonnet-4-5 #2 Winner ok";

#[tokio::test]
async fn retry_after_is_honoured_then_it_falls_back_across_formats() -> Result<(), Fault> {
    let start = Instant::now();
    let (body, attempts, calls) = rate_limited_then_fallback().await?;
    assert_eq!(attempts, [CLAUDE, LIMITED_0, LIMITED_1]);
    assert_eq!(calls, [2, 0, 1]);
    assert!(body.contains("Hi from Claude") && start.elapsed() >= Duration::from_secs(1));
    Ok(())
}

#[tokio::test]
async fn a_stream_cut_after_its_first_byte_is_not_retried() -> Result<(), Fault> {
    let (body, attempts, calls) = cut_after_first_words().await?;
    assert!(
        body.contains("Hello") && body.contains("broke off"),
        "{body}"
    );
    assert_eq!(attempts, ["openai gpt-4o #0 Winner upstream"]);
    assert_eq!(calls, [1, 0, 0], "committed partial stream was not retried");
    Ok(())
}

#[tokio::test]
async fn a_stalled_provider_is_cut_by_the_watchdog_and_falls_back() -> Result<(), Fault> {
    let start = Instant::now();
    let (body, attempts, calls) = stalled_then_fallback().await?;
    assert_eq!(attempts, [CLAUDE, LOST_0, LOST_1]);
    assert_eq!(
        calls,
        [0, 0, 1],
        "both stalled calls were canceled before dialing"
    );
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_secs(4) && elapsed < Duration::from_secs(5));
    assert!(body.contains("Hi from Claude"), "{body}");
    Ok(())
}

/// Regression: one bound for head and gaps cut every whole answer slower
/// than the gap, and the retry made the provider do the work again.
#[tokio::test]
async fn a_whole_answer_slower_than_the_gap_bound_is_not_cut() -> Result<(), Fault> {
    let (body, attempts, calls) = slow_whole_answer().await?;
    assert!(body.contains("an essay"), "{body}");
    assert_eq!(attempts, ["openai gpt-4o #0 Winner ok"]);
    assert_eq!(calls, [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn a_hedge_keeps_the_first_to_answer_and_abandons_the_other() -> Result<(), Fault> {
    let (body, attempts, calls) = hedged().await?;
    assert!(body.contains("fast") && !body.contains("late"), "{body}");
    let lost = "openai gpt-4o #0 Loser abandoned";
    assert_eq!(attempts, [lost, "openai-eu gpt-4o #1 Winner ok"]);
    assert_eq!(
        calls,
        [0, 1, 0],
        "slow primary was abandoned before dialing"
    );
    Ok(())
}
