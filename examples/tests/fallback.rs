use tokio::time::Instant;

use super::*;

const LOST_0: &str = "openai #0 Loser upstream";
const LOST_1: &str = "openai #1 Loser upstream";
const CLAUDE: &str = "anthropic #2 Winner ok";

#[tokio::test(start_paused = true)]
async fn retry_after_is_honoured_then_it_falls_back_across_formats() -> Result<(), Fault> {
    let start = Instant::now();
    let (body, attempts) = rate_limited_then_fallback().await?;
    assert_eq!(attempts, [CLAUDE, LOST_0, LOST_1]);
    assert!(body.contains("Hi from Claude") && start.elapsed() >= Duration::from_secs(1));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_stream_cut_after_its_first_byte_is_not_retried() -> Result<(), Fault> {
    let (body, attempts) = cut_after_first_words().await?;
    assert!(body.contains("Hello") && body.contains("reset"), "{body}");
    assert_eq!(attempts, ["openai #0 Winner upstream"]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_stalled_provider_is_cut_by_the_watchdog_and_falls_back() -> Result<(), Fault> {
    let start = Instant::now();
    let (body, attempts) = stalled_then_fallback().await?;
    assert_eq!(attempts, [CLAUDE, LOST_0, LOST_1]);
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_secs(4) && elapsed < Duration::from_secs(5));
    assert!(body.contains("Hi from Claude"), "{body}");
    Ok(())
}

/// Regression: one bound for head and gaps cut every whole answer slower
/// than the gap, and the retry made the provider do the work again.
#[tokio::test(start_paused = true)]
async fn a_whole_answer_slower_than_the_gap_bound_is_not_cut() -> Result<(), Fault> {
    let (body, attempts) = slow_whole_answer().await?;
    assert!(body.contains("an essay"), "{body}");
    assert_eq!(attempts, ["openai #0 Winner ok"]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_hedge_keeps_the_first_to_answer_and_abandons_the_other() -> Result<(), Fault> {
    let (body, attempts) = hedged().await?;
    assert!(body.contains("fast") && !body.contains("late"), "{body}");
    let lost = "openai #0 Loser abandoned";
    assert_eq!(attempts, [lost, "openai-eu #1 Winner ok"]);
    Ok(())
}
