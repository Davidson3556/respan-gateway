//! Running the tools of a step, within the limits of the agent.

use std::sync::Arc;

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::stream::{self};
use respan_gateway_core::{ErrorCode, Fault, Upstream};
use serde_json::Value;
use tokio::time::{Instant, timeout};

use super::{Limits, Outcome, ToolReport, Toolbox};

pub(super) async fn run_tools(
    toolbox: &Arc<dyn Toolbox>,
    limits: Limits,
    deadline: Instant,
    calls: Vec<(String, String, String)>,
) -> Vec<(String, Outcome, ToolReport)> {
    let each = calls.into_iter().map(|(id, name, arguments)| {
        let toolbox = toolbox.clone();
        async move {
            let started = Instant::now();
            let left = deadline.saturating_duration_since(started);
            let allowed = limits.tool_timeout.min(left);
            let parsed = match arguments.trim() {
                "" => Ok(Value::Object(Default::default())),
                text => serde_json::from_str::<Value>(text),
            };
            let outcome = match parsed {
                Err(e) => Outcome::error(format!("the arguments are not valid JSON: {e}")),
                Ok(arguments) => match timeout(allowed, toolbox.call(&name, arguments)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Outcome::error(format!(
                        "the tool did not answer within {} s",
                        allowed.as_secs().max(1)
                    )),
                },
            };
            let outcome = Outcome {
                text: cut(outcome.text, limits.max_result),
                ..outcome
            };
            let report = ToolReport {
                name,
                took: started.elapsed(),
                is_error: outcome.is_error,
                bytes: outcome.text.len(),
            };
            (id, outcome, report)
        }
    });
    stream::iter(each)
        .buffered(limits.parallel.max(1))
        .collect()
        .await
}

/// What a request has left for tool answers: each of `results`, in order,
/// is cut to what is left of `budget`, which it then takes from.
pub(super) fn spend(budget: &mut usize, results: &mut [(String, Outcome, ToolReport)]) {
    for (_, outcome, report) in results {
        let text = std::mem::take(&mut outcome.text);
        outcome.text = match *budget {
            0 => "[left out: the tool answers of this request reached their limit]".to_owned(),
            left => cut(text, left),
        };
        report.bytes = outcome.text.len();
        *budget = budget.saturating_sub(outcome.text.len());
    }
}

/// `text` of at most `max` bytes, saying what was cut off.
pub(super) fn cut(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let omitted = text.len() - end;
    format!("{}\n[cut: {omitted} more bytes]", &text[..end])
}

pub(super) fn limit(code: &'static str, message: String) -> Fault {
    Upstream::fault(message).with(ErrorCode(code.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_is_cut_at_a_character() {
        assert_eq!(cut("abc".into(), 3), "abc");
        assert_eq!(cut("aéb".into(), 2), "a\n[cut: 3 more bytes]");
        assert_eq!(cut("abcdef".into(), 4), "abcd\n[cut: 2 more bytes]");
    }

    #[test]
    fn answers_share_a_budget() {
        let result = |text: &str| {
            let report = ToolReport {
                name: "t".into(),
                took: Duration::ZERO,
                is_error: false,
                bytes: text.len(),
            };
            ("id".to_owned(), Outcome::ok(text), report)
        };
        let mut results = vec![result("aaaa"), result("bbbb"), result("cccc")];
        let mut budget = 6;
        spend(&mut budget, &mut results);
        assert_eq!(results[0].1.text, "aaaa");
        assert!(
            results[1].1.text.starts_with("bb\n[cut"),
            "{}",
            results[1].1.text
        );
        assert!(results[2].1.text.contains("left out"));
        assert_eq!(results[1].2.bytes, results[1].1.text.len());
        assert_eq!(budget, 0);
    }
}
