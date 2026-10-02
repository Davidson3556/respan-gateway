use respan_gateway_sdk::{Fault, Filter};

/// Values emitted by a filter, including its successful end.
pub fn run_filter<T>(filter: &mut impl Filter<T>, values: Vec<T>) -> Result<Vec<T>, Fault> {
    let mut out = Vec::new();
    for value in values {
        filter.edit(value, &mut out)?;
    }
    filter.end(&Ok(()), &mut out)?;
    Ok(out)
}

/// The arguments of tool call `block` in `events`: its deltas, joined.
#[allow(dead_code)]
pub fn arguments_of(events: &[respan_gateway_sdk::conversation::Reply], block: u32) -> String {
    use respan_gateway_sdk::conversation::{Delta, Reply};
    events
        .iter()
        .filter_map(|e| match e {
            Reply::Delta {
                block: b,
                delta: Delta::Arguments(arguments),
            } if *b == block => Some(arguments.as_str()),
            _ => None,
        })
        .collect()
}
