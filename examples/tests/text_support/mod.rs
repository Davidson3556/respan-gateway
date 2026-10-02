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

/// `deltas` of tool-call arguments (and any other events), with a
/// `BlockStart` of a tool call before the first delta of each block, as a
/// format's decoder opens them.
#[allow(dead_code)]
pub fn opened(
    events: Vec<respan_gateway_sdk::conversation::Reply>,
) -> Vec<respan_gateway_sdk::conversation::Reply> {
    use respan_gateway_sdk::conversation::{Block, Reply};
    let mut open = Vec::new();
    let mut out = Vec::new();
    for event in events {
        if let Reply::Delta { block, .. } = &event
            && !open.contains(block)
        {
            open.push(*block);
            let kind = Block::ToolCall {
                id: format!("call_{block}"),
                name: "f".into(),
            };
            out.push(Reply::block_start(*block, kind));
        }
        out.push(event);
    }
    out
}

/// The arguments of tool call `block` in `events`: its deltas, joined.
#[allow(dead_code)]
pub fn arguments_of(events: &[respan_gateway_sdk::conversation::Reply], block: u32) -> String {
    use respan_gateway_sdk::conversation::Reply;
    events
        .iter()
        .filter_map(|e| match e {
            Reply::Delta { block: b, delta } if *b == block => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}
