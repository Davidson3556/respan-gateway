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
