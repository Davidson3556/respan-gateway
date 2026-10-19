//! Reading a tool call's arguments, each failure a [`ToolError`] the model
//! can act on.

use crate::ToolError;
use serde_json::{Map, Value};

/// The arguments as an object; none at all is an empty one.
pub(crate) fn object(arguments: &Value) -> Result<&Map<String, Value>, ToolError> {
    static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
    match arguments {
        Value::Object(map) => Ok(map),
        Value::Null => Ok(EMPTY.get_or_init(Map::new)),
        _ => Err(ToolError::new("arguments must be a JSON object")),
    }
}

/// The non-empty string `key`, which must be there.
pub(crate) fn required_str<'a>(
    args: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ToolError> {
    optional_str(args, key)?.ok_or_else(|| ToolError::new(format!("`{key}` is required")))
}

/// The string `key`, if there and not empty.
pub(crate) fn optional_str<'a>(
    args: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.trim().is_empty() => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.as_str())),
        Some(_) => Err(ToolError::new(format!("`{key}` must be a string"))),
    }
}

/// The string `key`, if there, one of `allowed`.
pub(crate) fn optional_enum<'a>(
    args: &'a Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<Option<&'a str>, ToolError> {
    match optional_str(args, key)? {
        Some(v) if !allowed.contains(&v) => Err(ToolError::new(format!(
            "`{key}` must be one of {}, not {v:?}",
            allowed.join(", ")
        ))),
        v => Ok(v),
    }
}

/// The boolean `key`, if there.
pub(crate) fn optional_bool(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ToolError::new(format!("`{key}` must be a boolean"))),
    }
}

/// The integer `key`, if there, within `min..=max`.
pub(crate) fn optional_int(
    args: &Map<String, Value>,
    key: &str,
    min: u64,
    max: u64,
) -> Result<Option<u64>, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_u64().or_else(|| {
            v.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= 0.0)
                .map(|f| f as u64)
        }) {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(ToolError::new(format!(
                "`{key}` must be an integer from {min} to {max}"
            ))),
        },
    }
}

/// The list of strings `key`, if there and not empty. A single string is a
/// list of one.
pub(crate) fn optional_str_list(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, ToolError> {
    let invalid = || ToolError::new(format!("`{key}` must be an array of strings"));
    let list: Vec<String> = match args.get(key) {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(invalid()),
    };
    let list: Vec<String> = list
        .into_iter()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    Ok((!list.is_empty()).then_some(list))
}

/// The date `key`, if there, as `YYYY-MM-DD`.
pub(crate) fn optional_date<'a>(
    args: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, ToolError> {
    let Some(date) = optional_str(args, key)? else {
        return Ok(None);
    };
    let b = date.as_bytes();
    let shaped = b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
    if shaped {
        Ok(Some(date))
    } else {
        Err(ToolError::new(format!(
            "`{key}` must be a date as YYYY-MM-DD, not {date:?}"
        )))
    }
}

/// `value` as indented JSON.
pub(crate) fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn reads_and_rejects() {
        let a = map(json!({
            "s": "x", "blank": " ", "n": 3, "f": 2.0, "b": true,
            "l": ["a", " ", "b"], "one": "c", "d": "2025-01-31", "bad": "2025/01/31"
        }));
        assert_eq!(required_str(&a, "s"), Ok("x"));
        assert!(required_str(&a, "blank").is_err());
        assert!(required_str(&a, "missing").is_err());
        assert!(required_str(&a, "n").is_err());
        assert_eq!(optional_int(&a, "n", 1, 5), Ok(Some(3)));
        assert_eq!(optional_int(&a, "f", 1, 5), Ok(Some(2)));
        assert!(optional_int(&a, "n", 4, 5).is_err());
        assert_eq!(optional_bool(&a, "b"), Ok(Some(true)));
        assert!(optional_bool(&a, "s").is_err());
        assert_eq!(
            optional_str_list(&a, "l"),
            Ok(Some(vec!["a".into(), "b".into()]))
        );
        assert_eq!(optional_str_list(&a, "one"), Ok(Some(vec!["c".into()])));
        assert_eq!(optional_date(&a, "d"), Ok(Some("2025-01-31")));
        assert!(optional_date(&a, "bad").is_err());
        assert!(optional_enum(&a, "s", &["y"]).is_err());
        assert!(object(&json!([1])).is_err());
        assert!(object(&Value::Null).is_ok());
    }
}
