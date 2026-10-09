//! Structured output requirements shared by the conversation formats.
use crate::common::{NativeFieldsExt, untranslatable};
use crate::{anthropic::ANTHROPIC, openai::OPENAI};
use respan_gateway_conversation::{Input, ResponseFormat};
use respan_gateway_sdk::Fault;
use serde_json::{Map, Value, json};

pub(crate) fn read(format: &str, v: &Value, input: &mut Input) {
    let raw = if format == OPENAI {
        &v["response_format"]
    } else {
        &v["output_config"]["format"]
    };
    let parsed = match raw["type"].as_str() {
        Some("text") if format == OPENAI && raw.as_object().is_some_and(|m| m.len() == 1) => {
            Some(ResponseFormat::Text)
        }
        Some("json_object")
            if format == OPENAI && raw.as_object().is_some_and(|m| m.len() == 1) =>
        {
            Some(ResponseFormat::JsonObject)
        }
        Some("json_schema") => {
            let spec = if format == OPENAI {
                &raw["json_schema"]
            } else {
                raw
            };
            let known = if format == OPENAI {
                &["name", "description", "schema", "strict"][..]
            } else {
                &["type", "schema"][..]
            };
            let clean = spec
                .as_object()
                .is_some_and(|m| m.keys().all(|k| known.contains(&k.as_str())));
            if !clean
                || (format == OPENAI
                    && (!spec["name"].is_string()
                        || spec.get("description").is_some_and(|v| !v.is_string())
                        || spec.get("strict").is_some_and(|v| !v.is_boolean())))
                || !spec["schema"].is_object()
                || (format == OPENAI
                    && raw
                        .as_object()
                        .is_none_or(|m| m.keys().any(|k| k != "type" && k != "json_schema")))
            {
                None
            } else {
                Some(ResponseFormat::JsonSchema {
                    name: spec["name"].as_str().unwrap_or("response").into(),
                    description: spec["description"].as_str().map(str::to_owned),
                    schema: spec["schema"].clone(),
                    strict: if format == ANTHROPIC {
                        Some(true)
                    } else {
                        spec["strict"].as_bool()
                    },
                })
            }
        }
        _ => None,
    };
    if let Some(parsed) = parsed {
        input.options.response_format = Some(parsed);
        if let Some(native) = &mut input.native {
            if format == OPENAI {
                native.fields.remove("response_format");
            } else if let Some(Value::Object(config)) = native.fields.get_mut("output_config") {
                config.remove("format");
                if config.is_empty() {
                    native.fields.remove("output_config");
                }
            }
        }
        if input.native.is_empty() {
            input.native = None;
        }
    }
}

pub(crate) fn write(
    format: &str,
    input: &Input,
    out: &mut Map<String, Value>,
) -> Result<(), Fault> {
    let Some(requirement) = &input.options.response_format else {
        return Ok(());
    };
    match (format, requirement) {
        (OPENAI, ResponseFormat::Text) => {
            out.insert("response_format".into(), json!({"type":"text"}));
        }
        (OPENAI, ResponseFormat::JsonObject) => {
            out.insert("response_format".into(), json!({"type":"json_object"}));
        }
        (
            OPENAI,
            ResponseFormat::JsonSchema {
                name,
                description,
                schema,
                strict,
            },
        ) => {
            let mut spec = json!({"name":name,"schema":schema});
            if let Some(description) = description {
                spec["description"] = description.clone().into();
            }
            if let Some(strict) = strict {
                spec["strict"] = (*strict).into();
            }
            out.insert(
                "response_format".into(),
                json!({"type":"json_schema","json_schema":spec}),
            );
        }
        (ANTHROPIC, ResponseFormat::Text) => {}
        (ANTHROPIC, ResponseFormat::JsonObject) => {
            return Err(untranslatable(
                "unconstrained JSON object output in Anthropic; supply a schema",
            ));
        }
        (
            ANTHROPIC,
            ResponseFormat::JsonSchema {
                schema,
                description,
                strict,
                ..
            },
        ) => {
            // OpenAI uses the wrapper description to instruct the model, and
            // Anthropic always constrains the result. Do not silently discard
            // those requests when there is no equivalent target field.
            if description.is_some() || *strict == Some(false) {
                return Err(untranslatable(
                    "a schema wrapper description or non-strict schema output in Anthropic",
                ));
            }
            let existing = if input.native.format() == Some(ANTHROPIC) {
                input.native.fields().get("output_config").cloned()
            } else {
                None
            };
            let mut config = existing.unwrap_or_else(|| json!({}));
            if !config.is_object() {
                return Err(untranslatable("non-object output_config"));
            }
            config["format"] = json!({"type":"json_schema","schema":schema});
            out.insert("output_config".into(), config);
        }
        _ => return Err(untranslatable("this response format")),
    }
    Ok(())
}
