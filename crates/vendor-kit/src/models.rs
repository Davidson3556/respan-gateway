//! A provider's models: as OpenAI lists them (`GET /models`), unless its
//! vendor lists them its own way ([`Vendor::models`](crate::Vendor::models)).

use std::future::Future;
use std::pin::Pin;

use respan_gateway_core::Fault;
use respan_gateway_http::{Body, http};
use respan_gateway_http_faults::built_wrong;
use respan_gateway_http_transport::json;
use serde_json::Value;

use crate::VendorProvider;

/// How a vendor lists a provider's models: with its client
/// ([`VendorProvider::client`]), as it answers.
pub type ListModels = for<'a> fn(&'a VendorProvider) -> BoxFuture<'a, Models>;

/// A future to come, boxed.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The models a provider lists.
pub type Models = Result<Vec<Model>, Fault>;

/// A model a provider lists, with what its list says of it.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Model {
    /// Its id, the name a call takes.
    pub id: String,
    /// When it was made, in seconds since the Unix epoch, if it says.
    pub created: Option<u64>,
    /// Its name for a person, such as `Claude Haiku 5.5`.
    pub display_name: Option<String>,
    /// What it is.
    pub description: Option<String>,
    /// How many tokens it reads at most.
    pub context_window: Option<u64>,
    /// How many tokens it writes at most.
    pub max_output_tokens: Option<u64>,
    /// What it reads beyond text: `image`, `audio`, `pdf`, `video`.
    pub input_modalities: Vec<String>,
    /// What it can do: `reasoning`, `tools`, `structured_outputs`, ...
    pub capabilities: Vec<String>,
    /// What it costs, if its list says.
    pub pricing: Option<Pricing>,
    /// When it is retired, if it is to be.
    pub retires_at: Option<String>,
}

/// What a model costs, in US dollars per million tokens.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Pricing {
    /// Of each input token.
    pub input: f64,
    /// Of each output token.
    pub output: f64,
    /// Of each input token read from a cache, if it says.
    pub cached_input: Option<f64>,
}

impl Model {
    /// Model `id`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ..Self::default()
        }
    }

    /// The models of an answer as OpenAI's, `{"data": [{"id", "created"}]}`,
    /// with what OpenAI-compatible lists add read where they say it: a
    /// name, a description, a context length, an output limit, modalities
    /// and pricing (OpenRouter's per token, Together's per million).
    pub fn openai(answer: &Value) -> Vec<Model> {
        let models = answer["data"].as_array().into_iter().flatten();
        models.filter_map(Model::compatible).collect()
    }

    /// One entry of an OpenAI-compatible list.
    fn compatible(m: &Value) -> Option<Model> {
        let text = |keys: &[&str]| keys.iter().find_map(|k| m[k].as_str().map(str::to_owned));
        let count = |keys: &[&str]| keys.iter().find_map(|k| m[k].as_u64());
        let mut model = Model::new(m["id"].as_str()?);
        model.created = m["created"].as_u64();
        model.display_name = text(&["display_name", "name"]).filter(|n| *n != model.id);
        model.description = text(&["description"]).filter(|d| !d.is_empty());
        model.context_window = count(&["context_length", "context_window", "max_context_length"]);
        model.max_output_tokens = m["top_provider"]["max_completion_tokens"]
            .as_u64()
            .or_else(|| count(&["max_completion_tokens", "max_output_tokens"]));
        let modalities = m["architecture"]["input_modalities"].as_array();
        model.input_modalities = modalities
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|kind| *kind != "text")
            .map(str::to_owned)
            .collect();
        let parameters = m["supported_parameters"].as_array().into_iter().flatten();
        for parameter in parameters.filter_map(Value::as_str) {
            let can = match parameter {
                "tools" => "tools",
                "reasoning" => "reasoning",
                "structured_outputs" => "structured_outputs",
                _ => continue,
            };
            model.capabilities.push(can.to_owned());
        }
        if m["capabilities"]["vision"] == true {
            model.input_modalities.push("image".into());
        }
        if m["capabilities"]["function_calling"] == true {
            model.capabilities.push("tools".into());
        }
        model.pricing = pricing(&m["pricing"]);
        model.retires_at = text(&["shutdown_date", "deprecation_date"]);
        Some(model)
    }
}

/// A list's pricing: OpenRouter's, `prompt` and `completion` in dollars per
/// token as text; else Together's, `input` and `output` per million.
fn pricing(p: &Value) -> Option<Pricing> {
    let number = |v: &Value| v.as_f64().or_else(|| v.as_str()?.parse().ok());
    if let (Some(input), Some(output)) = (number(&p["prompt"]), number(&p["completion"])) {
        let cached = number(&p["input_cache_read"]).map(|c| c * 1e6);
        return Some(Pricing {
            input: input * 1e6,
            output: output * 1e6,
            cached_input: cached,
        });
    }
    let (input, output) = (number(&p["input"])?, number(&p["output"])?);
    Some(Pricing {
        input,
        output,
        cached_input: number(&p["cached_input"]),
    })
}

/// OpenAI's model list: `GET /models`.
pub fn openai(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let call = http::Request::get("/models").body(Body::default());
        let (answer, _) = p.client()?.send(call.map_err(built_wrong)?).await?;
        Ok(Model::openai(&json(answer).await?))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn what_compatible_lists_add_is_read_where_they_say_it() {
        let answer = json!({ "data": [
            // OpenRouter's: per-token prices as text, modalities, parameters.
            { "id": "anthropic/claude-x", "name": "Anthropic: Claude X", "context_length": 200000,
              "architecture": { "input_modalities": ["text", "image"] },
              "top_provider": { "max_completion_tokens": 64000 },
              "pricing": { "prompt": "0.000003", "completion": "0.000015", "input_cache_read": "0.0000003" },
              "supported_parameters": ["tools", "reasoning", "temperature"] },
            // Together's: per-million prices, a display name.
            { "id": "meta-llama/Llama-3", "display_name": "Llama 3", "context_length": 8192,
              "pricing": { "input": 0.88, "output": 0.88 } },
            // Mistral's: capabilities, a context length of its own name.
            { "id": "mistral-small", "max_context_length": 32000, "description": "Small",
              "capabilities": { "vision": true, "function_calling": true } },
            // OpenAI's own: an id and when, and a retirement.
            { "id": "gpt-old", "created": 1, "shutdown_date": "2026-10-23" },
        ] });
        let models = Model::openai(&answer);
        let router = &models[0];
        assert_eq!(router.display_name.as_deref(), Some("Anthropic: Claude X"));
        assert_eq!(
            (router.context_window, router.max_output_tokens),
            (Some(200000), Some(64000))
        );
        assert_eq!(router.input_modalities, ["image"]);
        assert_eq!(router.capabilities, ["tools", "reasoning"]);
        let price = router.pricing.expect("priced");
        assert!((price.input - 3.0).abs() < 1e-9 && (price.output - 15.0).abs() < 1e-9);
        assert!((price.cached_input.unwrap_or_default() - 0.3).abs() < 1e-9);
        assert_eq!(
            models[1].pricing,
            Some(Pricing {
                input: 0.88,
                output: 0.88,
                cached_input: None
            })
        );
        assert_eq!(models[2].context_window, Some(32000));
        assert_eq!(models[2].input_modalities, ["image"]);
        assert_eq!(models[2].capabilities, ["tools"]);
        assert_eq!(models[3].retires_at.as_deref(), Some("2026-10-23"));
        assert_eq!(models[3].pricing, None);
    }
}
