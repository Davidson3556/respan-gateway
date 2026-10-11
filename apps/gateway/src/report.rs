//! What `respan-gateway check` prints: the composed configuration, in the
//! words a person configured it with.

use std::net::SocketAddr;

use crate::compose::Plan;
use crate::formats::KnownFormat;
use crate::models::{Probe, Reach};
use crate::{Config, VERSION};

/// A label, then each value on a line of its own.
fn row(lines: &mut Vec<String>, label: &str, values: Vec<String>) {
    for (i, value) in values.into_iter().enumerate() {
        let label = if i == 0 { label } else { "" };
        lines.push(format!("  {label:<10} {value}"));
    }
}

/// The report on `config`, composed into `plan`, if served at `listen`.
pub fn check(config: &Config, plan: &Plan, formats: &[KnownFormat], listen: SocketAddr) -> String {
    let mut lines = vec![format!("respan-gateway {VERSION}")];
    row(&mut lines, "config", vec![config.source().to_string()]);
    row(&mut lines, "listen", vec![listen.to_string()]);

    let mut providers = Vec::new();
    for u in &plan.upstreams {
        let base_url = u.entries.first().map_or("", |e| e.base_url.as_str());
        providers.push(format!("{} at {base_url}", u.name));
        if let Some(vendor) = u.preset {
            providers.push(format!("  vendor  {}", vendor.id));
        }
        let names: Vec<&str> = u.entries.iter().map(|e| e.format.as_str()).collect();
        providers.push(format!("  formats {}", names.join(", ")));
        providers.push(format!("  key     {}", u.key_from));
    }
    row(&mut lines, "providers", providers);

    if !plan.mcp.is_empty() {
        let servers = plan.mcp.iter().map(|s| {
            let (name, kind, at) = (&s.name, s.transport_label(), s.endpoint());
            vec![
                format!("{name} at {at} ({kind})"),
                format!("  target  {}", s.target()),
                format!("  key     {}", s.key_from),
            ]
        });
        row(&mut lines, "mcp", servers.flatten().collect());
    }

    let mut endpoints: Vec<String> = plan
        .serve
        .iter()
        .filter_map(|name| formats.iter().find(|f| f.name == *name))
        .flat_map(|f| f.endpoints.iter().map(|e| (*e).to_owned()))
        .collect();
    if !config.raw_relay {
        endpoints.push("GET /v1/models".to_owned());
    }
    endpoints.push("GET /healthz".to_owned());
    if config.ui {
        endpoints.push("GET /ui/".to_owned());
    }
    row(&mut lines, "endpoints", endpoints);

    let defaults = plan
        .routes
        .defaults()
        .into_iter()
        .map(|(kind, provider)| format!("{kind} -> {provider}"));
    row(&mut lines, "defaults", defaults.collect());

    let mut claims: Vec<(String, Vec<String>)> = Vec::new();
    for (prefix, provider) in plan.routes.prefixes() {
        match claims.iter_mut().find(|(p, _)| *p == provider) {
            Some((_, prefixes)) => prefixes.push(prefix),
            None => claims.push((provider, vec![prefix])),
        }
    }
    let mut routes: Vec<String> = claims
        .into_iter()
        .map(|(provider, prefixes)| format!("{} -> {provider}", prefixes.join(", ")))
        .collect();
    routes.push("<provider>/<model> -> that provider".to_owned());
    row(&mut lines, "routes", routes);

    let auth = match plan.keys.len() {
        0 => "off: no client keys".to_owned(),
        1 => "on: 1 client key".to_owned(),
        n => format!("on: {n} client keys"),
    };
    row(&mut lines, "auth", vec![auth]);
    row(&mut lines, "web ui", vec![onoff(config.ui)]);
    row(
        &mut lines,
        "log",
        vec![format!("{:?}", config.log).to_lowercase()],
    );

    let quirks = plan.upstreams.iter().flat_map(|u| {
        let quirks = u.preset.map_or(&[][..], |p| p.quirks);
        quirks
            .iter()
            .map(move |q| format!("{}: {}: {}", u.name, q.name, q.description))
    });
    let quirks: Vec<String> = quirks.collect();
    if !quirks.is_empty() {
        row(&mut lines, "quirks", quirks);
    }
    lines.join("\n")
}

/// What `check --online` prints: how each provider answered a call made
/// with its key. No key, and no URL, is named.
pub fn online(probes: &[Probe]) -> String {
    let width = probes.iter().map(|p| p.provider.len()).max().unwrap_or(0);
    let mut lines = vec!["online (one call to each provider, with its key)".to_owned()];
    for p in probes {
        let (state, note) = match &p.reach {
            Reach::Reachable => ("reachable", String::new()),
            Reach::Unauthorized(status) => (
                "unauthorized",
                format!("  it answered {status}: check the key"),
            ),
            Reach::Unreachable(why) => ("unreachable", format!("  {why}")),
        };
        let ms = p.took.as_millis();
        let provider = &p.provider;
        lines.push(format!("  {provider:<width$}  {state:<12}  {ms} ms{note}"));
    }
    if probes.is_empty() {
        lines.push("  no provider lists its models".to_owned());
    }
    lines.join("\n")
}

/// Whether every provider probed answered.
pub fn all_reachable(probes: &[Probe]) -> bool {
    probes.iter().all(|p| p.reach == Reach::Reachable)
}

fn onoff(on: bool) -> String {
    match on {
        true => "on".to_owned(),
        false => "off".to_owned(),
    }
}
