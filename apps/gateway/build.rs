//! Embeds the web UI (`apps/ui/dist`) in the binary: writes a table of its
//! files, each `include_bytes!`, to `$OUT_DIR/ui_assets.rs`. Without a
//! built UI, the table holds one page that says how to build it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const PLACEHOLDER: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>respan-gateway</title>
<style>body{font:16px/1.5 system-ui,sans-serif;max-width:40rem;margin:4rem auto;padding:0 1rem;color:#222;background:#fff}
code,pre{background:#f2f2f2;border-radius:4px;padding:.1rem .3rem}pre{padding:.75rem}
@media (prefers-color-scheme:dark){body{color:#ddd;background:#111}code,pre{background:#222}}</style></head>
<body><h1>respan-gateway</h1>
<p>The gateway is running, but this binary was built without its web UI.</p>
<p>Build the UI, then the gateway again:</p>
<pre>cd apps/ui && pnpm install && pnpm build
cargo build -p respan-gateway</pre>
<p>The admin API is served regardless, at <a href="/ui/api/overview"><code>/ui/api/overview</code></a>.</p>
</body></html>
"#;

fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap_or_default());
    let dist = manifest.join("../ui/dist");
    // Watch the nearest folder that exists, so building the UI later
    // rebuilds the gateway.
    let watched = [dist.clone(), manifest.join("../ui"), manifest.join("..")];
    if let Some(watched) = watched.iter().find(|p| p.exists()) {
        println!("cargo:rerun-if-changed={}", watched.display());
    }
    println!("cargo:rerun-if-changed=build.rs");

    let mut files = Vec::new();
    if dist.join("index.html").is_file() {
        collect(&dist, &dist, &mut files);
    } else {
        let page = out.join("ui-placeholder.html");
        if let Err(e) = std::fs::write(&page, PLACEHOLDER) {
            panic!("write {}: {e}", page.display());
        }
        files.push(("index.html".to_owned(), page));
    }
    files.sort();
    let mut table = String::from("pub static ASSETS: &[(&str, &[u8])] = &[\n");
    for (name, path) in files {
        let _ = writeln!(
            table,
            "    ({name:?}, include_bytes!({:?})),",
            path.display().to_string()
        );
    }
    table.push_str("];\n");
    let target = out.join("ui_assets.rs");
    if let Err(e) = std::fs::write(&target, table) {
        panic!("write {}: {e}", target.display());
    }
}

/// Every file under `dir`, as its path from `root` with `/`, and its path.
fn collect(root: &Path, dir: &Path, files: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, files);
        } else if let Ok(relative) = path.strip_prefix(root) {
            let parts: Vec<String> = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect();
            files.push((parts.join("/"), path));
        }
    }
}
