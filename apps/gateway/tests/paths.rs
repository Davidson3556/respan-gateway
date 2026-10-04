//! Gateway path matching and passthrough, using local wire fixtures.

#[path = "paths/support.rs"]
mod path_support;
mod support;

#[path = "paths/allowlist.rs"]
mod allowlist;
#[path = "paths/case_matching.rs"]
mod case_matching;
#[path = "paths/client_paths.rs"]
mod client_paths;
#[path = "paths/encoded_traversal.rs"]
mod encoded_traversal;
#[path = "paths/normalization.rs"]
mod normalization;
#[path = "paths/segments.rs"]
mod segments;
#[path = "paths/spellings.rs"]
mod spellings;
#[path = "paths/suffixes.rs"]
mod suffixes;
