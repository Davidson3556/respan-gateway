#![warn(missing_docs)]
//! What plugins, vendors and hosts share beyond the core: a model's card
//! ([`Model`]), what it reads and can do, and what it costs ([`Price`]).
//!
//! A vendor fills a [`Model`] from its list; a host keeps them as its
//! [`Catalog`], its own declarations laid over its providers' lists
//! ([`Model::overlay`]), and writes them for its clients ([`Model`]
//! serializes as a `/v1/models` entry, beside its `id` and `object`); a
//! router ranks and filters by them, and a pricing plugin charges by their
//! [`Price`], each reading the same catalog.
//!
//! It is a leaf: it depends on no other crate of the gateway's.

mod catalog;
mod model;
mod price;

pub use catalog::Catalog;
pub use model::{Capability, Modality, Model, time};
pub use price::Price;
