use std::marker::PhantomData;

/// Scope marker: the request as the client sent it, once per Turn.
#[derive(Debug)]
pub struct Req;

/// Scope marker: one provider call. A Turn may have several.
#[derive(Debug)]
pub struct Attempt;

/// One request/response unit and the plugin's only handle.
///
/// Every shape (unary, stream, job, session) is a sequence of Turns grouped
/// into an Exchange. Closes into exactly one Record.
#[derive(Debug)]
pub struct Turn<S> {
    _scope: PhantomData<S>,
}

/// The capability to start provider attempts. Only a route's source gets one.
#[derive(Debug)]
pub struct Upstream {
    _private: (),
}

/// A running response. The Call a source returns is the commit.
#[derive(Debug)]
pub struct Call {
    _private: (),
}
