use std::sync::{Mutex, MutexGuard};

use crate::edits::{Edit, apply_once, lock};
use crate::store::{Class, Key, Scope};
use crate::{Doc, Edits, Envelope, Fault, Part, Target};

/// Who is calling. An auth plugin attaches it with
/// [`crate::Context::insert_ext`]; tenant-scoped store keys read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub id: String,
    pub tenant: String,
}

/// A group of client requests that share state. For now every request is a
/// session of one; sockets, jobs and sealed ids come later.
#[derive(Debug, Clone)]
pub struct Session {
    pub(crate) id: String,
}

impl Session {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn key(&self, class: Class, name: impl Into<String>) -> Key {
        Key {
            scope: Scope::Session(self.id.clone()),
            class,
            name: name.into(),
        }
    }
}

/// Who a request is exchanged with.
#[derive(Debug, Clone)]
pub enum Peer {
    /// The request came from a client.
    Client,
    /// The request goes to this provider.
    Provider(Target),
}

/// A message exchanged with a peer: the client's request to the gateway, or
/// one of the gateway's requests to a provider. Both have a format, a body,
/// and two streams to edit: the request on its way to whoever answers it, and
/// the response on its way back.
pub struct Request {
    peer: Peer,
    format: &'static str,
    envelope: Envelope,
    index: usize,
    body: Mutex<Doc>,
    request: Edits,
    response: Edits,
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("peer", &self.peer)
            .field("format", &self.format)
            .field("envelope", &self.envelope)
            .field("index", &self.index)
            .finish()
    }
}

impl Request {
    pub(crate) fn new(
        peer: Peer,
        format: &'static str,
        envelope: Envelope,
        body: Doc,
        index: usize,
    ) -> Self {
        Self {
            peer,
            format,
            envelope,
            index,
            body: Mutex::new(body),
            request: Edits::default(),
            response: Edits::default(),
        }
    }

    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    pub fn is_client(&self) -> bool {
        matches!(self.peer, Peer::Client)
    }

    /// The provider this request goes to, if it is a provider request.
    pub fn provider(&self) -> Option<&Target> {
        match &self.peer {
            Peer::Provider(t) => Some(t),
            Peer::Client => None,
        }
    }

    /// The name of the Format this request is in.
    pub fn format(&self) -> &str {
        self.format
    }

    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    pub fn model(&self) -> Option<&str> {
        self.envelope.model.as_deref()
    }

    pub fn is_stream(&self) -> bool {
        self.envelope.stream
    }

    /// For provider requests: 0 for the first one of this client request,
    /// then 1, 2... Always 0 for the client request.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The message, in this request's format. Do not hold the guard across
    /// an `.await`.
    pub fn body(&self) -> MutexGuard<'_, Doc> {
        lock(&self.body)
    }

    /// Edits to the request on its way to whoever answers it.
    pub fn request(&self) -> &Edits {
        &self.request
    }

    /// Edits to the response on its way back.
    pub fn response(&self) -> &Edits {
        &self.response
    }

    /// Apply pending request edits to the body, once.
    pub(crate) fn apply_request_edits(&self) -> Result<(), Fault> {
        let edits = self.request.take();
        if edits.is_empty() {
            return Ok(());
        }
        let mut body = self.body();
        let mut part = Part::Doc(std::mem::replace(&mut *body, Doc::new(bytes::Bytes::new())));
        let result = apply_once(&mut part, edits);
        if let Part::Doc(d) = part {
            *body = d;
        }
        result
    }

    pub(crate) fn take_response_edits(&self) -> Vec<Edit> {
        self.response.take()
    }
}
