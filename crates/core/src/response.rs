use std::sync::{Arc, Mutex};

use http::StatusCode;

use crate::edits::{Edit, lock};
use crate::{Body, Fault, Format, Part, Request, ResponseHead, Usage};

/// How a response ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseEnd {
    /// It finished normally.
    Done,
    /// It started and then failed: truncated, or an error mid-stream.
    Failed(Fault),
    /// It was never used, e.g. a fallback 5xx or a hedge loser.
    Cancelled,
    /// The client left before it finished.
    Abandoned,
}

/// What came back from a peer: a provider's response, or the response sent
/// to the client. Live while its body streams; finished (head, end, usage) in
/// close hooks.
pub struct Response {
    head: ResponseHead,
    end: Option<ResponseEnd>,
    usage: Usage,
    /// Behind a mutex only so `Response` is `Sync`; never contended.
    live: Mutex<Option<Live>>,
}

pub(crate) enum Live {
    Provider {
        format: Arc<dyn Format>,
        body: Body,
        edits: Vec<Edit>,
        closer: Box<Closer>,
    },
    Synthetic {
        parts: Vec<Part>,
    },
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.head.status)
            .field("end", &self.end)
            .field("usage", &self.usage)
            .field("live", &lock(&self.live).is_some())
            .finish()
    }
}

impl Response {
    /// A response a plugin made itself: a cache hit, a mock, a merge.
    pub fn synthetic(head: ResponseHead, parts: Vec<Part>) -> Self {
        Self {
            head,
            end: None,
            usage: Usage::default(),
            live: Mutex::new(Some(Live::Synthetic { parts })),
        }
    }

    pub(crate) fn provider(
        head: ResponseHead,
        format: Arc<dyn Format>,
        body: Body,
        edits: Vec<Edit>,
        closer: Closer,
    ) -> Self {
        Self {
            head,
            end: None,
            usage: Usage::default(),
            live: Mutex::new(Some(Live::Provider {
                format,
                body,
                edits,
                closer: Box::new(closer),
            })),
        }
    }

    pub(crate) fn finished(head: ResponseHead, end: ResponseEnd, usage: Usage) -> Self {
        Self {
            head,
            end: Some(end),
            usage,
            live: Mutex::new(None),
        }
    }

    pub fn head(&self) -> &ResponseHead {
        &self.head
    }

    pub fn status(&self) -> StatusCode {
        self.head.status
    }

    /// How it ended. `None` while it is still live.
    pub fn end(&self) -> Option<&ResponseEnd> {
        self.end.as_ref()
    }

    /// Usage read off the stream. Empty for a response that was not read.
    pub fn usage(&self) -> &Usage {
        &self.usage
    }

    pub(crate) fn take_live(&mut self) -> Option<Live> {
        self.live
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Drop for Response {
    fn drop(&mut self) {
        if let Some(Live::Provider { closer, .. }) = self.take_live() {
            (*closer).close(Ok(Response::finished(
                self.head.clone(),
                ResponseEnd::Cancelled,
                Usage::default(),
            )));
        }
    }
}

/// A provider request that has ended, waiting for the close phase.
pub(crate) struct Closed {
    pub request: Request,
    pub response: Result<Response, Fault>,
    /// How many chain entries saw the request.
    pub seen_by: usize,
}

pub(crate) type ClosedLog = Arc<Mutex<Vec<Closed>>>;

/// Records how a provider request ended, exactly once.
pub(crate) struct Closer {
    pub log: ClosedLog,
    pub request: Request,
    pub seen_by: usize,
}

impl Closer {
    pub fn close(self, response: Result<Response, Fault>) {
        lock(&self.log).push(Closed {
            request: self.request,
            response,
            seen_by: self.seen_by,
        });
    }
}
