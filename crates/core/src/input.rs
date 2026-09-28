use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use http::StatusCode;
use http_body_util::BodyExt;

use crate::fault::guarded;
use crate::format::{AnyDecoder, AnyFormat, AnyInput, Request, Values};
use crate::lifecycle::InFlight;
use crate::plugin::{Filters, all};
use crate::{Body, Fault, codes};

/// Bytes a client sent as one message, decoded when first read.
struct Base {
    format: Arc<dyn AnyFormat>,
    /// The client's head as it arrived, which decoding reads.
    head: Arc<http::request::Parts>,
    raw: Bytes,
    /// Whether `raw` is a message, as an upgrading request's head is not.
    message: bool,
    input: OnceLock<Result<AnyInput, Fault>>,
}

/// The input that came with a request's head: the client's bytes, and the
/// holder's changed copy.
#[derive(Clone)]
pub(crate) struct Input {
    base: Arc<Base>,
    /// Set once a filter ran over it or a call brings its own; a filter's
    /// failure is kept.
    changed: Option<Result<AnyInput, Fault>>,
    /// The format of a call's own input, which `changed` then has.
    pub(crate) own_format: Option<Arc<dyn AnyFormat>>,
}

/// Whose content an input is: whose fault it is if it cannot be written.
#[derive(Clone, Copy)]
pub(crate) enum Whose {
    /// The client's, as decoded, whatever filters ran over it.
    Client,
    /// The client's, changed by a plugin.
    Edited,
    /// A plugin's own.
    Plugin,
    /// The format's own, such as an end.
    Format,
}

impl Whose {
    /// `fault`, of content that cannot be written, as whose it is.
    pub(crate) fn fault(self, fault: Fault) -> Fault {
        match self {
            Whose::Client => fault.recoded(StatusCode::BAD_REQUEST, codes::BAD_REQUEST),
            Whose::Edited | Whose::Plugin => {
                fault.recoded(StatusCode::INTERNAL_SERVER_ERROR, codes::CONFIG)
            }
            Whose::Format => fault,
        }
    }
}

/// How a call's first input goes: as the body, the client's bytes unread,
/// or as a value.
pub(crate) type Sent = (Bytes, Option<AnyInput>);

impl Input {
    /// The input in `raw`; `None` for an upgrading request's head.
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        head: Arc<http::request::Parts>,
        raw: Option<Bytes>,
    ) -> Self {
        let input = OnceLock::new();
        if raw.is_none() {
            let later =
                "an upgrading request's inputs come after its head, to filters and take_inputs";
            let _ = input.set(Err(Fault::internal(later)));
        }
        let base = Base {
            format,
            head,
            message: raw.is_some(),
            raw: raw.unwrap_or_default(),
            input,
        };
        Self {
            base: Arc::new(base),
            changed: None,
            own_format: None,
        }
    }

    /// The bytes it came in; empty if it came in none.
    pub(crate) fn raw(&self) -> &Bytes {
        &self.base.raw
    }

    /// The client's head as it arrived.
    pub(crate) fn head(&self) -> &http::request::Parts {
        &self.base.head
    }

    /// Bring `input`, a call's own, of `format`'s kind.
    pub(crate) fn bring(&mut self, format: Arc<dyn AnyFormat>, input: AnyInput) {
        self.changed = Some(Ok(input));
        self.own_format = Some(format);
    }

    /// The format whose kind it is of.
    fn format(&self) -> &Arc<dyn AnyFormat> {
        self.own_format.as_ref().unwrap_or(&self.base.format)
    }

    /// The client's input, decoded.
    fn base(&self) -> Result<&AnyInput, Fault> {
        let base = &*self.base;
        let input = base.input.get_or_init(|| self.decode());
        input.as_ref().map_err(Clone::clone)
    }

    /// The input as it is now.
    fn value(&self) -> Result<&AnyInput, Fault> {
        match &self.changed {
            Some(changed) => changed.as_ref().map_err(Clone::clone),
            None => self.base(),
        }
    }

    /// The input as `T`; another type is refused without decoding.
    pub(crate) fn get<T: Any>(&self) -> Result<&T, Fault> {
        if self.format().kind().0 != TypeId::of::<T>() {
            return Err(Fault::wrong_kind::<T>());
        }
        self.value()?
            .downcast_ref()
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    /// Run the one input of a whole request or call through `filters`, and
    /// end them.
    pub(crate) fn filter(&mut self, mut filters: Filters) -> Result<(), Fault> {
        let format = self.format().clone();
        let filtered = self.value().cloned().and_then(|input| {
            // Dropping the held copy lets an input no one shares be moved.
            drop(self.changed.take());
            let filtered = guarded(|| {
                let mut values = Some(format.values(vec![input]));
                let edited = match &mut values {
                    Some(values) => filters.run(values, &all),
                    None => Ok(()),
                };
                filters.end(edited, &mut values, &all)?;
                let inputs = values.map(|v| format.inputs(v)).unwrap_or_default();
                // Dropping a whole body's one input is refusing, `Err`'s job.
                match <[AnyInput; 1]>::try_from(inputs) {
                    Ok([input]) => Ok(input),
                    Err(_) => Err(Fault::internal(
                        "an input filter left other than one input where a whole body is one",
                    )),
                }
            });
            self.changed = Some(filtered.clone());
            filtered
        });
        // Filters that a bad body or a panic kept from their end get it here.
        let _ = filters.end(filtered.clone().map(drop), &mut None, &all);
        filtered.map(drop)
    }

    /// How it goes to a provider, relayed if `relay`, and whose it is: a
    /// plugin's or filtered input as its value, the client's if filters left
    /// it equal to its decoding; else as it came, its bytes unread if
    /// relayed.
    pub(crate) fn sent(&self, relay: bool) -> Result<(Sent, Whose), Fault> {
        let changed = self.changed.clone().transpose()?;
        let own = self.own_format.is_some() || !self.base.message;
        Ok(match (changed, own) {
            (Some(input), true) => ((Bytes::new(), Some(input)), Whose::Plugin),
            (None, true) => ((Bytes::new(), None), Whose::Format),
            (Some(input), false) => {
                let same = self.base().is_ok_and(|b| self.base.format.same(b, &input));
                let whose = if same { Whose::Client } else { Whose::Edited };
                ((Bytes::new(), Some(input)), whose)
            }
            (None, false) if relay => ((self.base.raw.clone(), None), Whose::Client),
            (None, false) => ((Bytes::new(), Some(self.base()?.clone())), Whose::Client),
        })
    }

    /// The client's request as it arrived, with `body`.
    fn request(&self, body: Bytes) -> Request {
        http::Request::from_parts((*self.base.head).clone(), body)
    }

    /// The one input of a whole request's body.
    fn decode(&self) -> Result<AnyInput, Fault> {
        let base = &self.base;
        let mut decoder = base.format.request_decoder(&self.request(base.raw.clone()));
        let mut inputs = Vec::new();
        for bytes in [Some(base.raw.clone()), None] {
            let (values, end) = decoder.decode(bytes);
            inputs.extend(values.map(|v| base.format.inputs(v)).unwrap_or_default());
            end.transpose()?;
        }
        match (inputs.pop(), inputs.is_empty()) {
            (Some(input), true) => Ok(input),
            _ => Err(Fault::internal(
                "a whole request's body decoded to other than one input",
            )),
        }
    }
}

/// An upgrading request's inputs after its head: its body, read once, and
/// the client edge's input filters.
pub(crate) struct Later {
    body: Mutex<Option<Body>>,
    pub(crate) filters: Mutex<Filters>,
}

impl Later {
    pub(crate) fn new(body: Body) -> Self {
        Self {
            body: Mutex::new(Some(body)),
            filters: Mutex::default(),
        }
    }

    /// End the client edge's input filters `end`, if they have not ended.
    pub(crate) fn end(&self, end: Result<(), Fault>) {
        // The body goes too, so nothing that outlives the request keeps the
        // client's connection.
        locked(&self.body).take();
        let _ = locked(&self.filters).end(end, &mut None, &all);
    }
}

/// Lock `m`, even if poisoned: what it guards stays consistent.
pub(crate) fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A call's or reader's filters of the inputs.
pub(crate) type InputFilters = Arc<Mutex<Filters>>;

/// One input an upgrading request sends after its head.
pub(crate) struct Frame {
    pub(crate) input: AnyInput,
    pub(crate) whose: Whose,
}

/// `later`'s inputs as they arrive, decoded as they pass, through the
/// client edge's input filters, then `call`'s. The body is taken on the
/// first poll. It stops when `cut` is.
pub(crate) fn stream(
    later: Arc<Later>,
    like: Input,
    call: InputFilters,
    cut: &Arc<InFlight>,
) -> BoxStream<'static, Result<Frame, Fault>> {
    let reading = Reading {
        later,
        body: None,
        read: false,
        format: like.base.format.clone(),
        like,
        call,
        decoder: None,
        ready: VecDeque::new(),
        ended: false,
    };
    let frames = stream::unfold(
        reading,
        |mut r| async move { r.next().await.map(|i| (i, r)) },
    );
    frames.take_until(cut.idle()).boxed()
}

/// A body being read into frames.
struct Reading {
    later: Arc<Later>,
    /// Taken from `later` on the first poll.
    body: Option<Body>,
    /// Whether it read a data frame.
    read: bool,
    format: Arc<dyn AnyFormat>,
    like: Input,
    call: InputFilters,
    /// Built at the first frame.
    decoder: Option<Box<dyn AnyDecoder>>,
    ready: VecDeque<Result<Frame, Fault>>,
    ended: bool,
}

impl Reading {
    async fn next(&mut self) -> Option<Result<Frame, Fault>> {
        loop {
            if let Some(frame) = self.ready.pop_front() {
                return Some(frame);
            }
            if self.ended {
                return None;
            }
            let body = match &mut self.body {
                Some(body) => body,
                None => match locked(&self.later.body).take() {
                    Some(body) => self.body.insert(body),
                    None => {
                        self.ended = true;
                        // Filters ended and no body: the inputs ended.
                        if locked(&self.later.filters).ended() {
                            return None;
                        }
                        let taken = "the request's inputs were taken by another reader";
                        return Some(Err(Fault::internal(taken)));
                    }
                },
            };
            let bytes = match body.frame().await {
                Some(Ok(frame)) => match frame.into_data() {
                    // Every data frame is a message, even an empty one.
                    Ok(bytes) => Ok(Some(bytes)),
                    // Trailers carry no input.
                    Err(_) => continue,
                },
                Some(Err(fault)) => Err(fault),
                None => Ok(None),
            };
            self.read |= matches!(bytes, Ok(Some(_)));
            // A failure ends the inputs, and every filter with it.
            let read = guarded(|| self.frames(bytes?)).or_else(|f| guarded(|| self.end(Err(f))));
            match read {
                Ok(frames) => self.ready.extend(frames.into_iter().map(Ok)),
                Err(fault) => {
                    self.ended = true;
                    self.ready.push_back(Err(fault));
                }
            }
        }
    }

    /// The frames of `bytes`, or of the body's end (`None`), through the
    /// filters, then at the end what they kept back.
    fn frames(&mut self, bytes: Option<Bytes>) -> Result<Vec<Frame>, Fault> {
        let (format, like) = (&self.format, &self.like);
        let decoder = (self.decoder)
            .get_or_insert_with(|| format.request_decoder(&like.request(Bytes::new())));
        let (values, end) = decoder.decode(bytes);
        let mut frames = match values {
            Some(values) => self.filter(values)?,
            None => Vec::new(),
        };
        if let Some(end) = end {
            frames.extend(self.end(end)?);
        }
        Ok(frames)
    }

    /// Run `values` through the client edge's filters, then the call's;
    /// once either ended, they are cut.
    fn filter(&mut self, values: Values) -> Result<Vec<Frame>, Fault> {
        let format = self.format.clone();
        let decoded = format.inputs(values);
        let mut frames: Vec<Frame> = Vec::with_capacity(decoded.len());
        let (mut client, mut call) = (locked(&self.later.filters), locked(&self.call));
        for input in decoded {
            if client.ended() || call.ended() {
                return Err(Fault::abandoned());
            }
            if client.is_empty() && call.is_empty() {
                frames.push(Frame {
                    input,
                    whose: Whose::Client,
                });
                continue;
            }
            let mut values = format.values(vec![input.clone()]);
            client.run(&mut values, &all)?;
            call.run(&mut values, &all)?;
            // A frame the filters left equal to its decoding is the client's.
            for changed in format.inputs(values) {
                let same = format.same(&input, &changed);
                let whose = if same { Whose::Client } else { Whose::Edited };
                frames.push(Frame {
                    input: changed,
                    whose,
                });
            }
        }
        Ok(frames)
    }

    /// The inputs end `end`: each filter list ends once, the client edge's
    /// first, and what they kept back follows.
    fn end(&mut self, end: Result<(), Fault>) -> Result<Vec<Frame>, Fault> {
        self.ended = true;
        let mut tail = None;
        // A list that ended already keeps its end.
        let end = locked(&self.later.filters).end(end, &mut tail, &all);
        let mut call = locked(&self.call);
        let (mut tail, end) = match tail {
            Some(mut values) => match guarded(|| call.run(&mut values, &all)) {
                Ok(()) => (Some(values), end),
                Err(fault) => (None, Err(fault)),
            },
            None => (None, end),
        };
        call.end(end, &mut tail, &all)?;
        let inputs = tail.map(|v| self.format.inputs(v)).unwrap_or_default();
        let made = inputs.into_iter().map(|input| Frame {
            input,
            whose: Whose::Plugin,
        });
        Ok(made.collect())
    }
}

/// A reader that read no data frame of inputs still open gives the body
/// back, so another, such as a retry, reads every input.
impl Drop for Reading {
    fn drop(&mut self) {
        let live = !locked(&self.later.filters).ended();
        if let Some(body) = self.body.take().filter(|_| !self.read && live) {
            *locked(&self.later.body) = Some(body);
        }
    }
}
