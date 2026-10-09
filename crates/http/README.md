# HTTP bindings

`respan-gateway-http` connects native HTTP messages to core's streams and sinks.
It is for protocol authors, separate from the plugin SDK. Concrete vendor formats
live in `respan-gateway-openai` and `respan-gateway-anthropic`; MCP and WebSockets are optional integrations.

A format implements core directly in two orientations:

- `Format<Incoming, Input, Output>` accepts a request and writes its response.
- `Format<Outgoing, Output, Input>` sends a request and reads its response.

`Incoming` owns one accepted exchange. `Outgoing` owns one provider attempt.
Their `bind`s mirror each other, and a format gives each the same three
things in the order they happen on its edge:

| | `Incoming::bind` | `Outgoing::bind` |
| --- | --- | --- |
| first | decode the request into an `EventStream` | encode the request: its head and body `Encoder` |
| then | encode the response into an `EventSink`, from a `Reply` | decode the response into an `EventStream` |
| on failure | write the error body (`plain` writes text) | read the error answer (`provider_fault` reads none of it) |

The bindings own everything else: response handoff, backpressure and
completion, whole or streamed uploads, what a proxy strips from a call, a
fault's status and `Retry-After`, refused answers read whole into a fault
before the decoder could see them, and no body where HTTP has none. An answer
succeeds by HTTP's rule, a 2xx or an asked-for 101, unless the format says
otherwise (`Outgoing::with_success`). There is no additional HTTP format trait
or implementation macro.

The `formats::opaque` module owns the `Input<F>` / `Output<F>` passthrough
pair, its four functions, and the `Passthrough` format.

## A small format

This format sends UTF-8 questions to `POST /echo` and receives byte answers:

```rust
use respan_gateway_http as h;
use h::bytes::Bytes;
use h::formats::{self, whole};
use h::http::{Method, Uri};
use respan_gateway_sdk::{BadRequest, Connection, Fault, Format, Result};

#[derive(Clone, Copy)]
struct Echo;

impl Format<h::Incoming, String, Vec<u8>> for Echo {
    fn name(&self) -> &str { "echo" }

    fn matches(&self, io: &h::Incoming) -> bool {
        io.request().method() == Method::POST
            && io.request().uri().path() == "/echo"
    }

    fn bind(&self, io: h::Incoming) -> Result<Connection<String, Vec<u8>>> {
        Ok(io.bind(
            |request| h::messages(request, |_, bytes| {
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| BadRequest::fault("expected UTF-8"))
            }),
            |reply| {
                let head = reply.head();
                Ok(reply.write(head, formats::each(|answer: &Vec<u8>| {
                    Ok(Bytes::copy_from_slice(answer))
                })))
            },
            h::plain,
        ))
    }
}

impl Format<h::Outgoing, Vec<u8>, String> for Echo {
    fn name(&self) -> &str { "echo" }

    fn bind(&self, io: h::Outgoing) -> Result<Connection<Vec<u8>, String>> {
        Ok(io.bind(
            |head, question: Option<&String>| {
                question.ok_or_else(|| Fault::config("echo takes one question"))?;
                head.method = Method::POST;
                head.uri = Uri::from_static("/echo");
                Ok(formats::each(|question: &String| Ok(Bytes::from(question.clone()))))
            },
            |response, _sent| whole::read(response.into_body(), |bytes, out| {
                out.push(bytes.to_vec());
                Ok(())
            }),
            |answer| h::provider_fault("echo", answer),
        ))
    }
}

// The same encoder primitive works synchronously without a connection/runtime.
let bytes = h::encode_all(
    formats::each(|value: &String| Ok(Bytes::from(value.clone()))),
    &["hello".to_owned()],
)?;
assert_eq!(bytes, Bytes::from_static(b"hello"));
# Ok::<(), Fault>(())
```

For stateful output, implement the small `Encoder<T>` interface: `encode` writes
one value and `end` writes final success or error framing. It performs no I/O,
chooses no headers and does not control when a response opens. Simple formats
can use functions such as `formats::each`; stateful SSE formats keep their own
encoding state. Response decoders return event streams, with whole-body
(`whole::read`), SSE (`sse::read`) and generic frame (`formats::parse`)
helpers.

## Bodies and preparation

`Body` represents complete bytes or a live byte stream and implements both
`Stream<Item = Result<Bytes, Fault>>` and `http_body::Body`. Network chunks are
not semantic messages: accepted ordinary HTTP requests are collected, up to
the adapter's limit, before opening, while explicitly live/message ingress
remains streaming.

Use the native `http::request::Parts` and `http::response::Parts` for heads;
`http::Request::new(()).into_parts().0` and
`http::Response::new(()).into_parts().0` make empty heads. For an external
HTTP body, `http_body_util::BodyExt` supplies the standard data-frame stream:
`request.map(|body| Body::stream(body.map_err(Into::into).into_data_stream()))`
(and likewise for a response). This drops trailers, as gateway wire frames
carry data only. `BodyExt::collect(body).await?.to_bytes()` collects a streamed
body, retaining its frames until EOF. Incoming request accumulation instead
folds frames into `BytesMut`, releasing each before polling again so pooled
buffer producers can progress. A `Body` with `full_bytes()` can be cloned
without polling.

Keep `Body::full` for finite, inspectable bytes, and `Body::stream`
for pull-driven frames. Unlike a generic full or stream adapter, `Body` also
carries a private upload handoff for delivery and cancellation, and, for a
provider's answer, its transport's `limit`: the most bytes a reader holds of
it at once. Those gateway semantics are not replaced by standard body
utilities; ordinary conversions and collection need no additional facade.

`messages` decodes a complete body lazily or maps explicitly live message
frames. The outgoing binding encodes the request:

- One input is encoded, and finalized, synchronously. Failure occurs
  before the provider attempt is marked sent.
- Live input stays caller-owned. The sink encodes each submitted value with
  byte-reader backpressure, not an unbounded event queue.

The binding then snapshots the actual sent head and whole bytes for the
response decoder. Client negotiation and error writing instead receive the
client's request as it came, its `Wire`. A plugin can explicitly bypass
whole-body encoding with `SendRaw(Wire)`. For live input, this supplies the
handshake head while typed events continue through the encoder. An `Incoming`
publishes its `Wire`, `ClientHead` and the client's `Credential` apart
(`Incoming::extensions`); credentials must be copied deliberately.

A plugin attaches a `ProviderHead` slot to a call and copies its result into the
exchange's `ClientHead` to forward a response head. Otherwise `Output::head()`
returns a fresh success head. Provider failures carry `ProviderError` for
inspection; only an explicit `RawReply` opts into writing their original body,
which a call's `RawErrors` asks the binding for.

The incoming response handle constructs real sinks:

- `write` normally waits for bytes or completion before publishing the head.
  Explicitly live (`Framing::Live`) ingress retains immediate response
  opening, so a duplex peer can send without waiting for its own first reply.
- `open` publishes a quiet stream immediately, optionally with a prefix.
- Either writes no body where HTTP has none (`bodiless`: to a `HEAD`, or a
  204, 205 or 304): the head goes at once, the encoder is never called, and a
  source not yet read is stopped. Stopping retains source ownership until
  delivery settles.

Finite offline encoding uses `encode_all`; offline parsing reuses synchronous
parser operations rather than blocking on an async stream. Providers expose
finite response conversion helpers for applications such as durable jobs.

## Backends and completion

`connect(send, limit)` creates a provider factory from an async function
accepting `http::Request<Body>` and returning
`(http::Response<Body>, Option<Completion>)`; `limit` is the most bytes a
reader holds of one answer at once, past which it fails `upstream`. The
optional Reqwest `HttpClient` also supplies a `.connect()` convenience.
Backends can share connection pools; each factory invocation owns a fresh
attempt. A transport prepares a call with `prepare_call` against a base URL
`base_url` checked; the binding has already stripped what a proxy must not
pass on, so a custom transport cannot forward it by omission.

`Some(completion)` reports outgoing writing independently of the response:
response headers and response EOF do not shorten that wait. `None` means the
sender accepted the outgoing handoff; a retained byte reader may still pull live
input. The binding coordinates that reader with response termination. Refusal
cancels unfinished upload rather than waiting for completion.

Flush advances quiet transfers; close finalizes framing and waits for actual
delivery. Source faults may be encoded into terminal framing but cannot turn
an interrupted upload into successful EOF. Dropping a response cancels its work.

An answer succeeds by HTTP's rule, 2xx or a requested 101, unless the format
says otherwise (`Outgoing::with_success`): strict WebSocket formats require
the handshake. An answer is refused, with the fault its format's error reader
makes of its whole body, read up to the limit, if and only if it is no success; a success opens at
its head, and a fault its body says later fails it. Parsing helpers preserve
semantic decoder failures separately from broken wire bodies and avoid
unbounded prefetch across wire frames.
