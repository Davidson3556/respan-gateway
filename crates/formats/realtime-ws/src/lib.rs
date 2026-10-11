#![warn(missing_docs)]
//! The realtime kind over WebSocket: a session of JSON events, the kind's
//! `Input` in and `Output` out, as formats on the HTTP wire, one per spec:
//!
//! - [`OpenAiRealtime`]: OpenAI Realtime, `openai-realtime`.
//! - [`GeminiLive`]: Gemini Live, `gemini-live`.
//! - [`ResponsesWs`]: OpenAI's Responses over WebSocket, `responses-ws`,
//!   which is not realtime: it streams Responses over a socket, and uses the
//!   kind's types only as native events.
//!
//! [`OpenAiRealtime`] and [`GeminiLive`] decode each frame into the kind's
//! events, keeping what the kind has no place for as their own (an event's
//! `native`, or a native event), and write them back: a relay to the same
//! format keeps what each event says, not its exact bytes. Each writes the
//! other's events too. Audio is converted where a spec cannot carry it, and
//! what one spec has and the other has not is emulated, dropped where it
//! cannot change the answer, or refused: an input `bad_request`, closing
//! the client 1008; an output `upstream`. [`ResponsesWs`] decodes every
//! frame to the kind's native event of its name, and writes no other
//! format's. A frame that is not JSON fails: the client's, `bad_request`;
//! the provider's, `upstream`. A handshake one refuses is answered
//! text/plain with the fault's error code, never its message.

mod audio;
mod foreign;
mod gemini;
mod json;
mod openai;
mod responses;
mod session;

pub use gemini::GeminiLive;
pub use openai::OpenAiRealtime;
pub use responses::ResponsesWs;
