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
//! Each decodes every frame to the kind's native event of its name, and
//! writes only its own: a relay to the same format keeps what each event
//! says, not its exact bytes. A frame that is not JSON fails: the client's,
//! `bad_request`; the provider's, `upstream`. A handshake one refuses is
//! answered text/plain with the fault's error code, never its message.

mod gemini;
mod openai;
mod responses;
mod session;

pub use gemini::GeminiLive;
pub use openai::OpenAiRealtime;
pub use responses::ResponsesWs;
