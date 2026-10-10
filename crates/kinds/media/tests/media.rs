//! The media kinds relayed through their OpenAI formats to strict local
//! fakes of OpenAI's media endpoints.

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)]
mod support;

#[path = "media/images.rs"]
mod images;
#[path = "media/routing.rs"]
mod routing;
#[path = "media/speech.rs"]
mod speech;
#[path = "media/transcriptions.rs"]
mod transcriptions;
