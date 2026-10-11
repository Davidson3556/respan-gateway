//! The video kind: video generation as a job, created, then polled, then
//! downloaded.
//!
//! Each request is one operation on a job ([`Input`]); a response is a
//! sequence of events: a [`Output::Job`] for one job's state, a
//! [`Output::Jobs`] for a page of them, the content's bytes in
//! [`Output::Content`] chunks, or [`Output::Deleted`], with
//! [`Output::Native`] among them.
//!
//! A job's id is the provider's, opaque, as a client gives it back: a
//! follow-up (`Retrieve`, `Content`, `Remix`, `Delete`) has to reach the
//! provider that made the job, which the id alone says only by its spelling
//! ([`Input::job_id`]). Some providers serve a finished job's content only at
//! the URLs it names ([`Job::videos`]); [`LocateContent`] finds them first.

use bytes::Bytes;
use respan_gateway_conversation::{Media, MediaSource};
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::{
    BadRequest, ClientContext, ErrorCode, Fault, FormatId, NativeFields, NativeValue, Plugin,
    Response, Upstream, async_trait,
};
use serde_json::Value;

/// The kind's input: one operation on a video job.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Input {
    /// Start a job making a video.
    Create(Create),
    /// Start a job remaking the finished video `id` as `prompt` says.
    Remix {
        /// The video to remake.
        id: String,
        /// What to change.
        prompt: String,
        /// The request's fields the kind has no place for.
        native: Option<NativeFields>,
    },
    /// A job's state.
    Retrieve {
        /// The job.
        id: String,
    },
    /// A finished job's content.
    Content {
        /// The job.
        id: String,
        /// Which of its content.
        variant: Variant,
        /// Where the provider serves it, if the job named it
        /// ([`Job::videos`]); a provider that serves content only there
        /// needs it.
        url: Option<String>,
    },
    /// Delete a job and its content.
    Delete {
        /// The job.
        id: String,
    },
    /// A page of jobs, newest first unless `order` says otherwise.
    List {
        /// The id of the job the page starts after.
        after: Option<String>,
        /// How many jobs, at most.
        limit: Option<u32>,
        /// `asc` or `desc`, by when they were made.
        order: Option<String>,
    },
}

impl Input {
    /// The model a `Create` asks for; no other operation names one.
    pub fn model(&self) -> Option<&str> {
        match self {
            Input::Create(create) => create.model.as_deref(),
            _ => None,
        }
    }

    /// Asks for `model`, if a `Create`; no other operation names one, so
    /// the rest are left as they are.
    pub fn set_model(&mut self, model: String) {
        if let Input::Create(create) = self {
            create.model = Some(model);
        }
    }

    /// The job an operation is on: `None` for `Create` and `List`.
    pub fn job_id(&self) -> Option<&str> {
        match self {
            Input::Remix { id, .. }
            | Input::Retrieve { id }
            | Input::Content { id, .. }
            | Input::Delete { id } => Some(id),
            Input::Create(_) | Input::List { .. } => None,
        }
    }
}

/// A request for a video.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Create {
    /// The model asked for, if any; without one, the provider's default.
    pub model: Option<String>,
    /// What to show.
    pub prompt: String,
    /// How long, in seconds.
    pub seconds: Option<u32>,
    /// Its size, width by height, such as `1280x720`.
    pub size: Option<String>,
    /// An image to start from.
    pub reference: Option<Media>,
    /// The request's fields the kind has no place for.
    pub native: Option<NativeFields>,
}

impl Create {
    /// A video of `prompt`.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            ..Self::default()
        }
    }
}

/// Which of a finished job's content.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Variant {
    /// The video.
    #[default]
    Video,
    /// A still of it.
    Thumbnail,
    /// Stills of it in one image.
    Spritesheet,
}

/// The kind's output: one event of a response.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
// A response holds one job at most: its size costs nothing.
#[allow(clippy::large_enum_variant)]
pub enum Output {
    /// A job's state.
    Job(Job),
    /// A page of jobs.
    Jobs(Jobs),
    /// The next bytes of the content.
    Content(Bytes),
    /// The job deleted.
    Deleted(String),
    /// An event the kind has no form for, as `format` has it. Sent only in
    /// that format.
    Native(NativeValue),
}

impl Output {
    /// An event of `format` the kind has no form for.
    pub fn native(format: impl Into<FormatId>, raw: impl Into<Value>) -> Self {
        Output::Native(NativeValue::new(format, raw))
    }
}

/// A video job's state.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Job {
    /// Its id, as the provider's format gives it.
    pub id: String,
    /// Where it is.
    pub status: Status,
    /// The model making it.
    pub model: Option<String>,
    /// How far along, in percent.
    pub progress: Option<u32>,
    /// When it was made, in seconds since the Unix epoch.
    pub created_at: Option<u64>,
    /// When it finished.
    pub completed_at: Option<u64>,
    /// When its content will be gone.
    pub expires_at: Option<u64>,
    /// The video's length, in seconds.
    pub seconds: Option<u32>,
    /// The video's size, width by height.
    pub size: Option<String>,
    /// Why it failed.
    pub error: Option<JobError>,
    /// Where the provider serves the videos it made, if it names them.
    pub videos: Vec<Media>,
    /// Its fields the kind has no place for.
    pub native: Option<NativeFields>,
}

impl Job {
    /// The job `id`, at `status`.
    pub fn new(id: impl Into<String>, status: Status) -> Self {
        Self {
            id: id.into(),
            status,
            model: None,
            progress: None,
            created_at: None,
            completed_at: None,
            expires_at: None,
            seconds: None,
            size: None,
            error: None,
            videos: Vec::new(),
            native: None,
        }
    }

    /// The URL of its first video, if the provider named one.
    pub fn video_url(&self) -> Option<&str> {
        self.videos.iter().find_map(|media| match &media.source {
            MediaSource::Url(url) => Some(url.as_str()),
            MediaSource::Bytes(_) => None,
        })
    }
}

/// Where a job is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Status {
    /// Waiting to start.
    Queued,
    /// Being made.
    InProgress,
    /// Made: its content is ready.
    Completed,
    /// Given up ([`Job::error`]).
    Failed,
    /// A state the kind has no form for, as `format` names it; only a
    /// response in that format can say it.
    #[non_exhaustive]
    Native {
        /// The format that names it.
        format: FormatId,
        /// The state, as that format names it.
        status: String,
    },
}

impl Status {
    /// The state `status` of `format`, which the kind has no form for.
    pub fn native(format: impl Into<FormatId>, status: impl Into<String>) -> Self {
        Status::Native {
            format: format.into(),
            status: status.into(),
        }
    }
}

/// Why a job failed.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct JobError {
    /// What failed, as the provider names it.
    pub code: Option<String>,
    /// What the provider says of it.
    pub message: String,
}

impl JobError {
    /// A failure the provider says `message` of.
    pub fn new(code: Option<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// A page of jobs.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Jobs {
    /// The jobs, in the order asked.
    pub jobs: Vec<Job>,
    /// Whether more follow the last.
    pub has_more: bool,
    /// Its fields the kind has no place for.
    pub native: Option<NativeFields>,
}

impl Jobs {
    /// A page of `jobs`.
    pub fn new(jobs: Vec<Job>, has_more: bool) -> Self {
        Self {
            jobs,
            has_more,
            native: None,
        }
    }
}

/// For a provider that serves a finished job's content only at the URLs
/// the job names ([`Job::videos`]), as Veo does: a `Content` request without
/// a `url` whose target is one of its providers first retrieves the job
/// from it, then goes on with the URL of the job's first video. A job not
/// finished is refused, 400, as content not ready; register it after the
/// router that picks the target.
#[derive(Debug, Clone, Default)]
pub struct LocateContent {
    providers: Vec<String>,
}

impl LocateContent {
    /// For no provider yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// For `provider` too.
    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.providers.push(provider.into());
        self
    }
}

#[async_trait]
impl Plugin for LocateContent {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        // Only the video itself is at a URL; a provider refuses a still.
        let Ok(Input::Content {
            id,
            variant: Variant::Video,
            url: None,
        }) = cx.input::<Input>()
        else {
            return Ok(None);
        };
        let Some(target) = cx
            .target()
            .filter(|t| self.providers.iter().any(|p| p == t))
        else {
            return Ok(None);
        };
        let (id, target) = (id.clone(), target.to_owned());
        let retrieved = cx.call(target).with_input(Input::Retrieve { id }).await?;
        let mut outputs = retrieved.outputs::<Output>();
        let mut job = None;
        while let Some(output) = outputs.next().await {
            if let Output::Job(found) = output? {
                job = Some(found);
            }
        }
        let job = job.ok_or_else(|| Upstream::fault("the provider retrieved no job"))?;
        let url = match (&job.status, job.video_url()) {
            (Status::Completed, Some(url)) => url.to_owned(),
            (Status::Completed, None) => {
                return Err(Upstream::fault(
                    "the provider's finished job names no video",
                ));
            }
            (status, _) => {
                return Err(BadRequest::fault(format!(
                    "the video is not ready: its job is {status:?}"
                ))
                .with(ErrorCode("video_not_ready".into())));
            }
        };
        cx.filter(move |input: &mut Input| {
            if let Input::Content { url: at, .. } = input {
                *at = Some(url.clone());
            }
            Ok(())
        })?;
        Ok(None)
    }
}
