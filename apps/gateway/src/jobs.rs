//! Video jobs' follow-ups. A request on a job (retrieve, content, remix,
//! delete) goes to the provider that made it, which only the job's id says:
//! by its spelling (OpenAI's start `video_`, Veo's `veo_`), or, when several
//! providers spell their ids alike, by the provider's name the gateway
//! wrapped them in on the way out, `<provider>~<id>`, and unwraps on the
//! way in.

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_media::video::{Input, Output};

use crate::compose::Upstream;
use crate::formats::KnownFormat;

const SEPARATOR: char = '~';

/// A core provider of a video format.
#[derive(Debug)]
struct Maker {
    provider: String,
    internal: String,
    /// What its jobs' ids start with.
    spelling: &'static str,
    /// Whether another provider's ids are spelled alike.
    shared: bool,
}

/// The plugin that sends a follow-up to its job's provider, after routing.
#[derive(Debug)]
pub struct VideoJobs {
    makers: Vec<Maker>,
}

impl VideoJobs {
    pub fn new(upstreams: &[Upstream], formats: &[KnownFormat]) -> Self {
        let spelling = |format: &str| {
            let known = formats.iter().find(|f| f.name == format);
            known.and_then(|f| f.job_ids)
        };
        let mut makers: Vec<Maker> = upstreams
            .iter()
            .flat_map(|u| u.entries.iter().map(move |e| (u, e)))
            .filter_map(|(u, e)| {
                Some(Maker {
                    provider: u.name.clone(),
                    internal: e.internal.clone(),
                    spelling: spelling(&e.format)?,
                    shared: false,
                })
            })
            .collect();
        let spellings: Vec<&str> = makers.iter().map(|m| m.spelling).collect();
        for maker in &mut makers {
            maker.shared = spellings.iter().filter(|s| **s == maker.spelling).count() > 1;
        }
        Self { makers }
    }

    /// Who made job `id`, and its id as they know it: the provider it is
    /// wrapped in, else the target if it spells ids so, else the first that
    /// does.
    fn maker<'a>(&self, id: &'a str, target: Option<&str>) -> Option<(&Maker, &'a str)> {
        if let Some((provider, inner)) = id.split_once(SEPARATOR)
            && let Some(maker) = self.makers.iter().find(|m| m.provider == provider)
        {
            return Some((maker, inner));
        }
        let mut spelled = self.makers.iter().filter(|m| id.starts_with(m.spelling));
        let targeted = spelled
            .clone()
            .find(|m| target == Some(m.internal.as_str()));
        targeted.or_else(|| spelled.next()).map(|m| (m, id))
    }
}

#[async_trait]
impl Plugin for VideoJobs {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Ok(input) = cx.input::<Input>() else {
            return Ok(None);
        };
        let found = input.job_id().and_then(|id| {
            let (maker, inner) = self.maker(id, cx.target())?;
            Some((
                maker.internal.clone(),
                (inner != id).then(|| inner.to_owned()),
            ))
        });
        if let Some((internal, unwrapped)) = found {
            cx.set_target(internal);
            if let Some(inner) = unwrapped {
                cx.filter(move |input: &mut Input| {
                    set_job_id(input, &inner);
                    Ok(())
                })?;
            }
        }
        let target = cx.target();
        let maker = self.makers.iter().find(|m| target == Some(&m.internal));
        if let Some(maker) = maker.filter(|m| m.shared) {
            let provider = maker.provider.clone();
            cx.filter(move |output: &mut Output| {
                wrap_ids(output, &provider);
                Ok(())
            })?;
        }
        Ok(None)
    }
}

fn set_job_id(input: &mut Input, inner: &str) {
    match input {
        Input::Remix { id, .. }
        | Input::Retrieve { id, .. }
        | Input::Content { id, .. }
        | Input::Delete { id, .. } => *id = inner.to_owned(),
        _ => {}
    }
}

fn wrap_ids(output: &mut Output, provider: &str) {
    let wrap = |id: &mut String| *id = format!("{provider}{SEPARATOR}{id}");
    match output {
        Output::Job(job) => wrap(&mut job.id),
        Output::Jobs(jobs) => jobs.jobs.iter_mut().for_each(|job| wrap(&mut job.id)),
        Output::Deleted(id) => wrap(id),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jobs() -> VideoJobs {
        let maker = |provider: &str, internal: &str, spelling, shared| Maker {
            provider: provider.into(),
            internal: internal.into(),
            spelling,
            shared,
        };
        VideoJobs {
            makers: vec![
                maker("east", "east:openai-videos", "video_", true),
                maker("west", "west:openai-videos", "video_", true),
                maker("gemini", "gemini:gemini-veo", "veo_", false),
            ],
        }
    }

    fn maker(id: &str, target: Option<&str>) -> Option<(String, String)> {
        let jobs = jobs();
        let (maker, inner) = jobs.maker(id, target)?;
        Some((maker.provider.clone(), inner.to_owned()))
    }

    #[test]
    fn a_job_goes_to_its_maker() {
        let to = |provider: &str, id: &str| Some((provider.to_owned(), id.to_owned()));
        assert_eq!(maker("west~video_1", None), to("west", "video_1"));
        assert_eq!(maker("veo_abc", Some("east")), to("gemini", "veo_abc"));
        assert_eq!(maker("video_1", None), to("east", "video_1"));
        assert_eq!(
            maker("video_1", Some("west:openai-videos")),
            to("west", "video_1")
        );
        assert_eq!(maker("nobody~video_1", None), None);
        assert_eq!(maker("file_1", None), None);
    }

    #[test]
    fn ids_are_wrapped_in_their_provider() {
        let mut deleted = Output::Deleted("video_1".into());
        wrap_ids(&mut deleted, "east");
        assert_eq!(deleted, Output::Deleted("east~video_1".into()));
        let mut input = Input::Retrieve {
            id: "east~video_1".into(),
        };
        set_job_id(&mut input, "video_1");
        assert_eq!(input.job_id(), Some("video_1"));
    }
}
