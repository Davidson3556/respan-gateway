use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::fault::guarded;
use crate::format::Values;
use crate::{ClientContext, Fault, ProviderContext, Response};

/// How a request ([`Plugin::on_client_end`]) or one of its calls
/// ([`Plugin::on_provider_end`]) ended.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Outcome {
    /// `Ok` if the response was read to its end: delivered, or on the
    /// provider edge read by whoever read it. `Err` with the fault that
    /// refused, failed or stopped it: a provider's error its format read
    /// ([`Fault::is_answer`] if the provider answered with it), an internal
    /// fault for a panic, or [`codes::ABANDONED`](crate::codes::ABANDONED)
    /// if it was dropped or cut off first. A fault's message may carry a
    /// provider's words.
    pub end: Result<(), Fault>,
}

/// Ended `Ok`.
impl Default for Outcome {
    fn default() -> Self {
        Self { end: Ok(()) }
    }
}

/// The per-request extension point, on two edges: the client edge, once per
/// request, and the provider edge, once per call the request makes. A
/// plugin works on a kind through its types ([`ClientContext::input`],
/// [`ClientContext::filter`]), whatever the request's format.
///
/// Plugins registered with [`Gateway::plugin`](crate::Gateway::plugin) see
/// all traffic, in one order on both edges. A plugin for one kind lets
/// other traffic pass, or refuses it with a status of its own:
///
/// ```ignore
/// if !cx.is_kind::<Conversation>() {
///     return Ok(None);
/// }
/// cx.filter(|chat: &mut Conversation| {
///     chat.temperature = Some(0.0);
///     Ok(())
/// })?;
/// ```
///
/// To race calls, answer with the first read to its content
/// ([`Response::until_content`]), or to its end ([`Response::until_end`]),
/// and drop the others, which end
/// [`codes::ABANDONED`](crate::codes::ABANDONED). To tell the winner in
/// `on_provider_end`, give each call a flag
/// ([`Call::with_extension`](crate::Call::with_extension)) set once read:
///
/// ```ignore
/// #[derive(Clone, Default)]
/// struct Won(Arc<AtomicBool>);
///
/// let race = ["a", "b"].map(|provider| {
///     let won = Won::default();
///     let call = cx.call(provider).with_extension(won.clone());
///     Box::pin(async move {
///         let resp = call.await?.until_content().await?;
///         won.0.store(true, Ordering::Relaxed);
///         Ok::<_, Fault>(resp)
///     })
/// });
/// let (resp, _) = futures::future::select_ok(race).await?;
/// // In on_provider_end: px.extensions().get::<Won>()
/// ```
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// The client edge, once per request, at its head. `Ok(None)` passes to
    /// the next plugin; `Ok(Some(response))` answers, skipping the rest;
    /// `Err` refuses, as the client's format writes the fault. If all pass,
    /// the target is called ([`ClientContext::set_target`]); with none, the
    /// request is refused [`codes::NOT_FOUND`](crate::codes::NOT_FOUND).
    ///
    /// To fall back, answer with the first call whose response reads to its
    /// content ([`Response::until_content`]), else the last one's failure,
    /// which carries a provider's answer.
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        Ok(None)
    }

    /// Once per request, after the response ended and every call's
    /// `on_provider_end`, for each plugin whose `on_client` started. Calls
    /// still pending or open end [`codes::ABANDONED`](crate::codes::ABANDONED)
    /// first. Hooks run in order, detached, on the tokio runtime that ran
    /// [`Gateway::handle`](crate::Gateway::handle); a panic does not stop the
    /// others, and [`Gateway::idle`](crate::Gateway::idle) waits for all.
    /// `cx.target()` is the last target set.
    async fn on_client_end(&self, _cx: &ClientContext, _outcome: &Outcome) {}

    /// The provider edge, before every call the request makes, for each
    /// plugin in order until one refuses, whether or not its `on_client`
    /// ran, without waiting for the end hooks of the request's earlier
    /// calls, so a limit reserves here and settles in its end hook. `Err`
    /// fails the call before anything is sent. It may stop at any await,
    /// such as when a raced call is dropped, and its `on_provider_end` still
    /// runs, so record what it reserves before awaiting.
    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        Ok(())
    }

    /// Once per call whose `on_provider` started for this plugin: when the
    /// call fails, its response's end is decoded, or its reader stops at a
    /// failure, whoever reads it. One dropped or cut off first, as when its
    /// request ends, ends [`codes::ABANDONED`](crate::codes::ABANDONED). Runs
    /// like `on_client_end`, before it.
    async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {}
}

/// A shared plugin; it forwards every method.
#[async_trait]
impl<P: Plugin + ?Sized> Plugin for Arc<P> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        (**self).on_client(cx).await
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        (**self).on_client_end(cx, outcome).await;
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (**self).on_provider(px).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        (**self).on_provider_end(px, outcome).await;
    }
}

/// Edits a request's inputs or a response's outputs as they stream
/// ([`ClientContext::filter`], [`ProviderContext::filter`]): change, drop,
/// keep back or insert values of `T`, the format's input or output type.
/// Every value reaches it, content or not
/// ([`Format::is_content`](crate::Format::is_content)), such as a keep-alive
/// or a message of usage alone: pass on at once what it has no reason to
/// hold, or it waits too. What the other side's format cannot write fails
/// the call or response. A panic is an internal fault. Filters are synchronous:
/// compute what must be awaited in a hook, and apply it here.
pub trait Filter<T>: Send + 'static {
    /// Take `value` and push what replaces it, if anything, to `out`. `Err`
    /// fails the inputs or the response.
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The inputs or the response end `end`. Runs once at every end, a
    /// failure, drop or panic included: push what it kept back to `out`,
    /// sent before the end. Once it failed, what it passes on is dropped.
    /// `Err` fails the inputs or response, keeping a provider's answer.
    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}

/// A closure that changes each value in place, or fails.
impl<T, F: FnMut(&mut T) -> Result<(), Fault> + Send + 'static> Filter<T> for F {
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(value);
        out.last_mut().map_or(Ok(()), self)
    }
}

/// One edge's filters of one direction, their values' type erased.
trait AnyList: Send {
    fn as_any(&mut self) -> &mut dyn Any;
    fn is_empty(&self) -> bool;
    fn run(&mut self, values: &mut Values) -> Result<(), Fault>;
    fn end(&mut self, end: &Result<(), Fault>, values: &mut Option<Values>) -> Option<Fault>;
}

/// Filters of `T`, in reverse order of application, each with whether it
/// failed.
struct List<T>(Vec<(Box<dyn Filter<T>>, bool)>);

impl<T: Send + 'static> AnyList for List<T> {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Each value in turn through every filter, so what one fails on leaves
    /// those before it written.
    fn run(&mut self, values: &mut Values) -> Result<(), Fault> {
        let values = values
            .downcast_mut::<Vec<T>>()
            .ok_or_else(Fault::wrong_kind::<T>)?;
        for value in std::mem::take(values) {
            let mut each = vec![value];
            for (filter, failed) in self.0.iter_mut().rev() {
                let mut out = Vec::with_capacity(each.len());
                for value in each {
                    if let Err(f) = guarded(|| filter.edit(value, &mut out)) {
                        *failed = true;
                        return Err(f);
                    }
                }
                each = out;
            }
            values.append(&mut each);
        }
        Ok(())
    }

    /// Each filter's end, once: what earlier ones kept back goes through
    /// it, unless it failed, then what it keeps back joins them.
    fn end(&mut self, end: &Result<(), Fault>, values: &mut Option<Values>) -> Option<Fault> {
        let (mut tail, mut fault) = (Vec::new(), None);
        for (filter, failed) in self.0.iter_mut().rev() {
            let was = *failed;
            let mut passed = Vec::new();
            let mut edited = Ok(());
            for value in std::mem::take(&mut tail).into_iter().filter(|_| !was) {
                edited = guarded(|| filter.edit(value, &mut passed));
                if edited.is_err() {
                    break;
                }
            }
            // Caught here, so what it pushed before a panic is kept.
            let mut kept = Vec::new();
            let ended = guarded(|| filter.end(end, &mut kept));
            *failed |= edited.is_err() || ended.is_err();
            // What a failing call produced, and all after, is not written.
            tail = match was || edited.is_err() {
                true => Vec::new(),
                false => passed,
            };
            if !*failed {
                tail.append(&mut kept);
            }
            if let Err(f) = edited.and(ended) {
                fault.get_or_insert(f);
            }
        }
        if !tail.is_empty() {
            match values.as_mut().and_then(|v| v.downcast_mut::<Vec<T>>()) {
                Some(values) => values.append(&mut tail),
                None => *values = Some(Box::new(tail)),
            }
        }
        fault
    }
}

/// One edge's filters of one direction, and how they ended, once they did.
#[derive(Default)]
pub(crate) struct Filters(Option<Box<dyn AnyList>>, Option<Result<(), Fault>>);

impl Filters {
    /// Add `filter`: of outputs, applied the last added first; of inputs,
    /// the first added first, so a plugin sees inputs after earlier plugins
    /// and outputs before them. Once they ended, it sees their end at once,
    /// and returns it.
    pub(crate) fn add<T: Send + 'static>(
        &mut self,
        filter: impl Filter<T>,
        input: bool,
    ) -> Result<(), Fault> {
        if let Some(end) = self.1.clone() {
            return Self::of(filter).end(end, &mut None);
        }
        let list = self
            .0
            .get_or_insert_with(|| Box::new(List::<T>(Vec::new())));
        let List(list) = list
            .as_any()
            .downcast_mut::<List<T>>()
            .ok_or_else(Fault::wrong_kind::<T>)?;
        let filter = (Box::new(filter) as Box<dyn Filter<T>>, false);
        match input {
            true => list.insert(0, filter),
            false => list.push(filter),
        }
        Ok(())
    }

    /// A list of `filter` alone.
    pub(crate) fn of<T: Send + 'static>(filter: impl Filter<T>) -> Self {
        let mut filters = Self::default();
        let _ = filters.add(filter, false);
        filters
    }

    pub(crate) fn ended(&self) -> bool {
        self.1.is_some()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(|list| list.is_empty())
    }

    /// Run `values` through the filters until the first failure; `values`
    /// keeps what passed before it.
    pub(crate) fn run(&mut self, values: &mut Values) -> Result<(), Fault> {
        match &mut self.0 {
            Some(list) => guarded(|| list.run(values)),
            None => Ok(()),
        }
    }

    /// Run every filter's end for `end`, once, even after one fails or
    /// panics: what they kept back is added to `values`, and the end comes
    /// back, failed with the first failure; again, just that end.
    pub(crate) fn end(
        &mut self,
        end: Result<(), Fault>,
        values: &mut Option<Values>,
    ) -> Result<(), Fault> {
        if let Some(ended) = &self.1 {
            return ended.clone();
        }
        let end = match self.0.take().and_then(|mut list| list.end(&end, values)) {
            None => end,
            Some(f) => failed(f, &end),
        };
        self.1 = Some(end.clone());
        end
    }
}

/// `fault` failing a response that ended `end`, keeping the provider's
/// answer `end` carries.
pub(crate) fn failed(fault: Fault, end: &Result<(), Fault>) -> Result<(), Fault> {
    match end {
        Err(end) => Err(fault.with_answer_of(end)),
        Ok(()) => Err(fault),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Notes its name at its end, failing it if `fails`; fails the output
    /// equal to its name.
    struct Noted(Arc<Mutex<Vec<&'static str>>>, &'static str, bool);

    impl Filter<String> for Noted {
        fn edit(&mut self, output: String, out: &mut Vec<String>) -> Result<(), Fault> {
            if output == self.1 {
                return Err(Fault::internal("refused"));
            }
            out.push(output);
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<String>) -> Result<(), Fault> {
            self.0.lock().map(|mut n| n.push(self.1)).ok();
            match self.2 {
                true => Err(Fault::internal(self.1)),
                false => Ok(()),
            }
        }
    }

    /// Filters of `each`, applied the last first.
    fn filters(noted: &Arc<Mutex<Vec<&'static str>>>, each: &[(&'static str, bool)]) -> Filters {
        let mut filters = Filters::default();
        for &(name, fails) in each {
            let _ = filters.add(Noted(noted.clone(), name, fails), false);
        }
        filters
    }

    fn one(s: &str) -> Values {
        Box::new(vec![s.to_owned()])
    }

    fn strings(values: Option<Values>) -> Vec<String> {
        let values = values.and_then(|v| v.downcast::<Vec<String>>().ok());
        values.map(|v| *v).unwrap_or_default()
    }

    #[test]
    fn a_filter_whose_edit_failed_still_gets_its_end() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        let mut filters = filters(&noted, &[("bad", false), ("worse", false)]);
        assert!(filters.run(&mut one("bad")).is_err());
        let _ = filters.end(Ok(()), &mut None);
        assert_eq!(
            noted.lock().map(|n| n.clone()).ok(),
            Some(vec!["worse", "bad"])
        );
    }

    #[test]
    fn every_filter_gets_its_end_once_and_the_first_failure_wins() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        // Applied the last added first.
        let each = [("third", true), ("second", true), ("first", false)];
        let mut filters = filters(&noted, &each);
        let end = filters.end(Ok(()), &mut None);
        assert_eq!(
            end.err().map(|f| f.message().to_owned()).as_deref(),
            Some("second")
        );
        let _ = filters.end(Ok(()), &mut None);
        let noted = noted.lock().map(|n| n.clone()).ok();
        assert_eq!(noted, Some(vec!["first", "second", "third"]));
    }

    /// Keeps back every value, failing on it if `.1`, and releases it at
    /// its end.
    struct Keep<T>(Vec<T>, bool);

    impl<T: Send + 'static> Filter<T> for Keep<T> {
        fn edit(&mut self, v: T, _out: &mut Vec<T>) -> Result<(), Fault> {
            self.0.push(v);
            if self.1 {
                Err(Fault::internal("fails"))
            } else {
                Ok(())
            }
        }
        fn end(&mut self, _e: &Result<(), Fault>, out: &mut Vec<T>) -> Result<(), Fault> {
            out.append(&mut self.0);
            Ok(())
        }
    }

    #[test]
    fn a_filter_failing_on_a_released_value_still_gets_its_end() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        // Applied the last added first: Keep, then "bad".
        let mut filters = filters(&noted, &[("bad", false)]);
        let _ = filters.add(Keep::<String>(Vec::new(), false), false);
        assert!(filters.run(&mut one("bad")).is_ok());
        assert!(filters.end(Ok(()), &mut None).is_err());
        assert_eq!(noted.lock().map(|n| n.clone()).ok(), Some(vec!["bad"]));
    }

    /// Panics at its end.
    struct Panics;

    impl Filter<String> for Panics {
        fn edit(&mut self, _v: String, _out: &mut Vec<String>) -> Result<(), Fault> {
            Ok(())
        }
        fn end(&mut self, _e: &Result<(), Fault>, _out: &mut Vec<String>) -> Result<(), Fault> {
            panic!("a buggy filter")
        }
    }

    #[test]
    fn filter_ends_run_despite_a_panic_in_another_which_fails_the_end() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        let mut filters = filters(&noted, &[("after", false)]);
        let _ = filters.add(Panics, false);
        let end = filters.end(Ok(()), &mut None);
        assert_eq!(
            end.err().map(|f| f.code().to_owned()).as_deref(),
            Some("internal")
        );
        assert_eq!(noted.lock().map(|n| n.clone()).ok(), Some(vec!["after"]));
    }

    #[test]
    fn a_failed_filter_releases_nothing() {
        let mut outputs = Filters::default();
        let _ = outputs.add(Keep::<String>(Vec::new(), true), false);
        assert!(outputs.run(&mut one("kept")).is_err());
        let mut tail = None;
        assert!(outputs.end(Ok(()), &mut tail).is_ok());
        assert!(tail.is_none());
    }

    // What a filter fails on leaves the values before it to be written.
    #[test]
    fn the_values_before_a_failure_pass() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        let mut filters = filters(&noted, &[("bad", false)]);
        let mut values: Values = Box::new(vec!["a".to_owned(), "bad".to_owned(), "c".to_owned()]);
        assert!(filters.run(&mut values).is_err());
        assert_eq!(strings(Some(values)), ["a"]);
    }
}
