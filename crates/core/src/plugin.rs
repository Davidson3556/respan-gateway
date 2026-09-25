use async_trait::async_trait;

use crate::{Attempt, Call, Fault, Req, Turn, Upstream};

/// The only extension trait. Which method you implement decides where your
/// code runs.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Content a required entry guards. The kernel refuses Turns it cannot see.
    fn covers(&self) -> Covers {
        Covers::NONE
    }

    /// Once per Turn, before the body is read.
    async fn request(&self, _t: &Turn<Req>) -> Result<(), Fault> {
        Ok(())
    }

    /// Once per provider attempt, before the dial.
    async fn attempt(&self, _a: &Turn<Attempt>) -> Result<(), Fault> {
        Ok(())
    }

    /// The route's source. The returned Call is the commit.
    async fn serve(&self, _t: &Turn<Req>, _up: &Upstream) -> Result<Call, Fault> {
        Err(Fault::config("not a source"))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Covers {
    pub up: bool,
    pub down: bool,
}

impl Covers {
    pub const NONE: Self = Self {
        up: false,
        down: false,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;
    impl Plugin for Noop {}

    #[test]
    fn plugin_is_object_safe() {
        let plugins: Vec<Box<dyn Plugin>> = vec![Box::new(Noop)];
        assert_eq!(plugins[0].covers(), Covers::NONE);
    }
}
