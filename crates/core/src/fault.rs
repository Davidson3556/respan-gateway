use std::fmt;

/// Why a hook, filter or source failed.
#[derive(Debug)]
pub struct Fault {
    message: String,
}

impl Fault {
    pub fn config(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Fault {}
