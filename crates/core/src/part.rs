/// What flows in either direction of a Turn.
#[derive(Debug)]
pub enum Part {
    /// Status and headers.
    Head,
    /// A whole message, lazily parsed and lossless.
    Doc,
    Delta(Delta),
    /// Additive usage increments in an open unit map.
    Usage,
    /// Exactly one per stream.
    End,
}

#[derive(Debug)]
pub struct Delta {
    pub block: u32,
    pub kind: DeltaKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Text,
    Args,
    Reasoning,
    Bytes,
}
