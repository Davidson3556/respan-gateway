/// A wire codec between transport frames and Parts. Compiled in, never loaded.
pub trait Format: Send + Sync + 'static {
    fn name(&self) -> &'static str;
}
