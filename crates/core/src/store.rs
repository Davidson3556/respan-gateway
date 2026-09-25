/// Shared key-value store: atomic batches, leases, change waits, purge by
/// tenant and class. Holds no billing or queue types.
#[derive(Debug)]
pub struct Store {
    _private: (),
}
