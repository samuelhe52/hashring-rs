pub const DEFAULT_MAX_KEY_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_MUTATION_ID_BYTES: usize = 128;
/// Every replica keeps a deduplication receipt for at least this long after it
/// applies a mutation. A client must stop retrying one mutation ID within this
/// window, or a retry could apply the mutation a second time.
pub const RECEIPT_RETENTION_MILLIS: u64 = 60_000;
pub const MAX_MIGRATION_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Accommodates the maximum key and value plus protobuf framing and metadata.
pub const MAX_DATA_MESSAGE_BYTES: usize = 9 * 1024 * 1024;

/// Supports the largest legal canonical topology without constraining data values.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
