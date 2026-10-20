pub const DEFAULT_MAX_KEY_BYTES: usize = 64 * 1024;

/// The value ceiling a node starts with. `--max-value-bytes` moves it, up to
/// [`MAX_VALUE_BYTES`].
pub const DEFAULT_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;

/// The largest value any node may be configured to accept, which is the size
/// the transport and a migration page are sized to carry. A cluster only pays
/// for it once a node is configured above the default.
pub const MAX_VALUE_BYTES: usize = 128 * 1024 * 1024;

pub const MAX_MUTATION_ID_BYTES: usize = 128;

/// A migration page must carry at least one record, so its ceiling follows the
/// value ceiling rather than anything smaller.
pub const MAX_MIGRATION_PAGE_BYTES: usize = MAX_VALUE_BYTES;

/// What a coordinator asks a node for when it does not need a larger page.
pub const DEFAULT_MIGRATION_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Accommodates the maximum key and value plus protobuf framing and metadata.
pub const MAX_DATA_MESSAGE_BYTES: usize = MAX_VALUE_BYTES + 1024 * 1024;

/// Supports the largest legal canonical topology without constraining data values.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
