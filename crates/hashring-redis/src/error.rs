//! Errors as a Redis client sees them.

use hashring_client::{ClientError, MutationOutcome, OperationFailure};
use hashring_core::proto::ErrorCode;

/// A failure with the error code in front of the message, which is the part
/// clients branch on: `redis-py` matches the code, `redis-cli` only prints it.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code} {message}")]
pub struct RedisError {
    pub code: &'static str,
    pub message: String,
    /// A write may have been applied before this was returned, so the command
    /// is not safe to send again as if nothing happened.
    pub may_have_applied: bool,
}

pub type RedisResult<T> = Result<T, RedisError>;

impl RedisError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            may_have_applied: false,
        }
    }

    /// The code for a request this gateway cannot answer at all. Naming the
    /// command keeps the message useful without the client having to know what
    /// the gateway does support.
    pub fn unsupported(command: &str) -> Self {
        Self::new(
            "ERR",
            format!(
                "{command} is not supported by the hashring-rs Redis gateway; \
                 see docs/redis-compatibility.md"
            ),
        )
    }

    pub fn wrong_arity(command: &str) -> Self {
        Self::new(
            "ERR",
            format!("wrong number of arguments for '{command}' command"),
        )
    }

    pub fn syntax() -> Self {
        Self::new("ERR", "syntax error")
    }

    pub fn wrong_type() -> Self {
        Self::new(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
        )
    }

    pub fn not_an_integer() -> Self {
        Self::new("ERR", "value is not an integer or out of range")
    }

    pub fn not_a_float() -> Self {
        Self::new("ERR", "value is not a valid float")
    }

    pub fn wrong_length(expected: usize) -> Self {
        Self::new(
            "WRONGTYPE",
            format!("Expected {expected} arguments, but got a different number of arguments"),
        )
    }

    pub fn no_such_key() -> Self {
        Self::new("ERR", "no such key")
    }

    pub fn out_of_range() -> Self {
        Self::new("ERR", "value is out of range, must be positive")
    }

    pub fn overflow() -> Self {
        Self::new("ERR", "increment or decrement would overflow")
    }

    pub fn no_auth() -> Self {
        Self::new("NOAUTH", "Authentication required.")
    }

    pub fn wrong_pass() -> Self {
        Self::new(
            "WRONGPASS",
            "invalid username-password pair or user is disabled.",
        )
    }

    /// A command that ran out of time. A read can simply be sent again; a
    /// write may have been applied before the deadline cut it off, which
    /// `TRYAGAIN` would hide.
    pub fn deadline(writes: bool) -> Self {
        match writes {
            false => Self::new("TRYAGAIN", "the command did not finish before its deadline"),
            true => Self::outcome_unknown("the command did not finish before its deadline"),
        }
    }

    pub fn outcome_unknown(detail: &str) -> Self {
        Self {
            may_have_applied: true,
            ..Self::new(
                "ERR",
                format!("{detail}; its writes may or may not have been applied"),
            )
        }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new("ERR", message)
    }

    pub fn to_reply(&self) -> crate::resp::Reply {
        crate::resp::Reply::Error(format!("{} {}", self.code, self.message))
    }
}

impl From<ClientError> for RedisError {
    /// A retryable cluster condition becomes `TRYAGAIN`, which is the code
    /// clients already treat as "back off and send it again". A permanent
    /// condition becomes `ERR`, and so does any failure after which a write
    /// may have been applied: retrying that blindly is not safe.
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::DeadlineExceeded {
                outcome: MutationOutcome::MayHaveApplied,
            }
            | ClientError::TopologyRefresh {
                outcome: MutationOutcome::MayHaveApplied,
                ..
            }
            | ClientError::Rpc {
                outcome: MutationOutcome::MayHaveApplied,
                ..
            } => Self::outcome_unknown(&error.to_string()),
            ClientError::Operation(failure) => Self::from(failure),
            ClientError::TopologyRefresh { source, .. } => Self::new(
                "TRYAGAIN",
                format!("cluster topology is unavailable: {source}"),
            ),
            ClientError::Topology(source) => Self::new(
                "CLUSTERDOWN",
                format!("cluster topology is unavailable: {source}"),
            ),
            ClientError::DeadlineExceeded { .. } => Self::new(
                "TRYAGAIN",
                "the operation did not finish before its deadline",
            ),
            ClientError::MissingVersion => {
                Self::new("ERR", "the cluster returned a record without a version")
            }
            ClientError::Rpc { code, message, .. } => {
                if code == tonic::Code::Unavailable {
                    Self::new(
                        "TRYAGAIN",
                        format!("cluster node is unavailable: {message}"),
                    )
                } else {
                    Self::new("ERR", message)
                }
            }
        }
    }
}

impl From<OperationFailure> for RedisError {
    fn from(failure: OperationFailure) -> Self {
        let message = failure.message.trim().to_owned();
        if failure.outcome == MutationOutcome::MayHaveApplied {
            return Self::outcome_unknown(&message);
        }
        match failure.code {
            ErrorCode::NotFound => Self::new(
                "ERR",
                if message.is_empty() {
                    "key not found".into()
                } else {
                    message
                },
            ),
            ErrorCode::TooLarge => Self::new("OOM", message),
            ErrorCode::Moved
            | ErrorCode::RangeBusy
            | ErrorCode::Unavailable
            | ErrorCode::TemporarilyUnavailable
            | ErrorCode::ResourceExhausted
            | ErrorCode::ReplicaNotReady
            | ErrorCode::LeaseExpired => Self::new("TRYAGAIN", message),
            // A write whose outcome is unknown is reported as an error rather
            // than as a failure to apply: the value may well be stored.
            ErrorCode::OutcomeUnknown => Self::new("ERR", message),
            ErrorCode::InvalidArgument | ErrorCode::MutationIdConflict => Self::new("ERR", message),
            _ => Self::new(if failure.retryable { "TRYAGAIN" } else { "ERR" }, message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_that_may_have_landed_is_never_a_plain_retry() {
        let mut busy = failure(ErrorCode::RangeBusy, true);
        busy.outcome = MutationOutcome::MayHaveApplied;
        assert_eq!(RedisError::from(busy).code, "ERR");
        let timed_out = ClientError::DeadlineExceeded {
            outcome: MutationOutcome::MayHaveApplied,
        };
        assert_eq!(RedisError::from(timed_out).code, "ERR");
        let unsent = ClientError::DeadlineExceeded {
            outcome: MutationOutcome::KnownNotApplied,
        };
        assert_eq!(RedisError::from(unsent).code, "TRYAGAIN");
        assert_eq!(RedisError::deadline(false).code, "TRYAGAIN");
        assert_eq!(RedisError::deadline(true).code, "ERR");
    }

    fn failure(code: ErrorCode, retryable: bool) -> OperationFailure {
        OperationFailure {
            code,
            message: "detail".into(),
            retryable,
            outcome: MutationOutcome::KnownNotApplied,
        }
    }

    #[test]
    fn a_retryable_cluster_condition_asks_the_client_to_retry() {
        assert_eq!(
            RedisError::from(failure(ErrorCode::ResourceExhausted, true)).code,
            "TRYAGAIN"
        );
        assert_eq!(
            RedisError::from(failure(ErrorCode::Moved, true)).code,
            "TRYAGAIN"
        );
    }

    #[test]
    fn a_permanent_condition_is_an_error_not_a_retry() {
        assert_eq!(
            RedisError::from(failure(ErrorCode::InvalidArgument, false)).code,
            "ERR"
        );
        assert_eq!(
            RedisError::from(failure(ErrorCode::TooLarge, false)).code,
            "OOM"
        );
    }

    #[test]
    fn an_unknown_write_outcome_is_reported_as_an_error() {
        assert_eq!(
            RedisError::from(failure(ErrorCode::OutcomeUnknown, true)).code,
            "ERR"
        );
    }

    #[test]
    fn the_reply_carries_the_code_the_client_matches_on() {
        assert_eq!(
            RedisError::wrong_arity("get").to_reply(),
            crate::resp::Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
    }
}
