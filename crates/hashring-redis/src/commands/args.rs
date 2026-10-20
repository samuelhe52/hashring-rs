//! Reading a command's arguments.
//!
//! Every handler reads its arguments through [`Args`], which never indexes past
//! the end: an option whose operand is missing is a syntax error rather than a
//! panic, and an argument left over after a command has read everything it
//! takes is one too. The dispatcher has already checked the command's arity
//! against [`crate::table`], so a handler only reads what that arity
//! guarantees, or asks for optional arguments and handles their absence.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{RedisError, RedisResult};

/// The arguments after the command name, read front to back.
pub struct Args<'a> {
    name: &'static str,
    rest: &'a [Vec<u8>],
}

impl<'a> Args<'a> {
    /// `rest` excludes the command name; `name` is what errors call the
    /// command.
    pub fn new(name: &'static str, rest: &'a [Vec<u8>]) -> Self {
        Self { name, rest }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Everything not read yet.
    pub fn remaining(&self) -> &'a [Vec<u8>] {
        self.rest
    }

    /// The next argument, or a syntax error when there is none.
    // Required arguments report protocol errors rather than Iterator's Option.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> RedisResult<&'a [u8]> {
        self.next_optional().ok_or_else(RedisError::syntax)
    }

    pub fn next_optional(&mut self) -> Option<&'a [u8]> {
        let (first, rest) = self.rest.split_first()?;
        self.rest = rest;
        Some(first)
    }

    pub fn next_owned(&mut self) -> RedisResult<Vec<u8>> {
        self.next().map(<[u8]>::to_vec)
    }

    /// The next argument as an option keyword, lower case.
    pub fn next_keyword(&mut self) -> Option<String> {
        self.next_optional().map(keyword)
    }

    pub fn next_integer(&mut self) -> RedisResult<i64> {
        let value = self.next()?;
        integer(value, self.name)
    }

    pub fn next_float(&mut self) -> RedisResult<f64> {
        float(self.next()?)
    }

    /// Ends the command: anything left is a syntax error.
    pub fn finish(self) -> RedisResult<()> {
        match self.rest.is_empty() {
            true => Ok(()),
            false => Err(RedisError::syntax()),
        }
    }
}

/// A command word in lower case, for comparing against `NX`, `EX` and friends.
pub fn keyword(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_lowercase()
}

pub fn parse_integer(value: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(value).ok()?;
    text.trim().parse::<i64>().ok()
}

pub fn integer(value: &[u8], command: &str) -> RedisResult<i64> {
    parse_integer(value).ok_or_else(|| {
        RedisError::new(
            "ERR",
            format!("value is not an integer or out of range ({command})"),
        )
    })
}

pub fn float(value: &[u8]) -> RedisResult<f64> {
    let text = std::str::from_utf8(value).unwrap_or_default();
    match text.trim().parse::<f64>() {
        Ok(parsed) if parsed.is_finite() => Ok(parsed),
        _ => Err(RedisError::not_a_float()),
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeUnit {
    Seconds,
    Milliseconds,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadlineMode {
    /// A timeout from now: `EX`, `PX`, `EXPIRE`, `PEXPIRE`.
    Relative,
    /// A Unix timestamp: `EXAT`, `PXAT`, `EXPIREAT`, `PEXPIREAT`.
    Absolute,
}

/// How a command states a deadline: the unit and whether it counts from now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TtlForm {
    pub unit: TimeUnit,
    pub mode: DeadlineMode,
}

impl TtlForm {
    pub const fn new(unit: TimeUnit, mode: DeadlineMode) -> Self {
        Self { unit, mode }
    }

    /// The expiry option a keyword names, for the commands that take one.
    pub fn from_option(keyword: &str) -> Option<Self> {
        use DeadlineMode::*;
        use TimeUnit::*;
        Some(match keyword {
            "ex" => Self::new(Seconds, Relative),
            "px" => Self::new(Milliseconds, Relative),
            "exat" => Self::new(Seconds, Absolute),
            "pxat" => Self::new(Milliseconds, Absolute),
            _ => return None,
        })
    }

    /// The absolute deadline, in Unix milliseconds, that `amount` names at
    /// `now`. Signed and wide, because `EXPIRE key -10` names a deadline in the
    /// past, which a command treats as a delete rather than as a far future.
    /// `None` when it overflows what a deadline can hold, which Redis reports
    /// as an invalid expire time.
    pub fn deadline(self, amount: i64, now: u64) -> Option<i128> {
        let millis = match self.unit {
            TimeUnit::Seconds => i128::from(amount) * 1000,
            TimeUnit::Milliseconds => i128::from(amount),
        };
        let deadline = match self.mode {
            DeadlineMode::Relative => i128::from(now) + millis,
            DeadlineMode::Absolute => millis,
        };
        (deadline <= i128::from(i64::MAX)).then_some(deadline)
    }
}

pub fn invalid_expire_time(command: &str) -> RedisError {
    RedisError::new("ERR", format!("invalid expire time in '{command}' command"))
}

/// A deadline set by `SET`, `SETEX`, `PSETEX` or `GETEX`. Unlike `EXPIRE`,
/// these refuse a zero or negative amount instead of deleting the key.
pub fn positive_deadline(form: TtlForm, amount: i64, command: &str) -> RedisResult<u64> {
    if amount <= 0 {
        return Err(invalid_expire_time(command));
    }
    form.deadline(amount, now_millis())
        .and_then(|deadline| u64::try_from(deadline).ok())
        .ok_or_else(|| invalid_expire_time(command))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(args: &[&[u8]]) -> Vec<Vec<u8>> {
        args.iter().map(|arg| arg.to_vec()).collect()
    }

    #[test]
    fn a_missing_operand_is_a_syntax_error_not_a_panic() {
        let rest = owned(&[b"EX"]);
        let mut args = Args::new("set", &rest);
        assert_eq!(args.next_keyword().as_deref(), Some("ex"));
        assert_eq!(args.next_integer().unwrap_err().message, "syntax error");
    }

    #[test]
    fn a_leftover_argument_is_a_syntax_error() {
        let rest = owned(&[b"a", b"b"]);
        let mut args = Args::new("echo", &rest);
        args.next().unwrap();
        assert!(args.finish().is_err());
    }

    #[test]
    fn each_expiry_option_has_its_own_unit() {
        let now = 1_000_000;
        let deadline = |option: &str, amount| {
            TtlForm::from_option(option)
                .unwrap()
                .deadline(amount, now)
                .unwrap()
        };
        assert_eq!(deadline("ex", 10), 1_010_000);
        assert_eq!(deadline("px", 10), 1_000_010);
        assert_eq!(deadline("exat", 1_800_000_000), 1_800_000_000_000);
        assert_eq!(deadline("pxat", 1_800_000_000_000), 1_800_000_000_000);
        assert_eq!(TtlForm::from_option("persist"), None);
    }

    #[test]
    fn a_negative_timeout_is_a_deadline_in_the_past() {
        let form = TtlForm::new(TimeUnit::Seconds, DeadlineMode::Relative);
        assert_eq!(form.deadline(-1, 5_000), Some(4_000));
        assert_eq!(form.deadline(i64::MAX, 5_000), None);
    }

    #[test]
    fn set_style_deadlines_refuse_zero_and_overflow() {
        let ex = TtlForm::new(TimeUnit::Seconds, DeadlineMode::Relative);
        assert!(positive_deadline(ex, 0, "set").is_err());
        assert!(positive_deadline(ex, -5, "set").is_err());
        assert!(positive_deadline(ex, i64::MAX, "set").is_err());
        assert!(positive_deadline(ex, 10, "set").unwrap() > now_millis());
    }
}
