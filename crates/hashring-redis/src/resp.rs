//! The Redis serialization protocol, as much of it as this gateway speaks.
//!
//! Requests arrive as RESP arrays of bulk strings, which is the only framing
//! real clients use, plus the inline form `telnet` and `redis-cli` fall back to
//! when a command is typed by hand. Replies are written for whichever protocol
//! version the connection negotiated: the same logical answer, encoded as a
//! null bulk under RESP2 and as the RESP3 null under RESP3, for instance, or as
//! a map instead of a flat array.

/// Which dialect a connection speaks. Everything else about a reply is the
/// same; only the encoding of null, maps and sets differs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Resp2,
    Resp3,
}

impl Protocol {
    pub fn version(self) -> u8 {
        match self {
            Self::Resp2 => 2,
            Self::Resp3 => 3,
        }
    }
}

/// Bounds on what a connection may send, so a client cannot make the gateway
/// allocate without limit before a single command runs.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Largest accepted bulk string, and the largest value `SET`, `APPEND` and
    /// `SETRANGE` will build. A node's own `--max-value-bytes` is a separate,
    /// smaller question; `--max-bulk-bytes` is this one.
    pub max_bulk_bytes: usize,
    pub max_arguments: usize,
    /// Longest line without a newline: an inline command, or a `*` or `$`
    /// header. Redis's own bound, so a stream of bytes that never ends a line
    /// is refused instead of buffered.
    pub max_line_bytes: usize,
    /// Bytes a connection may have received and not yet run, across however
    /// many requests they hold. Redis's `client-query-buffer-limit`.
    pub max_query_buffer_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bulk_bytes: 16 * 1024 * 1024,
            max_arguments: 1 << 17,
            max_line_bytes: 64 * 1024,
            max_query_buffer_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl Limits {
    /// What a connection that has not authenticated may send. The handshake
    /// needs a handful of short arguments; anything larger is someone without
    /// the password asking the gateway to hold memory for them. Redis uses the
    /// same two numbers.
    pub fn unauthenticated(self) -> Self {
        Self {
            max_bulk_bytes: self.max_bulk_bytes.min(16 * 1024),
            max_arguments: self.max_arguments.min(10),
            ..self
        }
    }
}

/// What a request frame parsed off the buffer turned out to be.
#[derive(Debug)]
pub enum Frame {
    /// A complete command: its arguments, first one the command name.
    Command {
        args: Vec<Vec<u8>>,
        /// Bytes consumed from the front of the buffer.
        length: usize,
    },
    /// The buffer holds a prefix of a request; read more and try again.
    Incomplete,
    /// The bytes cannot be a request in any protocol version. The connection
    /// cannot resynchronize after this, so it is answered and closed.
    Invalid(String),
}

/// One parsed request.
pub fn parse(buffer: &[u8], limits: &Limits) -> Frame {
    match buffer.first() {
        // A newline before anything else is a blank line between pipelined
        // commands, which Redis skips.
        Some(b'\r') | Some(b'\n') => match find_line_end(buffer, 1) {
            Some(end) => Frame::Command {
                args: Vec::new(),
                length: end,
            },
            None => Frame::Incomplete,
        },
        Some(b'*') => parse_multibulk(buffer, limits),
        Some(_) => parse_inline(buffer, limits),
        None => Frame::Incomplete,
    }
}

/// `*<count>\r\n` followed by that many `$<length>\r\n<bytes>\r\n` elements.
fn parse_multibulk(buffer: &[u8], limits: &Limits) -> Frame {
    let count = match read_header(buffer, 0, b'*', limits) {
        Header::Incomplete => return Frame::Incomplete,
        Header::Invalid(error) => return Frame::Invalid(error),
        Header::Found { value, next } => (value, next),
    };
    if count.0 > limits.max_arguments {
        return Frame::Invalid(format!(
            "invalid multibulk length, the command has too many arguments (at most {})",
            limits.max_arguments
        ));
    }
    let mut args = Vec::with_capacity(count.0.min(1024));
    let mut cursor = count.1;
    for _ in 0..count.0 {
        let (length, start) = match read_header(buffer, cursor, b'$', limits) {
            Header::Incomplete => return Frame::Incomplete,
            Header::Invalid(error) => return Frame::Invalid(error),
            Header::Found { value, next } => (value, next),
        };
        if length > limits.max_bulk_bytes {
            return Frame::Invalid(format!(
                "invalid bulk length, a single argument is at most {} bytes",
                limits.max_bulk_bytes
            ));
        }
        // The bytes, then the CRLF that ends the element.
        let Some(end) = start
            .checked_add(length)
            .and_then(|at| at.checked_add(2))
            .filter(|end| *end <= buffer.len())
        else {
            return Frame::Incomplete;
        };
        if &buffer[end - 2..end] != b"\r\n" {
            return Frame::Invalid("invalid bulk length, missing CRLF after argument".into());
        }
        args.push(buffer[start..start + length].to_vec());
        cursor = end;
    }
    Frame::Command {
        args,
        length: cursor,
    }
}

/// A `<prefix><digits><CRLF>` header at one offset: the number it holds and the
/// offset of the first byte after the line.
enum Header {
    Found { value: usize, next: usize },
    Incomplete,
    Invalid(String),
}

fn read_header(buffer: &[u8], index: usize, expected: u8, limits: &Limits) -> Header {
    let Some(byte) = buffer.get(index) else {
        return Header::Incomplete;
    };
    if *byte != expected {
        return Header::Invalid(format!(
            "expected '{}', but got something else",
            expected as char
        ));
    }
    let digits_start = index + 1;
    let Some(end) = find_line_end(buffer, digits_start) else {
        if buffer.len() - index > limits.max_line_bytes {
            return Header::Invalid(match expected {
                b'*' => "too big mbulk count string".into(),
                _ => "too big bulk count string".into(),
            });
        }
        return Header::Incomplete;
    };
    // `end` is past the line ending, so drop the LF and a CR if the client sent
    // one; a bare LF ends a line here too.
    let mut digits = &buffer[digits_start..end - 1];
    if digits.last() == Some(&b'\r') {
        digits = &digits[..digits.len() - 1];
    }
    let Ok(text) = std::str::from_utf8(digits) else {
        return Header::Invalid("invalid multibulk length".into());
    };
    match text.parse::<usize>() {
        Ok(value) => Header::Found { value, next: end },
        Err(_) => Header::Invalid(if expected == b'*' {
            "invalid multibulk length".into()
        } else {
            "invalid bulk length".into()
        }),
    }
}

/// The `PING\r\n` form, with arguments separated by spaces.
fn parse_inline(buffer: &[u8], limits: &Limits) -> Frame {
    let Some(end) = find_line_end(buffer, 0) else {
        if buffer.len() > limits.max_line_bytes {
            return Frame::Invalid("too big inline request".into());
        }
        return Frame::Incomplete;
    };
    let line = &buffer[..end];
    let mut args = Vec::new();
    for argument in line.split(|byte| byte.is_ascii_whitespace()) {
        if argument.is_empty() {
            continue;
        }
        if argument.len() > limits.max_bulk_bytes {
            return Frame::Invalid(format!(
                "invalid bulk length, a single argument is at most {} bytes",
                limits.max_bulk_bytes
            ));
        }
        args.push(argument.to_vec());
    }
    if args.len() > limits.max_arguments {
        return Frame::Invalid(format!(
            "invalid multibulk length, the command has too many arguments (at most {})",
            limits.max_arguments
        ));
    }
    Frame::Command { args, length: end }
}

/// Reads a `<prefix><number>\r\n` header that starts at `start`, returning the
/// number and leaving the cursor on the first byte after the line.
/// The offset just past the line ending at or after `start`. A bare LF ends a
/// line as well as CRLF, so a hand-typed line through `nc` is not a protocol
/// error.
fn find_line_end(buffer: &[u8], start: usize) -> Option<usize> {
    buffer[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| start + offset + 1)
}

/// Everything a command can answer with. A reply is built without knowing how
/// it will be encoded, so a handler does not have to know the connection's
/// protocol version.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    /// A one-word status such as `OK`.
    Simple(String),
    Error(String),
    Integer(i64),
    /// A byte string, or `None` for a missing one.
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
    /// Bulk-keyed pairs. RESP2 has no map, so this encodes as a flat array.
    Map(Vec<(Vec<u8>, Reply)>),
    Set(Vec<Vec<u8>>),
    Double(f64),
    Boolean(bool),
    /// A missing value, in whichever spelling the protocol has for one.
    Nil,
}

impl Reply {
    pub fn bulk(value: impl Into<Vec<u8>>) -> Self {
        Self::Bulk(Some(value.into()))
    }

    pub fn status() -> Self {
        Self::Simple("OK".into())
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

pub fn encode(reply: &Reply, protocol: Protocol, out: &mut Vec<u8>) {
    match reply {
        Reply::Simple(text) => {
            out.push(b'+');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Error(message) => {
            out.push(b'-');
            out.extend_from_slice(message.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Integer(value) => {
            out.push(b':');
            out.extend_from_slice(value.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(Some(value)) => {
            out.push(b'$');
            out.extend_from_slice(value.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(None) | Reply::Nil => match protocol {
            // RESP2 spells an absent value as a bulk of length -1.
            Protocol::Resp2 => out.extend_from_slice(b"$-1\r\n"),
            Protocol::Resp3 => out.extend_from_slice(b"_\r\n"),
        },
        Reply::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode(item, protocol, out);
            }
        }
        Reply::Set(items) => match protocol {
            Protocol::Resp2 => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    encode(&Reply::bulk(item.clone()), protocol, out);
                }
            }
            Protocol::Resp3 => {
                out.push(b'~');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    encode(&Reply::bulk(item.clone()), protocol, out);
                }
            }
        },
        Reply::Map(entries) => match protocol {
            Protocol::Resp2 => {
                out.push(b'*');
                out.extend_from_slice((entries.len() * 2).to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for (key, value) in entries {
                    encode(&Reply::bulk(key.clone()), protocol, out);
                    encode(value, protocol, out);
                }
            }
            Protocol::Resp3 => {
                out.push(b'%');
                out.extend_from_slice(entries.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for (key, value) in entries {
                    encode(&Reply::bulk(key.clone()), protocol, out);
                    encode(value, protocol, out);
                }
            }
        },
        Reply::Double(value) => {
            out.push(b',');
            out.extend_from_slice(format_double(*value).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Boolean(value) => {
            out.push(b'#');
            out.extend_from_slice(if *value { b"t" } else { b"f" });
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// RESP3 doubles reject the shapes Redis never emits, and `inf` and `nan` are
/// rejected outright because they are not valid RESP3 doubles.
fn format_double(value: f64) -> String {
    if !value.is_finite() {
        return "0".into();
    }
    let text = format!("{value}");
    if text.contains(['.', 'e', 'E', 'n', 'i']) {
        text
    } else {
        format!("{text}.0")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits::default()
    }

    fn round_trip(request: &[u8], limits: &Limits) -> Frame {
        match parse(request, limits) {
            Frame::Command { args, length } => {
                assert_eq!(length, request.len(), "frame did not consume the request");
                Frame::Command { args, length: 0 }
            }
            other => other,
        }
    }

    #[test]
    fn parses_a_multibulk_command() {
        let request = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let Frame::Command { args, length } = round_trip(request, &limits()) else {
            panic!("expected a command");
        };
        assert_eq!(length, 0);
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
        );
    }

    #[test]
    fn parses_a_binary_argument_with_embedded_crlf() {
        let request: &[u8] = b"*2\r\n$3\r\nGET\r\n$6\r\na\r\nb\r\n\r\n";
        let Frame::Command { args, .. } = parse(request, &limits()) else {
            panic!("expected a command");
        };
        assert_eq!(args[1], b"a\r\nb\r\n".to_vec());
    }

    #[test]
    fn reports_a_partial_request_instead_of_failing() {
        let request = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nval";
        assert!(matches!(parse(request, &limits()), Frame::Incomplete));
    }

    #[test]
    fn parses_an_inline_command() {
        let Frame::Command { args, length } = parse(b"PING hello\r\n", &limits()) else {
            panic!("expected a command");
        };
        assert_eq!(length, 12);
        assert_eq!(args, vec![b"PING".to_vec(), b"hello".to_vec()]);
    }

    #[test]
    fn skips_the_blank_line_between_pipelined_commands() {
        let Frame::Command { args, length } = parse(b"\r\n*1\r\n$4\r\nPING\r\n", &limits()) else {
            panic!("expected a command");
        };
        assert!(args.is_empty());
        assert_eq!(length, 2);
    }

    #[test]
    fn rejects_a_bulk_longer_than_the_limit() {
        let limits = Limits {
            max_bulk_bytes: 4,
            ..Limits::default()
        };
        let request = b"*2\r\n$3\r\nGET\r\n$9\r\nkeykeykey\r\n";
        assert!(matches!(parse(request, &limits), Frame::Invalid(_)));
    }

    #[test]
    fn a_line_that_never_ends_is_refused_instead_of_buffered() {
        let limits = Limits {
            max_line_bytes: 16,
            ..limits()
        };
        assert!(matches!(parse(&[b'x'; 17], &limits), Frame::Invalid(_)));
        assert!(matches!(parse(&[b'x'; 16], &limits), Frame::Incomplete));
        let mut header = b"*".to_vec();
        header.extend([b'1'; 17]);
        assert!(matches!(parse(&header, &limits), Frame::Invalid(_)));
        let mut bulk = b"*1\r\n$".to_vec();
        bulk.extend([b'1'; 17]);
        assert!(matches!(parse(&bulk, &limits), Frame::Invalid(_)));
    }

    #[test]
    fn an_unauthenticated_connection_may_send_only_a_handshake() {
        let limits = Limits::default().unauthenticated();
        assert!(matches!(parse(b"*11\r\n", &limits), Frame::Invalid(_)));
        assert!(matches!(
            parse(b"*1\r\n$16385\r\n", &limits),
            Frame::Invalid(_)
        ));
        assert!(matches!(
            parse(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n", &limits),
            Frame::Command { .. }
        ));
    }

    #[test]
    fn rejects_a_bulk_that_is_not_a_bulk() {
        assert!(matches!(
            parse(b"*2\r\n+PING\r\n$3\r\nfoo\r\n", &limits()),
            Frame::Invalid(_)
        ));
    }

    #[test]
    fn a_line_that_is_not_a_bulk_is_an_unknown_command_rather_than_a_protocol_error() {
        // A hand-typed line that is not a command still gets an answer, so the
        // connection survives it and the client learns which command is unknown.
        let Frame::Command { args, length } = parse(b"+OK\r\n", &limits()) else {
            panic!("expected a command");
        };
        assert_eq!(args, vec![b"+OK".to_vec()]);
        assert_eq!(length, 5);
    }

    fn encoded(reply: &Reply, protocol: Protocol) -> String {
        let mut out = Vec::new();
        encode(reply, protocol, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_missing_value_uses_the_protocol_null() {
        assert_eq!(encoded(&Reply::Nil, Protocol::Resp2), "$-1\r\n");
        assert_eq!(encoded(&Reply::Nil, Protocol::Resp3), "_\r\n");
    }

    #[test]
    fn a_map_is_flat_in_resp2_and_a_map_in_resp3() {
        let reply = Reply::Map(vec![
            (b"maxmemory".to_vec(), Reply::bulk("0")),
            (b"save".to_vec(), Reply::bulk("")),
        ]);
        assert_eq!(
            encoded(&reply, Protocol::Resp2),
            "*4\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n$4\r\nsave\r\n$0\r\n\r\n"
        );
        assert_eq!(
            encoded(&reply, Protocol::Resp3),
            "%2\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n$4\r\nsave\r\n$0\r\n\r\n"
        );
    }

    #[test]
    fn a_set_is_an_array_in_resp2() {
        let reply = Reply::Set(vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(
            encoded(&reply, Protocol::Resp2),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
        assert_eq!(
            encoded(&reply, Protocol::Resp3),
            "~2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn a_double_always_carries_a_point_or_an_exponent() {
        assert_eq!(encoded(&Reply::Double(2.0), Protocol::Resp3), ",2.0\r\n");
        assert_eq!(encoded(&Reply::Double(2.5), Protocol::Resp3), ",2.5\r\n");
        assert_eq!(encoded(&Reply::Double(f64::NAN), Protocol::Resp3), ",0\r\n");
    }
}
