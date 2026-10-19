//! Turning script text into tokens.
//!
//! The grammar is Lua's, minus what the gateway does not run: this covers the
//! statements, expressions and literals a Redis client library sends in a
//! script, and nothing else. A script using anything else is refused by the
//! parser with the name of the construct, rather than misread.

/// One token, or the end of the script.
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    /// An identifier or keyword, lowercased only when it is one: `KEYS` is an
    /// ordinary name and `Keys` is too, while `then` is the keyword.
    Name(Vec<u8>),
    Number(f64),
    Str(Vec<u8>),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Hash,
    Equal,
    NotEqual,
    LessEqual,
    GreaterEqual,
    Less,
    Greater,
    Assign,
    Concat,
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Semi,
    Colon,
    Dot,
    And,
    Or,
    Not,
    If,
    Then,
    Else,
    ElseIf,
    End,
    While,
    Do,
    For,
    In,
    Return,
    Break,
    Nil,
    True,
    False,
    Eof,
}

impl Token {
    /// The keyword's own text, for a message that names what is missing.
    pub fn describe(&self) -> String {
        match self {
            Token::Name(name) => String::from_utf8_lossy(name).into_owned(),
            Token::Number(number) => format!("number {number}"),
            Token::Str(_) => "string".into(),
            Token::Eof => "end of script".into(),
            other => format!("'{}'", other.symbol()),
        }
    }

    fn symbol(&self) -> &'static str {
        match self {
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Star => "*",
            Token::Slash => "/",
            Token::Percent => "%",
            Token::Caret => "^",
            Token::Hash => "#",
            Token::Equal => "==",
            Token::NotEqual => "~=",
            Token::LessEqual => "<=",
            Token::GreaterEqual => ">=",
            Token::Less => "<",
            Token::Greater => ">",
            Token::Assign => "=",
            Token::Concat => "..",
            Token::LParen => "(",
            Token::RParen => ")",
            Token::LBrace => "{",
            Token::RBrace => "}",
            Token::LBracket => "[",
            Token::RBracket => "]",
            Token::Comma => ",",
            Token::Semi => ";",
            Token::Colon => ":",
            Token::Dot => ".",
            Token::And => "and",
            Token::Or => "or",
            Token::Not => "not",
            Token::If => "if",
            Token::Then => "then",
            Token::Else => "else",
            Token::ElseIf => "elseif",
            Token::End => "end",
            Token::While => "while",
            Token::Do => "do",
            Token::For => "for",
            Token::In => "in",
            Token::Return => "return",
            Token::Break => "break",
            Token::Nil => "nil",
            Token::True => "true",
            Token::False => "false",
            Token::Name(_) | Token::Number(_) | Token::Str(_) | Token::Eof => "",
        }
    }
}

/// Words Lua reserves, matched without regard to case as Lua matches them.
fn keyword(word: &[u8]) -> Option<Token> {
    let lower = word.to_ascii_lowercase();
    Some(match lower.as_slice() {
        b"and" => Token::And,
        b"or" => Token::Or,
        b"not" => Token::Not,
        b"if" => Token::If,
        b"then" => Token::Then,
        b"else" => Token::Else,
        b"elseif" => Token::ElseIf,
        b"end" => Token::End,
        b"while" => Token::While,
        b"do" => Token::Do,
        b"for" => Token::For,
        b"in" => Token::In,
        b"return" => Token::Return,
        b"break" => Token::Break,
        b"nil" => Token::Nil,
        b"true" => Token::True,
        b"false" => Token::False,
        _ => return None,
    })
}

pub fn lex(source: &[u8]) -> Result<Vec<Token>, String> {
    Lexer {
        source,
        at: 0,
        line: 1,
    }
    .run()
}

struct Lexer<'a> {
    source: &'a [u8],
    at: usize,
    line: usize,
}

impl Lexer<'_> {
    fn run(mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        loop {
            self.skip_ignored()?;
            let Some(byte) = self.peek() else {
                tokens.push(Token::Eof);
                return Ok(tokens);
            };
            tokens.push(self.read_token(byte)?);
        }
    }

    fn peek(&self) -> Option<u8> {
        self.source.get(self.at).copied()
    }

    fn peek_at(&self, ahead: usize) -> Option<u8> {
        self.source.get(self.at + ahead).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn error(&self, message: impl Into<String>) -> String {
        format!("{} near line {}", message.into(), self.line)
    }

    /// Whitespace, a line continuation and a comment. A backslash at the end
    /// of a line continues it, which is how a client library writes a script
    /// as one long line in its source.
    fn skip_ignored(&mut self) -> Result<(), String> {
        loop {
            match self.peek() {
                Some(b'\n') => {
                    self.at += 1;
                    self.line += 1;
                }
                Some(byte) if byte.is_ascii_whitespace() => self.at += 1,
                Some(b'\\') => {
                    // Only a backslash that is the last thing on its line is a
                    // continuation; anywhere else it is the escape of a
                    // character inside a string, which is not read here.
                    let mut ahead = 1;
                    if self.peek_at(ahead) == Some(b'\r') {
                        ahead += 1;
                    }
                    if self.peek_at(ahead) != Some(b'\n') {
                        return;
                    }
                    self.at += ahead + 1;
                    self.line += 1;
                }
                Some(b'-') if self.peek_at(1) == Some(b'-') => {
                    self.at += 2;
                    if let Some(level) = self.long_bracket_level() {
                        self.read_long_bracket(level, "comment")?;
                    } else {
                        while let Some(byte) = self.peek() {
                            self.at += 1;
                            if byte == b'\n' {
                                self.line += 1;
                                break;
                            }
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    /// The `=` count of a long bracket opening at the cursor, which is how Lua
    /// writes `[[text]]` and `--[==[text]==]`.
    fn long_bracket_level(&self) -> Option<usize> {
        let mut ahead = 0;
        while self.peek_at(ahead) == Some(b'=') {
            ahead += 1;
        }
        (self.peek_at(ahead) == Some(b'[')).then_some(ahead)
    }

    fn read_long_bracket(&mut self, level: usize, what: &str) -> Result<Vec<u8>, String> {
        self.at += level + 2;
        // A newline straight after the opening bracket is not part of the text.
        if self.peek() == Some(b'\r') {
            self.at += 1;
        }
        if self.peek() == Some(b'\n') {
            self.at += 1;
            self.line += 1;
        }
        let closing: Vec<u8> = [b"]".to_vec(), vec![b'='; level], vec![b"]"].concat();
        let Some(offset) = self.source[self.at..]
            .windows(closing.len())
            .position(|window| window == closing)
        else {
            return Err(self.error(format!("unfinished long {what}")));
        };
        let text = self.source[self.at..self.at + offset].to_vec();
        self.at += offset + closing.len();
        self.line += text.iter().filter(|byte| **byte == b'\n').count();
        Ok(text)
    }

    fn read_token(&mut self, byte: u8) -> Result<Token, String> {
        Ok(match byte {
            b'+' => {
                self.at += 1;
                Token::Plus
            }
            b'-' => {
                self.at += 1;
                Token::Minus
            }
            b'*' => {
                self.at += 1;
                Token::Star
            }
            b'/' => {
                self.at += 1;
                Token::Slash
            }
            b'%' => {
                self.at += 1;
                Token::Percent
            }
            b'^' => {
                self.at += 1;
                Token::Caret
            }
            b'#' => {
                self.at += 1;
                Token::Hash
            }
            b'(' => {
                self.at += 1;
                Token::LParen
            }
            b')' => {
                self.at += 1;
                Token::RParen
            }
            b'{' => {
                self.at += 1;
                Token::LBrace
            }
            b'}' => {
                self.at += 1;
                Token::RBrace
            }
            b'[' => {
                self.at += 1;
                Token::LBracket
            }
            b']' => {
                self.at += 1;
                Token::RBracket
            }
            b',' => {
                self.at += 1;
                Token::Comma
            }
            b';' => {
                self.at += 1;
                Token::Semi
            }
            b':' => {
                self.at += 1;
                Token::Colon
            }
            b'=' => {
                self.at += 1;
                if self.eat(b'=') {
                    Token::Equal
                } else {
                    Token::Assign
                }
            }
            b'~' => {
                self.at += 1;
                if !self.eat(b'=') {
                    return Err(self.error("'~' is not an operator, did you mean '~='?"));
                }
                Token::NotEqual
            }
            b'<' => {
                self.at += 1;
                if self.eat(b'=') {
                    Token::LessEqual
                } else {
                    Token::Less
                }
            }
            b'>' => {
                self.at += 1;
                if self.eat(b'=') {
                    Token::GreaterEqual
                } else {
                    Token::Greater
                }
            }
            b'.' => {
                self.at += 1;
                if self.eat(b'.') {
                    Token::Concat
                } else if self.peek().is_some_and(u8::is_ascii_digit) {
                    // `.5` is a number, not a field access on nothing.
                    self.read_number()?
                } else {
                    Token::Dot
                }
            }
            b'"' | b'\'' => self.read_string(byte)?,
            b'[' => unreachable!("handled as punctuation above"),
            byte if byte.is_ascii_digit() => self.read_number()?,
            byte if byte == b'_' || byte.is_ascii_alphabetic() => self.read_name()?,
            byte => return Err(self.error(format!("unexpected character '{byte}'"))),
        })
    }

    fn read_name(&mut self) -> Result<Token, String> {
        let start = self.at;
        while self
            .peek()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            self.at += 1;
        }
        let word = self.source[start..self.at].to_vec();
        Ok(keyword(&word).unwrap_or(Token::Name(word)))
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let start = self.at;
        if self.peek() == Some(b'0') && matches!(self.peek_at(1), Some(b'x') | Some(b'X')) {
            self.at += 2;
            while self.peek().is_some_and(|byte| byte.is_ascii_hexdigit()) {
                self.at += 1;
            }
            let digits = std::str::from_utf8(&self.source[start + 2..self.at])
                .map_err(|_| self.error("malformed number"))?;
            let value = u64::from_str_radix(digits, 16)
                .map(|value| value as f64)
                .map_err(|_| self.error("malformed number"))?;
            return Ok(Token::Number(value));
        }
        while self
            .peek()
            .is_some_and(|byte| byte.is_ascii_digit() || byte == b'.')
        {
            self.at += 1;
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            let exponent = self.at;
            self.at += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.at += 1;
            }
            if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                // `1e` on its own is a name in Lua, not a number.
                self.at = exponent;
                return Ok(Token::Number(1.0));
            }
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.at += 1;
            }
        }
        let text = std::str::from_utf8(&self.source[start..self.at])
            .map_err(|_| self.error("malformed number"))?;
        let value = text
            .parse::<f64>()
            .map_err(|_| self.error(format!("malformed number '{text}'")))?;
        if !value.is_finite() {
            return Err(self.error(format!("number '{text}' is out of range")));
        }
        Ok(Token::Number(value))
    }

    fn read_string(&mut self, quote: u8) -> Result<Token, String> {
        if self.peek_at(1) == Some(quote) && self.peek_at(2) == Some(quote) {
            return Ok(Token::Str(self.read_long_bracket(0, "string")?));
        }
        self.at += 1;
        let mut text = Vec::new();
        loop {
            let Some(byte) = self.peek() else {
                return Err(self.error("unfinished string"));
            };
            self.at += 1;
            match byte {
                b'\\' => text.push(self.read_escape()?),
                byte if byte == quote => return Ok(Token::Str(text)),
                b'\n' => {
                    self.line += 1;
                    text.push(byte);
                }
                byte => text.push(byte),
            }
        }
    }

    fn read_escape(&mut self) -> Result<u8, String> {
        let Some(byte) = self.peek() else {
            return Err(self.error("unfinished string"));
        };
        self.at += 1;
        Ok(match byte {
            b'a' => 0x07,
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 0x0b,
            b'\\' => b'\\',
            b'"' => b'"',
            b'\'' => b'\'',
            b'\n' => {
                self.line += 1;
                b'\n'
            }
            b'x' => {
                let mut digits = String::new();
                for _ in 0..2 {
                    let Some(digit) = self.peek().filter(u8::is_ascii_hexdigit) else {
                        return Err(self.error("hexadecimal digit expected"));
                    };
                    digits.push(char::from(digit));
                    self.at += 1;
                }
                u8::from_str_radix(&digits, 16).map_err(|_| self.error("malformed escape"))?
            }
            b'0'..=b'9' => {
                let mut digits = String::from(char::from(byte));
                while digits.len() < 3 && self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                    digits.push(char::from(self.source[self.at]));
                    self.at += 1;
                }
                digits
                    .parse::<u8>()
                    .map_err(|_| self.error("malformed escape"))?
            }
            other => {
                // Lua keeps an unknown escape as the character itself, which
                // is how a Windows path written as "\z" survives.
                other
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(source: &str) -> Vec<Token> {
        lex(source.as_bytes()).unwrap()
    }

    #[test]
    fn lexes_the_commands_a_client_library_sends() {
        assert_eq!(
            tokens("redis.call('GET', KEYS[1])"),
            vec![
                Token::Name(b"redis".to_vec()),
                Token::Dot,
                Token::Name(b"call".to_vec()),
                Token::LParen,
                Token::Str(b"GET".to_vec()),
                Token::Comma,
                Token::Name(b"KEYS".to_vec()),
                Token::LBracket,
                Token::Number(1.0),
                Token::RBracket,
                Token::RParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn a_backslash_at_the_end_of_a_line_continues_it() {
        assert_eq!(
            tokens("a = 1 \\\n+ 2"),
            vec![
                Token::Name(b"a".to_vec()),
                Token::Assign,
                Token::Number(1.0),
                Token::Plus,
                Token::Number(2.0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn a_number_may_carry_a_point_and_an_exponent() {
        assert_eq!(tokens("1.5")[0], Token::Number(1.5));
        assert_eq!(tokens(".5")[0], Token::Number(0.5));
        assert_eq!(tokens("1e3")[0], Token::Number(1000.0));
        assert_eq!(tokens("1E-3")[0], Token::Number(0.001));
        assert_eq!(tokens("0x10")[0], Token::Number(16.0));
        // `1e` is a name, not a broken number.
        assert_eq!(tokens("1e")[0], Token::Number(1.0));
        assert_eq!(tokens("1e")[1], Token::Name(b"e".to_vec()));
    }

    #[test]
    fn escapes_and_long_strings_read_the_way_lua_reads_them() {
        assert_eq!(tokens(r#"'a\nb\065\x41'"#)[0], Token::Str(b"a\nbAA".to_vec()));
        assert_eq!(tokens("[[plain\ntext]]")[0], Token::Str(b"plain\ntext".to_vec()));
        assert_eq!(tokens("[==[x]==]")[0], Token::Str(b"x".to_vec()));
        // A newline straight after the opening bracket is not part of the text.
        assert_eq!(tokens("[[\ntext]]")[0], Token::Str(b"text".to_vec()));
    }

    #[test]
    fn a_comment_runs_to_the_end_of_its_line() {
        assert_eq!(
            tokens("-- a comment\nreturn 1"),
            vec![Token::Return, Token::Number(1.0), Token::Eof]
        );
        assert_eq!(
            tokens("--[[ long\ncomment ]] return 1"),
            vec![Token::Return, Token::Number(1.0), Token::Eof]
        );
        // `x-1` is a subtraction, not the start of a comment.
        assert_eq!(tokens("x-1")[1], Token::Minus);
    }

    #[test]
    fn a_comment_after_an_expression_does_not_hide_it() {
        assert_eq!(
            tokens("local x = 1 -- set it"),
            vec![
                Token::Local,
                Token::Name(b"x".to_vec()),
                Token::Assign,
                Token::Number(1.0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn operators_are_distinguished_from_their_longer_neighbours() {
        assert_eq!(tokens("==")[0], Token::Equal);
        assert_eq!(tokens("=")[0], Token::Assign);
        assert_eq!(tokens("~=")[0], Token::NotEqual);
        assert_eq!(tokens("..")[0], Token::Concat);
        assert_eq!(tokens(".")[0], Token::Dot);
        assert!(lex(b"~1").is_err());
    }

    #[test]
    fn keywords_are_case_insensitive_and_names_are_not() {
        assert_eq!(tokens("THEN")[0], Token::Then);
        assert_eq!(tokens("Keys")[0], Token::Name(b"Keys".to_vec()));
    }
}
