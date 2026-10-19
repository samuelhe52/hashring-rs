//! The grammar a script may use, and the tree it parses into.
//!
//! This is Lua's grammar for the statements and expressions the gateway runs.
//! What is missing is missing on purpose, and each omission is refused by name
//! so a script that needs it is told what it asked for: functions, `repeat`,
//! and the generic `for ... in` loops, which need an iterator protocol that a
//! cache script has no use for.

use crate::script::lexer::{Token, lex};

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Local {
        names: Vec<Vec<u8>>,
        values: Vec<Expression>,
    },
    Assign {
        targets: Vec<Expression>,
        values: Vec<Expression>,
    },
    /// A call whose result is discarded, such as a bare `redis.call('DEL', k)`.
    Expression(Expression),
    If {
        arms: Vec<(Expression, Vec<Statement>)>,
        otherwise: Option<Vec<Statement>>,
    },
    While {
        condition: Expression,
        body: Vec<Statement>,
    },
    /// `for name = from, to [, step] do ... end`.
    For {
        name: Vec<u8>,
        from: Expression,
        to: Expression,
        step: Option<Expression>,
        body: Vec<Statement>,
    },
    Return(Vec<Expression>),
    Break,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Nil,
    Boolean(bool),
    Number(f64),
    Str(Vec<u8>),
    Name(Vec<u8>),
    Index(Box<Expression>, Box<Expression>),
    Field(Box<Expression>, Vec<u8>),
    Call(Box<Expression>, Vec<Expression>),
    /// `redis:call(...)`, which is Lua's spelling of a call with a named
    /// receiver and is what a hand-written script reaches for first.
    Method(Box<Expression>, Vec<u8>, Vec<Expression>),
    Table(Vec<TableField>),
    Binary(Operator, Box<Expression>, Box<Expression>),
    Unary(Operator, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableField {
    /// `{a, b}`: the values of the array part, in order.
    Positional(Expression),
    /// `{name = value}` and `{[key] = value}`: the named part.
    Named(Expression, Expression),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Power,
    Concat,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Negate,
    Not,
    Length,
}

pub fn parse(source: &[u8]) -> Result<Vec<Statement>, String> {
    let tokens = lex(source)?;
    let mut parser = Parser {
        tokens: &tokens,
        at: 0,
    };
    let body = parser.block(&[])?;
    parser.expect(Token::Eof)?;
    Ok(body)
}

/// What ends a block: the keywords that close the construct the block is in,
/// and `end`, which every one of them ends with.
const BLOCK_END: &[Token] = &[
    Token::End,
    Token::Else,
    Token::ElseIf,
    Token::Until,
    Token::Eof,
];

struct Parser<'a> {
    tokens: &'a [Token],
    at: usize,
}

impl Parser<'_> {
    fn peek(&self) -> &Token {
        self.tokens.get(self.at).unwrap_or(&Token::Eof)
    }

    fn peek_at(&self, ahead: usize) -> &Token {
        self.tokens.get(self.at + ahead).unwrap_or(&Token::Eof)
    }

    fn take(&mut self) -> Token {
        let token = self.peek().clone();
        self.at += 1;
        token
    }

    fn accept(&mut self, token: &Token) -> bool {
        if self.peek() == token {
            self.at += 1;
            return true;
        }
        false
    }

    fn expect(&mut self, token: Token) -> Result<(), String> {
        if self.peek() == &token {
            self.at += 1;
            return Ok(());
        }
        Err(format!(
            "'{}' expected near {}",
            token.describe(),
            self.peek().describe()
        ))
    }

    fn name(&mut self) -> Result<Vec<u8>, String> {
        let Token::Name(name) = self.take() else {
            return Err(format!("name expected near {}", self.peek().describe()));
        };
        Ok(name)
    }

    fn block(&mut self, stop: &[Token]) -> Result<Vec<Statement>, String> {
        let mut body = Vec::new();
        while !self.peek_is_stop(stop) {
            body.extend(self.statement()?);
        }
        Ok(body)
    }

    fn peek_is_stop(&self, stop: &[Token]) -> bool {
        let token = self.peek();
        BLOCK_END.contains(token) || stop.contains(token)
    }

    /// One statement, which may be several statements sharing a `then`, an
    /// `else` or a `;`, as Lua allows.
    fn statement(&mut self) -> Result<Vec<Statement>, String> {
        let mut statements = Vec::new();
        loop {
            if self.accept(&Token::Semi) {
                continue;
            }
            if self.peek_is_stop(&[]) {
                return Ok(statements);
            }
            let statement = match self.peek() {
                Token::If => self.if_statement()?,
                Token::While => self.while_statement()?,
                Token::For => self.for_statement()?,
                Token::Return => {
                    self.at += 1;
                    // `return` ends the block even with nothing after it, so
                    // the values are only read when one can be.
                    let values = if self.peek_is_stop(&[]) || self.peek() == &Token::Semi {
                        Vec::new()
                    } else {
                        self.expression_list()?
                    };
                    return Ok(vec![Statement::Return(values)]);
                }
                Token::Break => {
                    self.at += 1;
                    self.accept(&Token::Semi);
                    return Ok(vec![Statement::Break]);
                }
                Token::Do => {
                    self.at += 1;
                    let body = self.block(&[])?;
                    self.expect(Token::End)?;
                    return Ok(body);
                }
                Token::Repeat => {
                    return Err("'repeat ... until' is not supported in this gateway".into());
                }
                Token::Function => {
                    return Err("'function' is not supported in this gateway".into());
                }
                _ => self.simple_statement()?,
            };
            statements.push(statement);
            if self.accept(&Token::Semi) {
                continue;
            }
            if self.peek_is_stop(&[]) {
                return Ok(statements);
            }
        }
    }

    fn if_statement(&mut self) -> Result<Statement, String> {
        self.expect(Token::If)?;
        let mut arms = Vec::new();
        let condition = self.expression()?;
        self.expect(Token::Then)?;
        arms.push((condition, self.block(&[])?));
        let mut otherwise = None;
        loop {
            match self.peek() {
                Token::ElseIf => {
                    self.at += 1;
                    let condition = self.expression()?;
                    self.expect(Token::Then)?;
                    arms.push((condition, self.block(&[])?));
                }
                Token::Else => {
                    self.at += 1;
                    otherwise = Some(self.block(&[])?);
                    break;
                }
                Token::End => {
                    self.at += 1;
                    break;
                }
                _ => return Err(format!("'end' expected near {}", self.peek().describe())),
            }
        }
        Ok(Statement::If { arms, otherwise })
    }

    fn while_statement(&mut self) -> Result<Statement, String> {
        self.expect(Token::While)?;
        let condition = self.expression()?;
        self.expect(Token::Do)?;
        let body = self.block(&[])?;
        self.expect(Token::End)?;
        Ok(Statement::While { condition, body })
    }

    fn for_statement(&mut self) -> Result<Statement, String> {
        self.expect(Token::For)?;
        let name = self.name()?;
        if self.accept(&Token::Assign) {
            let from = self.expression()?;
            self.expect(Token::Comma)?;
            let to = self.expression()?;
            let step = if self.accept(&Token::Comma) {
                Some(self.expression()?)
            } else {
                None
            };
            self.expect(Token::Do)?;
            let body = self.block(&[])?;
            self.expect(Token::End)?;
            return Ok(Statement::For {
                name,
                from,
                to,
                step,
                body,
            });
        }
        // The only other `for` is the generic one, which needs an iterator.
        Err("'for ... in' is not supported in this gateway; only the numeric form is".into())
    }

    /// `local`, an assignment, or a call whose result is thrown away.
    fn simple_statement(&mut self) -> Result<Statement, String> {
        if self.accept(&Token::Local) {
            let mut names = vec![self.name()?];
            while self.accept(&Token::Comma) {
                names.push(self.name()?);
            }
            let values = if self.accept(&Token::Assign) {
                self.expression_list()?
            } else {
                Vec::new()
            };
            if values.len() > names.len() {
                return Err("too many values in a local declaration".into());
            }
            return Ok(Statement::Local { names, values });
        }
        let first = self.suffixed_expression()?;
        if self.accept(&Token::Assign) || self.accept(&Token::Comma) {
            let mut targets = vec![first];
            while self.accept(&Token::Comma) {
                targets.push(self.suffixed_expression()?);
            }
            for target in &targets {
                if !matches!(target, Expression::Name(_) | Expression::Index(..)) {
                    return Err("cannot assign to this expression".into());
                }
            }
            self.expect(Token::Assign)?;
            let values = self.expression_list()?;
            if values.len() != targets.len() {
                return Err(format!(
                    "{} values for {} targets",
                    values.len(),
                    targets.len()
                ));
            }
            return Ok(Statement::Assign { targets, values });
        }
        match first {
            Expression::Call(..) | Expression::Method(..) => Ok(Statement::Expression(first)),
            other => Err(format!("syntax error near {}", describe(&other))),
        }
    }

    fn expression_list(&mut self) -> Result<Vec<Expression>, String> {
        let mut values = vec![self.expression()?];
        while self.accept(&Token::Comma) {
            values.push(self.expression()?);
        }
        Ok(values)
    }

    /// Precedence, loosest first: `or`, `and`, the comparisons, `..`, the
    /// additive and multiplicative operators, the unary operators and `^`, in
    /// the order Lua resolves them. Each is its own function because the
    /// associativity differs between them, not because they share a table.
    fn expression(&mut self) -> Result<Expression, String> {
        self.or_expression()
    }

    fn or_expression(&mut self) -> Result<Expression, String> {
        let mut left = self.and_expression()?;
        while self.accept(&Token::Or) {
            let right = self.and_expression()?;
            left = Expression::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_expression(&mut self) -> Result<Expression, String> {
        let mut left = self.comparison()?;
        while self.accept(&Token::And) {
            let right = self.comparison()?;
            left = Expression::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn comparison(&mut self) -> Result<Expression, String> {
        let mut left = self.concat()?;
        while let Some(operator) = [
            (Token::LessEqual, Operator::LessEqual),
            (Token::GreaterEqual, Operator::GreaterEqual),
            (Token::Less, Operator::Less),
            (Token::Greater, Operator::Greater),
            (Token::NotEqual, Operator::NotEqual),
            (Token::Equal, Operator::Equal),
        ]
        .into_iter()
        .find(|(token, _)| self.peek() == token)
        {
            self.at += 1;
            let right = self.concat()?;
            left = Expression::Binary(operator.1, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn concat(&mut self) -> Result<Expression, String> {
        let mut left = self.additive()?;
        while self.accept(&Token::Concat) {
            let right = self.additive()?;
            left = Expression::Binary(
                Operator::Concat,
                Box::new(left),
                Box::new(right),
            );
        }
        Ok(left)
    }

    fn additive(&mut self) -> Result<Expression, String> {
        let mut left = self.multiplicative()?;
        while let Some(operator) = [
            (Token::Plus, Operator::Add),
            (Token::Minus, Operator::Subtract),
        ]
        .into_iter()
        .find(|(token, _)| self.peek() == token)
        {
            self.at += 1;
            let right = self.multiplicative()?;
            left = Expression::Binary(operator.1, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> Result<Expression, String> {
        let mut left = self.unary()?;
        while let Some(operator) = [
            (Token::Star, Operator::Multiply),
            (Token::Slash, Operator::Divide),
            (Token::Percent, Operator::Remainder),
        ]
        .into_iter()
        .find(|(token, _)| self.peek() == token)
        {
            self.at += 1;
            let right = self.unary()?;
            left = Expression::Binary(operator.1, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expression, String> {
        if self.accept(&Token::Not) {
            return Ok(Expression::Unary(Operator::Not, Box::new(self.unary()?)));
        }
        if self.accept(&Token::Minus) {
            return Ok(Expression::Unary(
                Operator::Negate,
                Box::new(self.unary()?),
            ));
        }
        if self.accept(&Token::Hash) {
            return Ok(Expression::Unary(
                Operator::Length,
                Box::new(self.unary()?),
            ));
        }
        self.power()
    }

    fn power(&mut self) -> Result<Expression, String> {
        let base = self.suffixed_expression()?;
        if self.accept(&Token::Caret) {
            let exponent = self.unary()?;
            return Ok(Expression::Binary(
                Operator::Power,
                Box::new(base),
                Box::new(exponent),
            ));
        }
        Ok(base)
    }

    /// A name and everything that can follow it: indexing, field access and
    /// calls. A literal on its own is not one of these, which is why
    /// `("x"):upper()` would be refused and `x.y` is not.
    fn suffixed_expression(&mut self) -> Result<Expression, String> {
        let mut expression = Expression::Name(self.name()?);
        loop {
            match self.peek() {
                Token::LBracket => {
                    self.at += 1;
                    let key = self.expression()?;
                    self.expect(Token::RBracket)?;
                    expression = Expression::Index(Box::new(expression), Box::new(key));
                }
                Token::Dot => {
                    self.at += 1;
                    let field = self.name()?;
                    expression = Expression::Field(Box::new(expression), field);
                }
                Token::Colon => {
                    self.at += 1;
                    let method = self.name()?;
                    let arguments = self.call_arguments()?;
                    expression = Expression::Method(Box::new(expression), method, arguments);
                }
                Token::LParen | Token::Str(_) | Token::LBrace => {
                    let arguments = self.call_arguments()?;
                    expression = Expression::Call(Box::new(expression), arguments);
                }
                _ => return Ok(expression),
            }
        }
    }

    fn call_arguments(&mut self) -> Result<Vec<Expression>, String> {
        match self.peek() {
            Token::LParen => {
                self.at += 1;
                let arguments = if self.accept(&Token::RParen) {
                    Vec::new()
                } else {
                    let arguments = self.expression_list()?;
                    self.expect(Token::RParen)?;
                    arguments
                };
                Ok(arguments)
            }
            // A call written without brackets: `f "text"` and `f {1, 2}`.
            Token::Str(_) => Ok(vec![self.simple_expression()?]),
            Token::LBrace => Ok(vec![self.simple_expression()?]),
            _ => Err(format!(
                "function arguments expected near {}",
                self.peek().describe()
            )),
        }
    }

    /// A call argument that is one value rather than a list.
    fn simple_expression(&mut self) -> Result<Expression, String> {
        Ok(match self.take() {
            Token::Nil => Expression::Nil,
            Token::True => Expression::Boolean(true),
            Token::False => Expression::Boolean(false),
            Token::Number(value) => Expression::Number(value),
            Token::Str(value) => Expression::Str(value),
            Token::LBrace => self.table()?,
            other => return Err(format!("unexpected token '{}'", other.describe())),
        })
    }

    fn table(&mut self) -> Result<Expression, String> {
        let mut fields = Vec::new();
        loop {
            if self.accept(&Token::RBrace) {
                return Ok(Expression::Table(fields));
            }
            // `{name = value}` and `{[key] = value}` name a field; anything
            // else is the next value of the array part.
            if self.peek_is_named_field() {
                let key = if self.accept(&Token::LBracket) {
                    let key = self.expression()?;
                    self.expect(Token::RBracket)?;
                    key
                } else {
                    Expression::Name(self.name()?)
                };
                self.expect(Token::Assign)?;
                let value = self.expression()?;
                fields.push(TableField::Named(key, value));
            } else {
                fields.push(TableField::Positional(self.expression()?));
            }
            if !self.accept(&Token::Comma) && !self.accept(&Token::Semi) {
                self.expect(Token::RBrace)?;
                return Ok(Expression::Table(fields));
            }
        }
    }

    /// Whether the tokens at the cursor name a field of a table constructor.
    fn peek_is_named_field(&self) -> bool {
        matches!(self.peek(), Token::Name(_)) && self.peek_at(1) == &Token::Assign
    }
}

fn describe(expression: &Expression) -> String {
    match expression {
        Expression::Name(name) => String::from_utf8_lossy(name).into_owned(),
        other => format!("{other:?}").to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(source: &str) -> Statement {
        let mut body = parse(source.as_bytes()).unwrap();
        assert_eq!(body.len(), 1, "expected one statement in {source}");
        body.pop().unwrap()
    }

    #[test]
    fn parses_the_claim_script_a_cache_client_sends() {
        let body = parse(
            b"if redis.call('GET', KEYS[1]) == '1' then return 0 end \
              redis.call('SET', KEYS[1], '1', 'EX', ARGV[1]) return 1",
        )
        .unwrap();
        assert_eq!(body.len(), 3);
        assert!(matches!(body[0], Statement::If { .. }));
        assert!(matches!(body[1], Statement::Expression(_)));
        assert_eq!(body[2], Statement::Return(vec![Expression::Number(1.0)]));
    }

    #[test]
    fn parses_the_rate_limiter_script_a_client_library_sends() {
        let body = parse(
            b"redis.call('set', KEYS[1], 0, 'EX', ARGV[2], 'NX') \
              local consumed = redis.call('incrby', KEYS[1], ARGV[1]) \
              local ttl = redis.call('pttl', KEYS[1]) \
              if ttl == -1 then \
                redis.call('expire', KEYS[1], ARGV[2]) \
                ttl = 1000 * ARGV[2] \
              end \
              return {consumed, ttl}",
        )
        .unwrap();
        assert_eq!(body.len(), 6);
        assert_eq!(
            body[5],
            Statement::Return(vec![Expression::Table(vec![TableField::Positional(
                Expression::Name(b"consumed".to_vec())
            ), TableField::Positional(Expression::Name(b"ttl".to_vec())])])
        );
    }

    #[test]
    fn several_statements_may_share_one_line() {
        let body = parse(b"local a = 1 local b = 2").unwrap();
        assert_eq!(body.len(), 2);
    }

    #[test]
    fn local_without_a_value_declares_nothing_yet() {
        assert_eq!(
            parse_one("local a"),
            Statement::Local {
                names: vec![b"a".to_vec()],
                values: vec![]
            }
        );
    }

    #[test]
    fn precedence_follows_lua() {
        // `1 + 2 * 3` binds the multiplication first.
        assert_eq!(
            parse_one("return 1 + 2 * 3"),
            Statement::Return(vec![Expression::Binary(
                Operator::Add,
                Box::new(Expression::Number(1.0)),
                Box::new(Expression::Binary(
                    Operator::Multiply,
                    Box::new(Expression::Number(2.0)),
                    Box::new(Expression::Number(3.0))
                ))
            )])
        );
        // Concatenation binds tighter than comparison, and looser than `+`.
        assert_eq!(
            parse_one("return 'a' .. 'b' == 'ab'"),
            Statement::Return(vec![Expression::Binary(
                Operator::Equal,
                Box::new(Expression::Binary(
                    Operator::Concat,
                    Box::new(Expression::Str(b"a".to_vec())),
                    Box::new(Expression::Str(b"b".to_vec()))
                )),
                Box::new(Expression::Str(b"ab".to_vec()))
            )])
        );
        // `and` binds tighter than `or`.
        assert!(matches!(
            parse_one("return a and b or c"),
            Statement::Return(vec![Expression::Or(..)])
        ));
        assert_eq!(
            parse_one("return -x"),
            Statement::Return(vec![Expression::Unary(
                Operator::Negate,
                Box::new(Expression::Name(b"x".to_vec()))
            )])
        );
        assert_eq!(
            parse_one("return not x"),
            Statement::Return(vec![Expression::Unary(
                Operator::Not,
                Box::new(Expression::Name(b"x".to_vec()))
            )])
        );
        assert_eq!(
            parse_one("return #t"),
            Statement::Return(vec![Expression::Unary(
                Operator::Length,
                Box::new(Expression::Name(b"t".to_vec()))
            )])
        );
        assert!(matches!(
            parse_one("return 2 ^ 3 ^ 2"),
            Statement::Return(vec![Expression::Binary(Operator::Power, ..)])
        ));
    }

    #[test]
    fn a_method_call_is_a_call_with_a_named_receiver() {
        assert_eq!(
            parse_one("return redis:call('GET', 'k')"),
            Statement::Return(vec![Expression::Method(
                Box::new(Expression::Name(b"redis".to_vec())),
                b"call".to_vec(),
                vec![
                    Expression::Str(b"GET".to_vec()),
                    Expression::Str(b"k".to_vec())
                ]
            )])
        );
    }

    #[test]
    fn a_table_constructor_holds_array_and_named_fields() {
        assert_eq!(
            parse_one("return {1, [2] = 3, four = 4}"),
            Statement::Return(vec![Expression::Table(vec![
                TableField::Positional(Expression::Number(1.0)),
                TableField::Named(Expression::Number(2.0), Expression::Number(3.0)),
                TableField::Named(Expression::Name(b"four".to_vec()), Expression::Number(4.0)),
            ])])
        );
    }

    #[test]
    fn loops_parse_into_their_own_statements() {
        assert!(matches!(parse_one("while true do end"), Statement::While { .. }));
        assert!(matches!(
            parse_one("for i = 1, 10 do local x = i end"),
            Statement::For { .. }
        ));
        assert!(matches!(
            parse_one("for i = 10, 1, -1 do end"),
            Statement::For { .. }
        ));
    }

    #[test]
    fn what_is_not_supported_is_refused_by_name() {
        for (source, expected) in [
            ("function f() end", "function"),
            ("repeat until true", "repeat"),
            ("for k, v in pairs(t) do end", "for ... in"),
            ("local f = function() end", "function"),
        ] {
            let error = parse(source.as_bytes()).unwrap_err();
            assert!(
                error.contains(expected),
                "{source} reported {error}, which does not name {expected}"
            );
        }
    }

    #[test]
    fn a_mistake_is_reported_rather_than_guessed_at() {
        assert!(parse(b"local = 1").is_err());
        assert!(parse(b"if x then").is_err());
        assert!(parse(b"return 1 +").is_err());
        assert!(parse(b"local a = {1").is_err());
        assert!(parse(b"x == 1").is_err(), "an expression is not a statement");
    }

    #[test]
    fn an_empty_script_is_a_script_that_does_nothing() {
        assert!(parse(b"").unwrap().is_empty());
        assert!(parse(b"-- nothing\n").unwrap().is_empty());
    }
}
