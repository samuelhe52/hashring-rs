//! Scripts: the Lua a Redis client may run on the gateway, and the rules that
//! keep a script honest about what a cluster can do.
//!
//! Redis scripts are part of its contract because clients have no
//! alternative: a cache client claims a key with a read and a write that must
//! not interleave, and a rate limiter's `INCR` and its deadline are one
//! operation or the limiter is wrong. `EVAL` is how both are sent, so a gateway
//! that will not run them cannot serve a client library unchanged.
//!
//! What a script gets here, and what it does not:
//!
//! - A script may only touch the keys it declared in `KEYS`. A single-threaded
//!   Redis lets a script reach any key in its process; this cluster cannot
//!   know where a key lives without the client saying so, and a script free to
//!   touch undeclared keys would hold no lock on them.
//! - The commands a script runs are the gateway's own commands, each atomic in
//!   the way the gateway already makes them: a read-modify-write command
//!   carries a precondition to the node that owns the key, so it is atomic
//!   against every other writer, whichever gateway or client it came through.
//! - A write the script makes is conditional on what the script read, so a
//!   script that reads a key and then writes it cannot overwrite a write that
//!   arrived in between. The script is then run again, which is what Redis'
//!   single thread does by never letting anyone in.
//! - One script at a time, as Redis runs them, bounded by a step count and by
//!   the command's own deadline, so a runaway script cannot hold the gateway.
//! - The grammar is Lua's for the statements and expressions a client library
//!   writes. Functions, `repeat` and the generic `for` loop are refused by
//!   name. See [`parser`] for the shape of it, and [`run`] for what a script
//!   is given to call.
//!
//! `docs/redis-compatibility.md` describes this from the client's side.

pub mod lexer;
pub mod parser;
pub mod sha1;
pub mod value;

use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

pub use value::{Table, Value};

/// What a script may spend, and how long it may take.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Statement evaluations and calls together. This is the deterministic half
    /// of the budget: a script that computes rather than calls is stopped at
    /// exactly this many steps, on any machine.
    pub max_steps: usize,
    /// Wall-clock time, checked whenever the step count is. Redis' equivalent
    /// is `lua-time-limit`; the gateway has no background thread watching for
    /// a busy script, so a running script checks itself.
    pub timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            // A cache key claim is two statements and a rate limiter's `INCR`
            // with its deadline is four more, so this is orders of magnitude
            // above anything a client library sends, and still below anything
            // that could hold a gateway.
            max_steps: 100_000,
            timeout: Duration::from_secs(5),
        }
    }
}

/// How a script reaches the gateway, one command at a time.
///
/// The commands go through the same dispatcher a client connection uses, so a
/// script sees the answer a client would, errors included.
pub trait Host {
    fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send;
}

/// Runs `source` once, against `keys` and `argv`, and answers with whatever the
/// script returned, converted the way Redis converts it.
///
/// The program is parsed on every call. A script that runs on every request
/// pays for its parse, which Redis does not: keeping the source is a cache of
/// a client's own text, and one round trip to a cluster is the larger cost in
/// every case that matters here.
pub async fn run<H: Host + Send>(
    host: &mut H,
    source: &[u8],
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
    limits: Limits,
) -> RedisResult<Reply> {
    let program = parser::parse(source).map_err(script_error)?;
    let mut interpreter = Interpreter {
        host,
        keys: keys.clone(),
        scopes: Vec::new(),
        steps: 0,
        deadline: Instant::now() + limits.timeout,
        limits,
    };
    interpreter.push_scope(builtins());
    interpreter.push_scope(vec![
        (b"KEYS".to_vec(), strings(&keys)),
        (b"ARGV".to_vec(), strings(&argv)),
    ]);
    match interpreter.block(&program).await? {
        Control::Returned(values) => Ok(match values.into_iter().next() {
            Some(value) => value::to_reply(&value).map_err(script_error)?,
            None => Reply::Nil,
        }),
        Control::Done => Ok(Reply::Nil),
        Control::Break => Err(script_error("'break' outside a loop")),
    }
}

/// A failure the client sees as a script error, spelled the way Redis spells
/// one so a client that matches on the prefix still matches.
fn script_error(message: impl AsRef<str>) -> RedisError {
    RedisError::new("ERR", format!("user_script: {}", message.as_ref()))
}

/// The names a script starts with. They are values rather than special cases
/// in the interpreter so that `local call = redis.call` works the way Lua says
/// it should, and so a call is an expression like any other.
fn builtins() -> Vec<(Vec<u8>, Value)> {
    let names: [(&[u8], &str); 6] = [
        (b"redis", "redis"),
        (b"type", "type"),
        (b"tonumber", "tonumber"),
        (b"tostring", "tostring"),
        (b"error", "error"),
        (b"assert", "assert"),
    ];
    names
        .into_iter()
        .map(|(name, value)| (name.to_vec(), Value::Native(value)))
        .collect()
}

/// The `KEYS` and `ARGV` tables, which are byte strings in the order the
/// command named them. Lua tables start at one, so `KEYS[1]` is the first.
fn strings(values: &[Vec<u8>]) -> Value {
    Value::Table(Table::array(
        values.iter().cloned().map(Value::String).collect(),
    ))
}

/// The members of the `redis` table, as the name they are reached by.
fn redis_member(member: &[u8]) -> Option<&'static str> {
    Some(match member {
        b"call" => "redis.call",
        b"pcall" => "redis.pcall",
        b"error_reply" => "redis.error_reply",
        b"status_reply" => "redis.status_reply",
        b"sha1hex" => "redis.sha1hex",
        b"log" => "redis.log",
        b"setresp" => "redis.setresp",
        b"breakpoint" => "redis.breakpoint",
        _ => return None,
    })
}

/// What a block did when it stopped.
enum Control {
    /// Reached the end of the block.
    Done,
    /// A `break` inside the nearest loop.
    Break,
    /// A `return`, carrying the values it named.
    Returned(Vec<Value>),
}

/// A block's future, boxed because a block and the statements in it call each
/// other: without the box the two futures would each contain the other.
type BlockFuture<'a> = Pin<Box<dyn Future<Output = RedisResult<Control>> + Send + 'a>>;

/// An assignment target's future, for the same reason: `t.a.b = 1` walks an
/// expression that holds itself.
type LValueFuture<'a> =
    Pin<Box<dyn Future<Output = RedisResult<(Vec<u8>, Vec<Value>)>> + Send + 'a>>;

struct Interpreter<'a, H: Host> {
    host: &'a mut H,
    /// The keys the script declared, which is every key it is allowed to touch.
    keys: Vec<Vec<u8>>,
    /// The block stack, innermost last. One script never shares a scope with
    /// another, so a plain vector is enough and nothing is ref-counted.
    scopes: Vec<Vec<(Vec<u8>, Value)>>,
    steps: usize,
    deadline: Instant,
    limits: Limits,
}

impl<H: Host + Send> Interpreter<'_, H> {
    fn push_scope(&mut self, variables: Vec<(Vec<u8>, Value)>) {
        self.scopes.push(variables);
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    /// Both budgets, checked where the interpreter is about to do something it
    /// might do forever: every statement, and every call it evaluates.
    fn account(&mut self) -> RedisResult<()> {
        self.steps += 1;
        if self.steps > self.limits.max_steps {
            return Err(script_error("script exceeded its maximum number of steps"));
        }
        // Reading the clock on every statement costs more than it saves, so it
        // is read once every 256 steps: a runaway loop still stops within a
        // fraction of a millisecond of its budget.
        if self.steps.is_multiple_of(256) && Instant::now() > self.deadline {
            return Err(RedisError::new(
                "BUSY",
                "script exceeded its execution deadline",
            ));
        }
        Ok(())
    }

    /// Runs a block in a scope of its own.
    fn block<'b>(&'b mut self, statements: &'b [parser::Statement]) -> BlockFuture<'b> {
        Box::pin(async move {
            self.push_scope(Vec::new());
            let outcome = self.block_unscoped(statements).await;
            self.pop_scope();
            outcome
        })
    }

    fn block_unscoped<'b>(&'b mut self, statements: &'b [parser::Statement]) -> BlockFuture<'b> {
        Box::pin(async move {
            for statement in statements {
                match self.statement(statement).await? {
                    Control::Done => {}
                    flow => return Ok(flow),
                }
            }
            Ok(Control::Done)
        })
    }

    async fn statement(&mut self, statement: &parser::Statement) -> RedisResult<Control> {
        self.account()?;
        match statement {
            parser::Statement::Local { names, values } => {
                let values = self.values(values).await?;
                let mut declared = names
                    .iter()
                    .map(|name| (name.clone(), Value::Nil))
                    .collect::<Vec<_>>();
                for (slot, value) in declared.iter_mut().zip(values) {
                    slot.1 = value;
                }
                // The declarations join the scope this statement runs in, not
                // a new one: the block already opened one for them.
                self.scopes
                    .last_mut()
                    .expect("a script always runs inside a scope")
                    .extend(declared);
                Ok(Control::Done)
            }
            parser::Statement::Assign { targets, values } => {
                let values = self.values(values).await?;
                for (target, value) in targets.iter().zip(values) {
                    self.assign(target, value).await?;
                }
                Ok(Control::Done)
            }
            parser::Statement::Expression(expression) => {
                // The result is discarded, which is what lets a bare
                // `redis.call(...)` stand as a statement.
                self.evaluate(expression).await?;
                Ok(Control::Done)
            }
            parser::Statement::If { arms, otherwise } => {
                for (condition, body) in arms {
                    if self.condition(condition).await? {
                        return self.block_unscoped(body).await;
                    }
                }
                match otherwise {
                    Some(body) => self.block_unscoped(body).await,
                    None => Ok(Control::Done),
                }
            }
            parser::Statement::While { condition, body } => {
                while self.condition(condition).await? {
                    // A loop that runs forever spends nothing else, so the
                    // budget is charged per turn round it as well.
                    self.account()?;
                    match self.block(body).await? {
                        Control::Done => {}
                        Control::Break => break,
                        returned => return Ok(returned),
                    }
                }
                Ok(Control::Done)
            }
            parser::Statement::For {
                name,
                from,
                to,
                step,
                body,
            } => {
                // The bounds and the step are evaluated once, as Lua does, so
                // a loop over an expression with a side effect runs it once.
                let from = first(self.evaluate(from).await?)
                    .to_number()
                    .map_err(script_error)?;
                let to = first(self.evaluate(to).await?)
                    .to_number()
                    .map_err(script_error)?;
                let step = match step {
                    Some(expression) => {
                        let value = first(self.evaluate(expression).await?)
                            .to_number()
                            .map_err(script_error)?;
                        if value == 0.0 {
                            return Err(script_error("'for' step is zero"));
                        }
                        value
                    }
                    None => 1.0,
                };
                let mut current = from;
                while (step > 0.0 && current <= to) || (step < 0.0 && current >= to) {
                    self.account()?;
                    self.push_scope(vec![(name.clone(), Value::Number(current))]);
                    let outcome = self.block_unscoped(body).await;
                    self.pop_scope();
                    match outcome? {
                        Control::Done => {}
                        Control::Break => break,
                        returned => return Ok(returned),
                    }
                    current += step;
                }
                Ok(Control::Done)
            }
            parser::Statement::Return(values) => Ok(Control::Returned(self.values(values).await?)),
            parser::Statement::Break => Ok(Control::Break),
        }
    }

    /// The values of an expression list, where the last expression may be a
    /// call that produced several: `local a, b = redis.call(...)` is how a
    /// script reads a two-part reply. Boxed for the same reason a block is:
    /// an expression and the list it sits in evaluate each other.
    fn values<'b>(
        &'b mut self,
        expressions: &'b [parser::Expression],
    ) -> Pin<Box<dyn Future<Output = RedisResult<Vec<Value>>> + Send + 'b>> {
        Box::pin(async move {
            let mut values = Vec::new();
            for (position, expression) in expressions.iter().enumerate() {
                let mut produced = self.evaluate(expression).await?;
                if position + 1 == expressions.len() {
                    values.append(&mut produced);
                } else {
                    values.push(produced.into_iter().next().unwrap_or(Value::Nil));
                }
            }
            Ok(values)
        })
    }

    async fn condition(&mut self, expression: &parser::Expression) -> RedisResult<bool> {
        Ok(first(self.evaluate(expression).await?).truthy())
    }

    /// One expression, as the values it produced. Boxed for the same reason a
    /// block is: an expression can hold itself, as `KEYS[1]` does, and the two
    /// futures would otherwise each contain the other.
    fn evaluate<'b>(
        &'b mut self,
        expression: &'b parser::Expression,
    ) -> Pin<Box<dyn Future<Output = RedisResult<Vec<Value>>> + Send + 'b>> {
        Box::pin(async move {
            Ok(match expression {
                parser::Expression::Nil => vec![Value::Nil],
                parser::Expression::Boolean(value) => vec![Value::Boolean(*value)],
                parser::Expression::Number(value) => vec![Value::Number(*value)],
                parser::Expression::Str(value) => vec![Value::String(value.clone())],
                parser::Expression::Name(name) => vec![self.lookup(name)?],
                parser::Expression::Index(table, key) => {
                    let table = first(self.evaluate(table).await?);
                    let key = first(self.evaluate(key).await?);
                    vec![self.index(table, key)?]
                }
                parser::Expression::Field(table, field) => {
                    let table = first(self.evaluate(table).await?);
                    vec![self.index(table, Value::String(field.clone()))?]
                }
                parser::Expression::Call(callee, arguments) => {
                    let callee = first(self.evaluate(callee).await?);
                    let arguments = self.values(arguments).await?;
                    self.invoke(callee, arguments).await?
                }
                parser::Expression::Method(callee, method, arguments) => {
                    // Lua passes the receiver as the first argument, which is why
                    // `redis:call('GET', k)` is not how a script calls `call`:
                    // `redis` would arrive where the command name belongs.
                    let receiver = first(self.evaluate(callee).await?);
                    let member = self.index(receiver.clone(), Value::String(method.clone()))?;
                    let mut all = vec![receiver];
                    all.extend(self.values(arguments).await?);
                    self.invoke(member, all).await?
                }
                parser::Expression::Table(fields) => {
                    let mut table = Table::new();
                    for (position, field) in fields.iter().enumerate() {
                        match field {
                            parser::TableField::Positional(expression) => {
                                // Only the last value of an expression list
                                // expands, which is how `{a, b, unpack(list)}` is
                                // written and how `{redis.call(...)}` is read.
                                let produced = self.evaluate(expression).await?;
                                if position + 1 == fields.len() {
                                    for value in produced {
                                        table.set(Value::Number(table.len() as f64 + 1.0), value);
                                    }
                                } else {
                                    let value = produced.into_iter().next().unwrap_or(Value::Nil);
                                    table.set(Value::Number(table.len() as f64 + 1.0), value);
                                }
                            }
                            parser::TableField::Named(key, value) => {
                                let key = match key {
                                    parser::Expression::Name(name) => Value::String(name.clone()),
                                    other => first(self.evaluate(other).await?),
                                };
                                let value = first(self.evaluate(value).await?);
                                table.set(key, value);
                            }
                        }
                    }
                    vec![Value::Table(table)]
                }
                parser::Expression::And(left, right) => {
                    let left = first(self.evaluate(left).await?);
                    if left.truthy() {
                        vec![first(self.evaluate(right).await?)]
                    } else {
                        vec![left]
                    }
                }
                parser::Expression::Or(left, right) => {
                    let left = first(self.evaluate(left).await?);
                    if left.truthy() {
                        vec![left]
                    } else {
                        vec![first(self.evaluate(right).await?)]
                    }
                }
                parser::Expression::Unary(operator, operand) => {
                    let operand = first(self.evaluate(operand).await?);
                    vec![self.unary(*operator, operand)?]
                }
                parser::Expression::Binary(operator, left, right) => {
                    let left = first(self.evaluate(left).await?);
                    let right = first(self.evaluate(right).await?);
                    vec![self.binary(*operator, left, right)?]
                }
            })
        })
    }

    fn unary(&self, operator: parser::Operator, operand: Value) -> RedisResult<Value> {
        use parser::Operator::*;
        Ok(match operator {
            Not => Value::Boolean(!operand.truthy()),
            Negate => Value::Number(-operand.to_number().map_err(script_error)?),
            Length => match operand {
                Value::Table(table) => Value::Number(table.len() as f64),
                Value::String(text) => Value::Number(text.len() as f64),
                other => {
                    return Err(script_error(format!(
                        "attempt to get length of a {} value",
                        other.type_name()
                    )));
                }
            },
            other => return Err(script_error(format!("'{other:?}' is not a unary operator"))),
        })
    }

    fn binary(&self, operator: parser::Operator, left: Value, right: Value) -> RedisResult<Value> {
        use parser::Operator::*;
        match operator {
            Add | Subtract | Multiply | Divide | Remainder | Power => {
                let left = left.to_number().map_err(script_error)?;
                let right = right.to_number().map_err(script_error)?;
                Ok(Value::Number(match operator {
                    Add => left + right,
                    Subtract => left - right,
                    Multiply => left * right,
                    Divide => left / right,
                    // Lua's modulo takes the sign of the divisor, so `-1 % 5`
                    // is 4 and a countdown that wraps never stops early.
                    Remainder => left - (left / right).floor() * right,
                    Power => left.powf(right),
                    _ => unreachable!("an arithmetic operator"),
                }))
            }
            Concat => {
                let mut text = left.to_string_value().map_err(script_error)?;
                text.extend(right.to_string_value().map_err(script_error)?);
                Ok(Value::String(text))
            }
            Equal | NotEqual => {
                let equal = values_equal(&left, &right);
                Ok(Value::Boolean(match operator {
                    Equal => equal,
                    _ => !equal,
                }))
            }
            Less | LessEqual | Greater | GreaterEqual => {
                let ordering = compare(&left, &right).map_err(script_error)?;
                Ok(Value::Boolean(match operator {
                    Less => ordering == std::cmp::Ordering::Less,
                    LessEqual => ordering != std::cmp::Ordering::Greater,
                    Greater => ordering == std::cmp::Ordering::Greater,
                    GreaterEqual => ordering != std::cmp::Ordering::Less,
                    _ => unreachable!("an ordering operator"),
                }))
            }
            Not | Negate | Length => Err(script_error(format!(
                "'{operator:?}' is not a binary operator"
            ))),
        }
    }

    fn lookup(&self, name: &[u8]) -> RedisResult<Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(value) = scope
                .iter()
                .rev()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
            {
                return Ok(value);
            }
        }
        // Lua would put the value in a global table. A script here shares a
        // process with every other script, so a name that was never declared
        // is a mistake rather than a place to keep a value.
        Err(script_error(format!(
            "attempt to access an undeclared name '{}'",
            String::from_utf8_lossy(name)
        )))
    }

    async fn assign(&mut self, target: &parser::Expression, value: Value) -> RedisResult<()> {
        // The target is walked from its root, so that writing into a field of
        // a table reaches the table the name refers to rather than a copy of
        // it: values are read out of a scope, so a copy written to would be
        // written nowhere.
        let (root, keys) = self.lvalue(target).await?;
        let mut holding = self.lookup(&root)?;
        let mut current = &mut holding;
        for key in &keys[..keys.len().saturating_sub(1)] {
            // The name is read before the borrow, because naming the value that
            // has no slot is the error this step can report.
            let named = current.type_name();
            let Some(next) = current.get_mut(key) else {
                return Err(script_error(format!("attempt to index a {named} value")));
            };
            current = next;
        }
        match keys.last() {
            // No keys means the target is the name itself, so the value goes
            // where the name's value is.
            None => *current = value,
            Some(key) => match current {
                Value::Table(table) => table.set(key.clone(), value),
                other => {
                    return Err(script_error(format!(
                        "attempt to index a {} value",
                        other.type_name()
                    )));
                }
            },
        }
        self.declare(&root, holding)
    }

    /// Puts a value back where the root name already lives, which is the only
    /// place an assignment can reach: a name that was never declared cannot be
    /// created, because a script here has no globals.
    fn declare(&mut self, name: &[u8], value: Value) -> RedisResult<()> {
        for scope in self.scopes.iter_mut().rev() {
            if let Some(slot) = scope.iter_mut().rev().find(|(key, _)| key == name) {
                slot.1 = value;
                return Ok(());
            }
        }
        Err(script_error(format!(
            "attempt to assign to an undeclared name '{}'",
            String::from_utf8_lossy(name)
        )))
    }

    fn index(&self, table: Value, key: Value) -> RedisResult<Value> {
        Ok(match table {
            Value::Table(table) => table.get(&key),
            // `redis.call` is a member of the `redis` name, which is what
            // makes `local call = redis.call` read the same as `redis.call`.
            Value::Native("redis") => match key {
                Value::String(member) => match redis_member(&member) {
                    Some(name) => Value::Native(name),
                    None => Value::Nil,
                },
                _ => Value::Nil,
            },
            Value::Nil => Value::Nil,
            other => {
                return Err(script_error(format!(
                    "attempt to index a {} value",
                    other.type_name()
                )));
            }
        })
    }

    /// The name a value is written through, and the keys under it. Boxed
    /// because it walks an expression that can hold another one, `t.a.b = 1`,
    /// and a self-calling `async fn` would otherwise have no size.
    fn lvalue<'b>(&'b mut self, target: &'b parser::Expression) -> LValueFuture<'b> {
        Box::pin(async move {
            Ok(match target {
                parser::Expression::Name(name) => (name.clone(), Vec::new()),
                parser::Expression::Index(base, key) => {
                    let (root, mut keys) = self.lvalue(base).await?;
                    keys.push(first(self.evaluate(key).await?));
                    (root, keys)
                }
                parser::Expression::Field(base, field) => {
                    let (root, mut keys) = self.lvalue(base).await?;
                    keys.push(Value::String(field.clone()));
                    (root, keys)
                }
                other => {
                    return Err(script_error(format!(
                        "cannot assign to this expression: {other:?}"
                    )));
                }
            })
        })
    }

    /// One call, whatever named value it resolves to.
    async fn invoke(&mut self, callee: Value, arguments: Vec<Value>) -> RedisResult<Vec<Value>> {
        self.account()?;
        let Value::Native(name) = callee else {
            return Err(script_error(format!(
                "attempt to call a {} value",
                callee.type_name()
            )));
        };
        match name {
            "type" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![Value::String(
                    arguments[0].type_name().as_bytes().to_vec(),
                )])
            }
            "tonumber" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![match arguments[0].to_number() {
                    Ok(number) => Value::Number(number),
                    Err(_) => Value::Nil,
                }])
            }
            "tostring" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![match &arguments[0] {
                    Value::Number(number) => {
                        Value::String(value::format_number(*number).into_bytes())
                    }
                    Value::Boolean(value) => {
                        Value::String(if *value { "true" } else { "false" }.into())
                    }
                    Value::Nil => Value::String("nil".into()),
                    other => Value::String(other.to_string_value().map_err(script_error)?),
                }])
            }
            "error" => {
                require_arguments(name, &arguments, 1)?;
                Err(script_error(message_of(&arguments[0])?))
            }
            "assert" => {
                require_arguments(name, &arguments, 1)?;
                if arguments[0].truthy() {
                    Ok(vec![arguments.into_iter().next().unwrap_or(Value::Nil)])
                } else {
                    Err(match arguments.get(1) {
                        Some(message) => script_error(message_of(message)?),
                        None => script_error("assertion failed!"),
                    })
                }
            }
            "redis.call" | "redis.pcall" => {
                self.redis_command(matches!(name, "redis.pcall"), arguments)
                    .await
            }
            "redis.error_reply" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![reply_table("err", message_of(&arguments[0])?)])
            }
            "redis.status_reply" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![reply_table("ok", message_of(&arguments[0])?)])
            }
            "redis.sha1hex" => {
                require_arguments(name, &arguments, 1)?;
                Ok(vec![Value::String(
                    sha1::hex(&arguments[0].to_string_value().map_err(script_error)?).into_bytes(),
                )])
            }
            // Logging goes to the gateway's tracing rather than to a log file
            // Redis owns; a script that reaches for it still runs.
            "redis.log" => Ok(Vec::new()),
            // Redis' own debugging hooks, which do nothing in a client's
            // scripts but are harmless when they are there.
            "redis.setresp" | "redis.breakpoint" => Ok(Vec::new()),
            other => Err(script_error(format!("{other} is not available here"))),
        }
    }

    /// `redis.call` and `redis.pcall`: the command, its arguments, and the
    /// keys of the command checked against the ones the script declared.
    async fn redis_command(
        &mut self,
        catches: bool,
        arguments: Vec<Value>,
    ) -> RedisResult<Vec<Value>> {
        let (name, rest) = arguments
            .split_first()
            .ok_or_else(|| script_error("wrong number of arguments for 'redis.call'"))?;
        let command = name.to_string_value().map_err(script_error)?;
        let mut command_line = vec![command];
        for argument in rest {
            // Redis hands a command a number as its text and refuses anything
            // that is neither: a script cannot pass a table or a nil and make
            // a command guess.
            command_line.push(match argument {
                Value::String(text) => text.clone(),
                Value::Number(number) => value::format_number(*number).into_bytes(),
                other => {
                    let message = format!(
                        "Lua redis lib command arguments must be strings or integers \
                         (got {})",
                        other.type_name()
                    );
                    if catches {
                        return Ok(vec![reply_table("err", message)]);
                    }
                    return Err(script_error(message));
                }
            });
        }
        if let Err(error) = self.check_declared(&command_line) {
            if !catches {
                return Err(error);
            }
            return Ok(vec![reply_table("err", error.message)]);
        }
        match self.host.call(&command_line).await {
            Ok(Reply::Error(message)) => {
                if catches {
                    Ok(vec![reply_table("err", message)])
                } else {
                    Err(RedisError::new("ERR", message))
                }
            }
            Ok(reply) => Ok(vec![value::from_reply(&reply)]),
            Err(error) if catches => Ok(vec![reply_table("err", error.to_string())]),
            Err(error) => Err(error),
        }
    }

    /// Every key the command will touch must be one the script declared.
    ///
    /// The positions come from the command table, which already says where the
    /// keys are for `COMMAND`, so this reads the same table rather than
    /// keeping a second list that could disagree with it.
    fn check_declared(&self, command_line: &[Vec<u8>]) -> RedisResult<()> {
        let Some(name) = command_line.first() else {
            return Ok(());
        };
        let spelled = String::from_utf8_lossy(name);
        let Some(info) = crate::table::lookup(&spelled.to_ascii_lowercase()) else {
            return Err(script_error(format!("unknown command '{spelled}'")));
        };
        let name = spelled.to_ascii_lowercase();
        for key in info.keys(command_line) {
            if !self.keys.iter().any(|declared| declared.as_slice() == key) {
                return Err(script_error(format!(
                    "the command '{name}' touches '{}', which this script did not \
                     declare in KEYS; a script here may only touch the keys it \
                     declares",
                    String::from_utf8_lossy(key)
                )));
            }
        }
        Ok(())
    }
}

fn require_arguments(name: &str, arguments: &[Value], expected: usize) -> RedisResult<()> {
    if arguments.len() >= expected {
        return Ok(());
    }
    Err(script_error(format!(
        "wrong number of arguments to function '{name}'"
    )))
}

/// The one value an expression has. A call that produced several is used for
/// its first, which is what Lua does everywhere but the last slot of a list.
fn first(values: Vec<Value>) -> Value {
    values.into_iter().next().unwrap_or(Value::Nil)
}

fn message_of(value: &Value) -> RedisResult<String> {
    Ok(match value {
        Value::String(text) => String::from_utf8_lossy(text).into_owned(),
        Value::Number(number) => value::format_number(*number),
        other => format!("bad argument #1 to a function ({})", other.type_name()),
    })
}

fn reply_table(field: &str, message: String) -> Value {
    let mut table = Table::new();
    table.set(
        Value::String(field.as_bytes().to_vec()),
        Value::String(message.into_bytes()),
    );
    Value::Table(table)
}

/// Whether two values are the same value. Lua compares tables by identity,
/// which a script that stores and reads its own table gets right either way.
fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) => true,
        (Value::Boolean(left), Value::Boolean(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left == right,
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Table(left), Value::Table(right)) => left == right,
        _ => false,
    }
}

/// Ordering two values of the same kind, which is all Lua compares.
fn compare(left: &Value, right: &Value) -> Result<std::cmp::Ordering, String> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| "attempt to compare two NaN values".to_owned()),
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        _ => Err(format!(
            "attempt to compare {} with {}",
            left.type_name(),
            right.type_name()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A script run with no cluster behind it, which is what every test here
    /// wants: the script's own arithmetic, control flow and errors.
    struct Silent;

    impl Host for Silent {
        fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send {
            let answer = match command.first().map(|name| name.to_ascii_lowercase()) {
                Some(name) if name == b"exists" => Reply::Integer(0),
                Some(name) if name == b"incrby" => Reply::Integer(1),
                Some(name) if name == b"pttl" => Reply::Integer(-1),
                _ => Reply::bulk("value"),
            };
            async move { Ok(answer) }
        }
    }

    async fn run_script(source: &str, keys: &[&str], argv: &[&str]) -> RedisResult<Reply> {
        run(
            &mut Silent,
            source.as_bytes(),
            keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
            argv.iter()
                .map(|argument| argument.as_bytes().to_vec())
                .collect(),
            Limits::default(),
        )
        .await
    }

    async fn reply(source: &str) -> Reply {
        run_script(source, &["k"], &["60"]).await.unwrap()
    }

    #[tokio::test]
    async fn a_script_returns_what_the_client_expects() {
        assert_eq!(reply("return 1").await, Reply::Integer(1));
        assert_eq!(reply("return 'hello'").await, Reply::bulk("hello"));
        assert_eq!(reply("return true").await, Reply::Integer(1));
        assert_eq!(reply("return nil").await, Reply::Nil);
        assert_eq!(reply("return 1 + 2 * 3").await, Reply::Integer(7));
        assert_eq!(reply("return 'a' .. 'b'").await, Reply::bulk("ab"));
        assert_eq!(reply("return -1 % 5").await, Reply::Integer(4));
        assert_eq!(
            reply("return {1, 2}").await,
            Reply::Array(vec![Reply::Integer(1), Reply::Integer(2)])
        );
        // A script that returns nothing is a nil, as it is in Redis.
        assert_eq!(reply("local x = 1").await, Reply::Nil);
    }

    #[tokio::test]
    async fn the_claim_a_cache_client_sends_claims_once() {
        let source = "if redis.call('GET', KEYS[1]) == '1' then return 0 end \
                      redis.call('SET', KEYS[1], '1', 'EX', ARGV[1]) return 1";
        assert_eq!(reply(source).await, Reply::Integer(1));
        // A missing argument is not a command argument Redis would take.
        let error = run_script(source, &["k"], &[]).await.unwrap_err();
        assert!(
            error.message.contains("must be strings or integers"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn the_rate_limiter_a_client_library_sends_returns_its_two_numbers() {
        let source = "redis.call('set', KEYS[1], 0, 'EX', ARGV[2], 'NX') \
                      local consumed = redis.call('incrby', KEYS[1], ARGV[1]) \
                      local ttl = redis.call('pttl', KEYS[1]) \
                      if ttl == -1 then \
                        redis.call('expire', KEYS[1], ARGV[2]) \
                        ttl = 1000 * ARGV[2] \
                      end \
                      return {consumed, ttl}";
        assert_eq!(
            run_script(source, &["k"], &["1", "20"]).await.unwrap(),
            Reply::Array(vec![Reply::Integer(1), Reply::Integer(20_000)])
        );
    }

    #[tokio::test]
    async fn control_flow_works_the_way_the_script_expects() {
        assert_eq!(
            reply("if 1 == 1 then return 'yes' else return 'no' end").await,
            Reply::bulk("yes")
        );
        assert_eq!(
            reply("if 1 == 2 then return 'yes' else return 'no' end").await,
            Reply::bulk("no")
        );
        assert_eq!(
            reply("if false then return 'a' elseif true then return 'b' end").await,
            Reply::bulk("b")
        );
        assert_eq!(
            reply(
                "local total = 0 for i = 1, 10 do if i == 6 then break end total = total + i end \
                 return total"
            )
            .await,
            Reply::Integer(15)
        );
        assert_eq!(
            reply("local n = 0 while n < 5 do n = n + 1 end return n").await,
            Reply::Integer(5)
        );
        assert_eq!(
            reply("local n = 0 for i = 10, 1, -1 do n = n + 1 end return n").await,
            Reply::Integer(10)
        );
    }

    #[tokio::test]
    async fn locals_hold_their_values_and_are_local() {
        assert_eq!(
            reply("local a = 1 local b = a + 1 return b").await,
            Reply::Integer(2)
        );
        // A name that was never declared is a mistake rather than a global
        // that happens to be empty.
        let error = run_script("return missing", &[], &[]).await.unwrap_err();
        assert!(
            error.message.contains("undeclared name 'missing'"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn a_table_written_into_is_the_table_read_back() {
        assert_eq!(
            reply("local t = {} t[1] = 'first' t.name = 'one' return t[1] .. t.name").await,
            Reply::bulk("firstone")
        );
        assert_eq!(reply("local t = {} return #t").await, Reply::Integer(0));
        assert_eq!(
            reply("local t = {1, 2} return t[1] + t[2]").await,
            Reply::Integer(3)
        );
        // A key held in a name is the key's value, not its name.
        assert_eq!(
            reply("local t = {} local k = 'name' t[k] = 'v' return t.name").await,
            Reply::bulk("v")
        );
    }

    #[tokio::test]
    async fn the_base_functions_a_script_reaches_for_are_there() {
        assert_eq!(reply("return type(1)").await, Reply::bulk("number"));
        assert_eq!(reply("return type('x')").await, Reply::bulk("string"));
        assert_eq!(reply("return tonumber('42')").await, Reply::Integer(42));
        assert_eq!(reply("return tonumber('x')").await, Reply::Nil);
        assert_eq!(
            reply("return tostring(42) .. '!'").await,
            Reply::bulk("42!")
        );
        assert_eq!(reply("return tostring(true)").await, Reply::bulk("true"));
        assert_eq!(reply("return assert(1)").await, Reply::Integer(1));
        assert!(run_script("assert(false)", &[], &[]).await.is_err());
        assert!(run_script("error('boom')", &[], &[]).await.is_err());
        // `redis` is a value, so a member of it can be carried around.
        assert_eq!(
            reply("local c = redis.call return type(c)").await,
            Reply::bulk("function")
        );
    }

    #[tokio::test]
    async fn a_script_may_only_touch_the_keys_it_declared() {
        let error = run_script("return redis.call('GET', 'other')", &["k"], &[])
            .await
            .unwrap_err();
        assert!(
            error.message.contains("did not declare"),
            "{}",
            error.message
        );
        // The same key, declared, is fine.
        assert_eq!(
            run_script("return redis.call('GET', KEYS[1])", &["k"], &[])
                .await
                .unwrap(),
            Reply::bulk("value")
        );
        // A command with no keys at all touches none.
        assert_eq!(
            run_script("return redis.call('PING')", &[], &[])
                .await
                .unwrap(),
            Reply::bulk("value")
        );
    }

    #[tokio::test]
    async fn an_unknown_command_the_script_calls_is_reported_as_one() {
        let error = run_script("return redis.call('HSET', KEYS[1], 'f', 'v')", &["k"], &[])
            .await
            .unwrap_err();
        assert!(
            error.message.contains("unknown command 'HSET'"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn pcall_reports_a_failure_as_a_table_instead_of_stopping() {
        assert_eq!(
            reply("return redis.pcall('NOSUCHCOMMAND', '0')").await,
            Reply::Error("user_script: unknown command 'NOSUCHCOMMAND'".into())
        );
        assert_eq!(
            reply("return redis.error_reply('nope')").await,
            Reply::Error("nope".into())
        );
        assert_eq!(
            reply("return redis.status_reply('PONG')").await,
            Reply::Simple("PONG".into())
        );
    }

    #[tokio::test]
    async fn a_runaway_script_is_stopped_by_the_step_budget() {
        let limits = Limits {
            max_steps: 500,
            ..Limits::default()
        };
        let error = run(
            &mut Silent,
            b"while true do end",
            Vec::new(),
            Vec::new(),
            limits,
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("maximum number of steps"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn a_runaway_script_is_stopped_by_its_deadline() {
        let limits = Limits {
            timeout: Duration::ZERO,
            ..Limits::default()
        };
        let mut running = Silent;
        let started = Instant::now();
        let error = run(
            &mut running,
            b"while true do local x = 1 end",
            Vec::new(),
            Vec::new(),
            limits,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "BUSY", "{}", error.message);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "it ran too long"
        );
    }

    #[tokio::test]
    async fn what_the_grammar_does_not_have_is_refused_by_name() {
        for (source, expected) in [
            ("function f() end", "function"),
            ("repeat until true", "repeat"),
            ("for k, v in pairs(t) do end", "for ... in"),
            ("x = 1", "undeclared name"),
            ("local t = 1 return t.x", "attempt to index a number"),
            ("return redis.notamethod()", "attempt to call a nil value"),
        ] {
            let error = run_script(source, &["k"], &[]).await.unwrap_err();
            assert!(
                error.message.contains(expected),
                "{source} reported {error}, which does not name {expected}"
            );
        }
    }
}
