//! Running a client's Lua script against the cluster.
//!
//! The language is Lua itself, embedded as [`mlua`] with a vendored Lua 5.4,
//! rather than a subset of it: what a client library sends is ordinary Lua, so
//! a script gets the whole grammar and the `string`, `table`, `math`, `utf8`
//! and `coroutine` libraries. What it does not get is anything a cache has no
//! business doing. There is no `io`, `os`, `debug` or `package`, so a script
//! cannot reach the filesystem, the clock, the environment or the process, and
//! there is no `load`, `dofile`, `loadfile` or `require`, so it cannot pull in
//! code it was not sent. A script also may only touch the keys it declared in
//! `KEYS`, which is what lets the gateway hold those locks for the whole run
//! instead of one call at a time.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::Future;
use mlua::{HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table, Value, VmState};
use sha1::Digest;
use tokio::sync::Mutex;

use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

/// The SHA-1 that names a script, as the twenty bytes it is.
pub fn digest(source: &[u8]) -> [u8; 20] {
    sha1::Sha1::digest(source).into()
}

/// The same hash as the forty hex characters `SCRIPT LOAD` answers with, and
/// the name `EVALSHA` asks for.
pub fn hash_hex(source: &[u8]) -> String {
    hex::encode(digest(source))
}

/// What a script may ask the cluster to do: run one command, as a client would.
pub trait Host {
    fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send;
}

/// How long a script may run, and how much it may compute.
#[derive(Clone)]
pub struct Limits {
    /// Lua virtual-machine instructions, counted by a hook, so the bound holds
    /// on any machine rather than on how fast one is. This is the deterministic
    /// half of the budget: a script that computes rather than calls is stopped
    /// at exactly this many instructions.
    pub max_steps: usize,
    /// Wall-clock time, checked by the same hook. Redis' equivalent is
    /// `lua-time-limit`; the gateway has no background thread watching for a
    /// busy script, so a running script checks itself.
    pub timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            // A claim is two statements and a rate limiter's `INCR` with its
            // deadline is four more, each a handful of instructions, so this is
            // orders of magnitude above anything a client library sends.
            max_steps: 10_000_000,
            timeout: Duration::from_secs(5),
        }
    }
}

/// How many instructions pass between two checks of the budget. Small enough
/// that a stopped script stops promptly, large enough that the hook costs
/// nothing next to the work it watches.
const CHECK_INTERVAL: u64 = 1_000;

/// One Lua state, reused by every script this gateway runs.
///
/// Creating a state and loading its libraries is the expensive part, and a
/// gateway runs one script at a time, so there is nothing to contend. Each run
/// gets its own environment table and drops it afterwards, so a script cannot
/// see what an earlier one left behind. What a state does keep is the memory
/// its largest script needed, which is the same thing Redis keeps.
#[derive(Default)]
pub struct Runtime {
    state: Option<Lua>,
}

impl Runtime {
    /// Runs `source` once, against `keys` and `argv`, and answers with whatever
    /// the script returned, converted the way Redis converts it.
    pub async fn run<H: Host + Send + 'static>(
        &mut self,
        host: H,
        source: &[u8],
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        limits: Limits,
    ) -> RedisResult<Reply> {
        if self.state.is_none() {
            self.state = Some(new_state()?);
        }
        let lua = self.state.as_ref().expect("the state was just made");
        run(lua, host, source, keys, argv, limits).await
    }
}

/// A state with only the libraries a script may have: no `io`, `os`, `package`
/// or `debug`, and so no way to touch the machine it runs on. The base library
/// comes with every state, and its doors to more code are closed here, as they
/// are in Redis: a script is text that was already read, not a way to read more.
fn new_state() -> RedisResult<Lua> {
    let libraries =
        StdLib::STRING | StdLib::TABLE | StdLib::MATH | StdLib::UTF8 | StdLib::COROUTINE;
    let lua = Lua::new_with(libraries, LuaOptions::default())
        .map_err(|error| internal(format!("the script state could not be made: {error}")))?;
    let globals = lua.globals();
    for door in ["load", "loadfile", "dofile", "require"] {
        globals
            .set(door, mlua::Value::Nil)
            .map_err(|error| internal(format!("the script state could not be made: {error}")))?;
    }
    Ok(lua)
}

async fn run<H: Host + Send + 'static>(
    lua: &Lua,
    host: H,
    source: &[u8],
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
    limits: Limits,
) -> RedisResult<Reply> {
    let program = lua.load(source).set_name("user_script");
    let budget = Budget::new(limits);
    let watched = budget.clone();
    // The script runs on its own coroutine, and the hook is set on that
    // coroutine rather than the main thread: a hook on the main thread never
    // fires while a coroutine runs, which is what let a runaway script loop
    // forever under the old hand-written interpreter.
    let function = execute(lua, program, host, keys, argv)?;
    let thread = lua
        .create_thread(function)
        .map_err(|error| internal(format!("the script could not be watched: {error}")))?;
    thread
        .set_hook(
            HookTriggers::new()
                .every_nth_instruction(u32::try_from(CHECK_INTERVAL).unwrap_or(1_000)),
            move |_, _| watched.check(),
        )
        .map_err(|error| internal(format!("the script could not be watched: {error}")))?;
    let keep = thread.clone();
    let answer = match thread.into_async::<MultiValue>(()) {
        Ok(pending) => pending.await.map_err(script_failure),
        Err(error) => Err(script_failure(error)),
    };
    keep.remove_hook();
    match answer {
        Ok(values) => reply_of(values),
        Err(error) => Err(error),
    }
}

/// What a script is allowed to compute, and for how long.
#[derive(Clone)]
struct Budget {
    /// Instructions the run has left.
    left: Arc<AtomicU64>,
    timeout: Duration,
    until: Instant,
}

impl Budget {
    fn new(limits: Limits) -> Self {
        Self {
            left: Arc::new(AtomicU64::new(limits.max_steps as u64)),
            timeout: limits.timeout,
            until: Instant::now() + limits.timeout,
        }
    }

    /// Runs on every instruction the hook sees. Returning an error from a Lua
    /// hook is what aborts the script, with this message.
    fn check(&self) -> mlua::Result<VmState> {
        let spent = self.left.fetch_sub(CHECK_INTERVAL, Ordering::Relaxed);
        if spent <= CHECK_INTERVAL {
            return Err(mlua::Error::runtime(
                "this script exceeded its maximum number of steps",
            ));
        }
        if Instant::now() >= self.until {
            return Err(mlua::Error::runtime(format!(
                "this script was stopped after {}ms, which is longer than the deadline its \
                 command was given",
                self.timeout.as_millis()
            )));
        }
        Ok(VmState::Continue)
    }
}

/// Builds the environment one script runs in, and answers with the function
/// that runs there. The coroutine it runs on is made by the caller, which is
/// also what watches it.
fn execute<H: Host + Send + 'static>(
    lua: &Lua,
    program: mlua::chunk::Chunk<'_>,
    host: H,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
) -> Result<mlua::Function, RedisError> {
    // Every command this script may run goes through here, and the handle is
    // what lets Lua's own call be `async`: the virtual machine suspends at
    // `redis.call` while the cluster is asked.
    let host = Arc::new(Mutex::new(host));
    let declared = Arc::new(keys.clone());

    let globals = lua.globals();
    let environment = lua.create_table().map_err(setup_failure)?;
    // A script reads the standard library through the globals, and keeps
    // whatever it writes to itself, so nothing it defines outlives the run.
    let metatable = lua.create_table().map_err(setup_failure)?;
    metatable.set("__index", globals).map_err(setup_failure)?;
    environment
        .set_metatable(Some(metatable))
        .map_err(setup_failure)?;
    environment
        .set("KEYS", list(lua, &keys).map_err(setup_failure)?)
        .map_err(setup_failure)?;
    environment
        .set("ARGV", list(lua, &argv).map_err(setup_failure)?)
        .map_err(setup_failure)?;
    environment
        .set(
            "redis",
            commands(lua, &host, &declared).map_err(setup_failure)?,
        )
        .map_err(setup_failure)?;

    program
        .set_environment(environment)
        .into_function()
        .map_err(script_failure)
}

/// The `redis` table: what a script may ask of the cluster.
fn commands<H: Host + Send + 'static>(
    lua: &Lua,
    host: &Arc<Mutex<H>>,
    declared: &Arc<Vec<Vec<u8>>>,
) -> mlua::Result<Table> {
    let table = lua.create_table().map_err(lua_failure)?;

    let raising = host.clone();
    let keys = declared.clone();
    let call = lua
        .create_async_function(move |lua, arguments: MultiValue| {
            let host = raising.clone();
            let declared = keys.clone();
            async move {
                // A command that answers with an error is an error to the
                // script, the way `redis.call` treats it.
                match run_command(&host, &declared, arguments).await {
                    Err(error) => Err(mlua::Error::external(error)),
                    Ok(Reply::Error(message)) => {
                        Err(mlua::Error::runtime(script_error(message).message))
                    }
                    Ok(reply) => into_lua(&lua, reply),
                }
            }
        })
        .map_err(lua_failure)?;
    table.set("call", call).map_err(lua_failure)?;

    let catching = host.clone();
    let keys = declared.clone();
    let pcall = lua
        .create_async_function(move |lua, arguments: MultiValue| {
            let host = catching.clone();
            let declared = keys.clone();
            async move {
                let answer = match run_command(&host, &declared, arguments).await {
                    Ok(Reply::Error(message)) => Err(message),
                    Ok(reply) => into_lua(&lua, reply).map_err(|error| error.to_string()),
                    Err(error) => Err(error.message),
                };
                Ok::<MultiValue, mlua::Error>(match answer {
                    Ok(value) => MultiValue::from_iter([value]),
                    // `redis.pcall` answers with the error rather than raising
                    // it, which is what a script that wants to try twice needs.
                    Err(message) => MultiValue::from_iter([failed(&lua, message)?]),
                })
            }
        })
        .map_err(lua_failure)?;
    table.set("pcall", pcall).map_err(lua_failure)?;

    let error_reply = lua
        .create_function(|lua, message: String| failed(lua, message))
        .map_err(lua_failure)?;
    table.set("error_reply", error_reply).map_err(lua_failure)?;

    let status_reply = lua
        .create_function(|lua, message: String| {
            let table = lua.create_table()?;
            table.set("ok", message)?;
            Ok(Value::Table(table))
        })
        .map_err(lua_failure)?;
    table
        .set("status_reply", status_reply)
        .map_err(lua_failure)?;

    let sha1hex = lua
        .create_function(|_, argument: mlua::LuaString| Ok(hash_hex(argument.as_bytes().as_ref())))
        .map_err(lua_failure)?;
    table.set("sha1hex", sha1hex).map_err(lua_failure)?;

    Ok(table)
}

/// One command from a script, checked against the keys it declared and then run
/// against the cluster.
async fn run_command<H: Host + Send + 'static>(
    host: &Arc<Mutex<H>>,
    declared: &Arc<Vec<Vec<u8>>>,
    arguments: MultiValue,
) -> RedisResult<Reply> {
    let command = command_line(arguments).map_err(script_failure)?;
    check_declared(declared, &command)?;
    let mut host = host.lock().await;
    host.call(&command).await
}

/// Every key the command will touch must be one the script declared.
///
/// The positions come from the command table, which already says where the
/// keys are for `COMMAND`, so this reads the same table rather than keeping a
/// second list that could disagree with it.
fn check_declared(declared: &[Vec<u8>], command: &[Vec<u8>]) -> RedisResult<()> {
    let Some(name) = command.first() else {
        return Ok(());
    };
    let spelled = String::from_utf8_lossy(name);
    let Some(info) = crate::table::lookup(&spelled.to_ascii_lowercase()) else {
        return Err(script_error(format!("unknown command '{spelled}'")));
    };
    let name = spelled.to_ascii_lowercase();
    for key in info.keys(command) {
        if !declared.iter().any(|one| one.as_slice() == key) {
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

/// The arguments a script passed, as a command line. A table is expanded into
/// the several arguments it holds, as Redis expands it.
fn command_line(arguments: MultiValue) -> mlua::Result<Vec<Vec<u8>>> {
    let mut line = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match argument {
            Value::String(text) => line.push(text.as_bytes().as_ref().to_vec()),
            Value::Integer(number) => line.push(number.to_string().into_bytes()),
            Value::Number(number) => line.push(number_text(number).into_bytes()),
            Value::Table(table) => {
                for index in 1..=table.raw_len() {
                    line.push(argument_text(table.raw_get::<Value>(index)?)?);
                }
            }
            other => {
                return Err(mlua::Error::runtime(format!(
                    "Lua redis lib command arguments must be strings or integers (got {})",
                    other.type_name()
                )));
            }
        }
    }
    Ok(line)
}

fn argument_text(value: Value) -> mlua::Result<Vec<u8>> {
    match value {
        Value::String(text) => Ok(text.as_bytes().as_ref().to_vec()),
        Value::Integer(number) => Ok(number.to_string().into_bytes()),
        Value::Number(number) => Ok(number_text(number).into_bytes()),
        Value::Table(table) => {
            let mut joined = Vec::new();
            for index in 1..=table.raw_len() {
                joined.push(argument_text(table.raw_get::<Value>(index)?)?);
            }
            Ok(joined.join(&[b' '][..]))
        }
        other => Err(mlua::Error::runtime(format!(
            "Lua redis lib command arguments must be strings or integers (got {})",
            other.type_name()
        ))),
    }
}

/// A Lua number as its shortest exact text, which is what a client means when
/// it passes one.
fn number_text(number: f64) -> String {
    if number.is_finite() && number.fract() == 0.0 && number.abs() < 9.007_199_254_740_992e15 {
        format!("{}", number as i64)
    } else {
        format!("{number}")
    }
}

/// `KEYS` and `ARGV` as Lua sees them: byte strings, indexed from one.
fn list(lua: &Lua, values: &[Vec<u8>]) -> mlua::Result<Table> {
    let table = lua.create_table().map_err(lua_failure)?;
    for (index, value) in values.iter().enumerate() {
        table
            .set(
                index + 1,
                Value::String(lua.create_string(value).map_err(lua_failure)?),
            )
            .map_err(lua_failure)?;
    }
    Ok(table)
}

/// The `err` table `redis.pcall` and `redis.error_reply` answer with.
fn failed(lua: &Lua, message: String) -> mlua::Result<Value> {
    let table = lua.create_table()?;
    table.set("err", message)?;
    Ok(Value::Table(table))
}

/// What the cluster answered, as Lua sees it. A missing value is `false`,
/// which is what a script tests for.
fn into_lua(lua: &Lua, reply: Reply) -> mlua::Result<Value> {
    Ok(match reply {
        Reply::Nil | Reply::Bulk(None) => Value::Boolean(false),
        Reply::Bulk(Some(bytes)) => Value::String(lua.create_string(bytes).map_err(lua_failure)?),
        Reply::Integer(number) => Value::Integer(number),
        Reply::Double(number) => Value::Number(number),
        Reply::Boolean(yes) => Value::Boolean(yes),
        Reply::Array(items) => {
            let table = lua.create_table().map_err(lua_failure)?;
            for (index, item) in items.into_iter().enumerate() {
                let item = into_lua(lua, item)?;
                table.set(index + 1, item).map_err(lua_failure)?;
            }
            Value::Table(table)
        }
        Reply::Set(items) => {
            let table = lua.create_table().map_err(lua_failure)?;
            for (index, item) in items.into_iter().enumerate() {
                table
                    .set(
                        index + 1,
                        Value::String(lua.create_string(item).map_err(lua_failure)?),
                    )
                    .map_err(lua_failure)?;
            }
            Value::Table(table)
        }
        Reply::Map(entries) => {
            let table = lua.create_table().map_err(lua_failure)?;
            for (key, value) in entries {
                let value = into_lua(lua, value)?;
                table
                    .set(
                        Value::String(lua.create_string(key).map_err(lua_failure)?),
                        value,
                    )
                    .map_err(lua_failure)?;
            }
            Value::Table(table)
        }
        Reply::Simple(status) => {
            let table = lua.create_table().map_err(lua_failure)?;
            table.set("ok", status).map_err(lua_failure)?;
            Value::Table(table)
        }
        Reply::Error(message) => {
            return Err(mlua::Error::runtime(script_error(message).message));
        }
    })
}

/// What a script returned, converted the way Redis converts it: one value is
/// itself, several are an array, and a table is the array it holds.
fn reply_of(values: MultiValue) -> RedisResult<Reply> {
    match values.len() {
        0 => Ok(Reply::Nil),
        1 => value_of(values.into_iter().next().expect("one value was counted")),
        _ => values
            .into_iter()
            .map(value_of)
            .collect::<RedisResult<Vec<_>>>()
            .map(Reply::Array),
    }
}

fn value_of(value: Value) -> RedisResult<Reply> {
    match value {
        Value::Nil => Ok(Reply::Nil),
        Value::Boolean(yes) => Ok(Reply::Integer(i64::from(yes))),
        Value::Integer(number) => Ok(Reply::Integer(number)),
        Value::Number(number) => Ok(number_reply(number)),
        Value::String(text) => Ok(Reply::Bulk(Some(text.as_bytes().as_ref().to_vec()))),
        Value::Table(table) => table_reply(&table),
        other => Err(script_error(format!(
            "a script may only return nil, a boolean, a number, a string or a table (got {})",
            other.type_name()
        ))),
    }
}

/// A whole number answers as an integer, as it does in Redis; anything else
/// answers as its text.
fn number_reply(number: f64) -> Reply {
    if number.is_finite() && number.fract() == 0.0 && number.abs() < 9.007_199_254_740_992e15 {
        Reply::Integer(number as i64)
    } else {
        Reply::Bulk(Some(number_text(number).into_bytes()))
    }
}

/// A table answers as the array it holds. A table that carries an `err` or an
/// `ok` answers as that instead, which is what `redis.error_reply`,
/// `redis.status_reply` and `redis.pcall` hand back. Both fields are read by
/// name, as Redis reads them, and neither needs an array part: a table built
/// out of `err` alone has no numbered entries at all, so counting them would
/// miss the one field that matters.
fn table_reply(table: &Table) -> RedisResult<Reply> {
    if let Ok(Value::String(message)) = table.raw_get::<Value>("err") {
        return Err(script_error(text(message)));
    }
    if let Ok(Value::String(status)) = table.raw_get::<Value>("ok") {
        return Ok(Reply::Simple(text(status)));
    }
    let mut items = Vec::with_capacity(table.raw_len());
    for index in 1..=table.raw_len() {
        match table.raw_get::<Value>(index) {
            Ok(value) if !value.is_nil() => items.push(value_of(value)?),
            // A hole ends the array, as it does everywhere in Redis.
            _ => break,
        }
    }
    Ok(Reply::Array(items))
}

/// A Lua string as text. A script's own text is text; a value it built out of
/// bytes that are not is reported as it reads rather than refused.
fn text(value: mlua::LuaString) -> String {
    String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned()
}

/// A failure inside the script, as Redis words it. The chunk is named
/// `user_script`, so Lua's own errors already start that way and are passed
/// through rather than named twice.
fn script_failure(error: mlua::Error) -> RedisError {
    // A failure the cluster reported crosses into Lua as itself and comes back
    // out unchanged, so a conflict still reads as one: the caller runs the
    // script again rather than handing the client a mangled error. Only a
    // failure that began in Lua is given the script's own wording.
    if let Some(inner) = error.downcast_ref::<RedisError>() {
        return inner.clone();
    }
    match error {
        mlua::Error::RuntimeError(message) if message.contains("user_script") => {
            script_error(message)
        }
        other => script_error(other.to_string()),
    }
}

/// A failure while building a script's world, as the error Lua carries.
fn lua_failure(error: mlua::Error) -> mlua::Error {
    mlua::Error::external(script_error(error.to_string()))
}

/// The same failure, as the error the gateway answers a client with.
fn setup_failure(error: mlua::Error) -> RedisError {
    script_error(error.to_string())
}

/// A script's failure, as the client is told it. Lua words an error with the
/// script's own message first and a traceback after it, and only the first is
/// an answer: the traceback describes the interpreter to whoever runs the
/// gateway, not the failure to the client, and Redis leaves it out too. What
/// remains is one line, since RESP ends an error at a line break, so a message
/// carrying one of its own would otherwise end early and leave the client
/// reading the rest as the next reply.
fn script_error(message: impl Into<String>) -> RedisError {
    let message = message.into();
    let message = match message.find("\nstack traceback") {
        Some(end) => &message[..end],
        None => &message[..],
    };
    RedisError::new("ERR", format!("user_script: {}", one_line(message)))
}

/// Text as one line: every line break is a space.
fn one_line(message: &str) -> String {
    let mut line = String::with_capacity(message.len());
    let mut broke = false;
    for character in message.chars() {
        match character {
            '\r' | '\n' => broke = true,
            ' ' if broke && line.ends_with(' ') => {}
            _ => {
                if broke {
                    line.push(' ');
                    broke = false;
                }
                line.push(character);
            }
        }
    }
    line.trim_end().to_owned()
}

fn internal(message: impl Into<String>) -> RedisError {
    RedisError::new("ERR", message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    /// A cluster that answers from a script, and remembers what it was asked.
    /// A key the script is told changed underneath it answers with the conflict
    /// the gateway would report, so a test can watch it cross the interpreter.
    #[derive(Default)]
    struct Canned {
        answers: HashMap<String, Reply>,
        conflicts: HashSet<Vec<u8>>,
        asked: Vec<String>,
    }

    /// A host the test keeps a handle on, since a script takes its own.
    type Shared = Arc<Mutex<Canned>>;

    impl Host for Shared {
        fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send {
            // The lock is only held for the bookkeeping, never across the call,
            // and the test's own cluster answers without waiting.
            let host = self.clone();
            async move {
                let mut host = host.lock().await;
                host.call(command).await
            }
        }
    }

    impl Host for Canned {
        fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send {
            let name = String::from_utf8_lossy(&command[0]).to_ascii_lowercase();
            self.asked.push(
                command
                    .iter()
                    .map(|argument| String::from_utf8_lossy(argument).into_owned())
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            // A write to a key that changed underneath the script is the
            // conflict the gateway turns into a re-run; it must reach the
            // caller as that conflict and not as a reworded script error.
            let key = command
                .get(1)
                .filter(|key| self.conflicts.contains(key.as_slice()));
            let answer = self.answers.get(&name).cloned().unwrap_or(Reply::Nil);
            async move {
                match key {
                    Some(key) => Err(RedisError::conflict(key)),
                    None => Ok(answer),
                }
            }
        }
    }

    fn keys() -> Vec<Vec<u8>> {
        vec![b"alpha".to_vec(), b"beta".to_vec()]
    }

    async fn evaluate(
        host: Canned,
        source: &str,
        keys: Vec<Vec<u8>>,
        limits: Limits,
    ) -> RedisResult<Reply> {
        Runtime::default()
            .run(
                host,
                source.as_bytes(),
                keys,
                vec![b"one".to_vec(), b"2".to_vec()],
                limits,
            )
            .await
    }

    #[tokio::test]
    async fn ordinary_lua_is_lua() {
        let limits = Limits::default();
        // Arithmetic, both integer kinds, and the libraries a client reaches for.
        assert_eq!(
            evaluate(
                Canned::default(),
                "return 1 + 2 * 3",
                keys(),
                limits.clone()
            )
            .await
            .unwrap(),
            Reply::Integer(7)
        );
        assert_eq!(
            evaluate(
                Canned::default(),
                "return math.floor(7 / 2), 7 % 4, #('ab')",
                keys(),
                limits.clone()
            )
            .await
            .unwrap(),
            Reply::Array(vec![
                Reply::Integer(3),
                Reply::Integer(3),
                Reply::Integer(2)
            ])
        );
        // Closures, varargs, metatables and generic `for` are all just Lua.
        assert_eq!(
            evaluate(
                Canned::default(),
                "local add = function(...) local n = 0 for _, v in ipairs({...}) do n = n + v end \
                 return n end return add(1, 2, 3)",
                keys(),
                limits.clone()
            )
            .await
            .unwrap(),
            Reply::Integer(6)
        );
        // Several values answer as an array, as they do in Redis.
        assert_eq!(
            evaluate(Canned::default(), "return 'a', 'b'", keys(), limits.clone())
                .await
                .unwrap(),
            Reply::Array(vec![Reply::bulk("a"), Reply::bulk("b")])
        );
        // A whole number answers as an integer and a fraction as its text.
        assert_eq!(
            evaluate(Canned::default(), "return 2.0", keys(), limits.clone())
                .await
                .unwrap(),
            Reply::Integer(2)
        );
        assert_eq!(
            evaluate(Canned::default(), "return 1.5", keys(), limits.clone())
                .await
                .unwrap(),
            Reply::Bulk(Some(b"1.5".to_vec()))
        );
        // Nothing at all is a missing value.
        assert_eq!(
            evaluate(Canned::default(), "local x = 1", keys(), limits.clone())
                .await
                .unwrap(),
            Reply::Nil
        );
    }

    #[tokio::test]
    async fn strings_are_byte_strings() {
        let answer = evaluate(
            Canned::default(),
            "return KEYS[1] .. ':' .. ARGV[2]",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(answer, Reply::bulk("alpha:2"));
        // Bytes that are not text survive, since a value here is a byte string.
        let answer = evaluate(
            Canned::default(),
            "return string.char(0, 255, 10)",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(answer, Reply::Bulk(Some(vec![0, 255, 10])));
    }

    #[tokio::test]
    async fn a_script_may_not_reach_the_machine() {
        for source in [
            "return os.time()",
            "return io.open('/etc/passwd')",
            "return require('os')",
            "return load('return 1')()",
            "return dofile('/etc/passwd')",
            "return package.path",
        ] {
            let error = evaluate(Canned::default(), source, keys(), Limits::default())
                .await
                .unwrap_err();
            assert!(
                error.message.contains("user_script"),
                "{source} answered {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_runaway_script_is_stopped_by_its_budget() {
        let error = evaluate(
            Canned::default(),
            "while true do end",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("maximum number of steps"),
            "{error:?}"
        );
        // A small budget stops a script that a large one would let finish.
        let error = evaluate(
            Canned::default(),
            "local n = 0 for i = 1, 100000 do n = n + i end return n",
            keys(),
            Limits {
                max_steps: 10_000,
                ..Limits::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("maximum number of steps"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_script_only_touches_the_keys_it_declared() {
        let mut host = Canned::default();
        host.answers.insert("get".into(), Reply::bulk("value"));
        let answer = evaluate(
            host,
            "return redis.call('GET', KEYS[1])",
            vec![b"alpha".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(answer, Reply::bulk("value"));

        let error = evaluate(
            Canned::default(),
            "return redis.call('GET', 'not:declared')",
            vec![b"alpha".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("declare"), "{error:?}");

        // `pcall` answers with the error instead of raising it.
        let answer = evaluate(
            Canned::default(),
            "local r = redis.pcall('GET', 'not:declared') if r.err then return 'refused' end \
             return 'ran'",
            vec![b"alpha".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(answer, Reply::bulk("refused"));
    }

    #[tokio::test]
    async fn what_a_command_answered_is_what_a_script_sees() {
        let mut host = Canned::default();
        host.answers.insert("exists".into(), Reply::Integer(1));
        host.answers.insert(
            "mget".into(),
            Reply::Array(vec![Reply::bulk("one"), Reply::Nil]),
        );
        host.answers.insert("set".into(), Reply::status());
        host.answers.insert(
            "del".into(),
            Reply::Error("wrong number of arguments".into()),
        );
        host.answers.insert("incr".into(), Reply::Integer(41));
        let host = Canned {
            asked: Vec::new(),
            ..host
        };
        let answer = evaluate(
            host,
            "local found = redis.call('EXISTS', KEYS[1]) \
             local both = redis.call('MGET', KEYS[1], KEYS[2]) \
             local written = redis.call('SET', KEYS[1], 'v') \
             return {found, both[1], both[2], written.ok, redis.call('INCR', KEYS[1])}",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            answer,
            Reply::Array(vec![
                Reply::Integer(1),
                Reply::bulk("one"),
                // A missing value is `false` to a script, and `false` comes back
                // as a zero rather than as a missing reply.
                Reply::Integer(0),
                Reply::bulk("OK"),
                Reply::Integer(41),
            ])
        );

        // An error reply raises for `call` and answers for `pcall`.
        let host = Canned {
            answers: HashMap::from([(
                "del".to_owned(),
                Reply::Error("wrong number of arguments".into()),
            )]),
            ..Canned::default()
        };
        let error = evaluate(
            host,
            "return redis.call('DEL', KEYS[1])",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("wrong number of arguments"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_command_reaches_the_cluster_as_the_client_spelled_it() {
        let host = Arc::new(Mutex::new(Canned {
            asked: Vec::new(),
            ..Canned::default()
        }));
        let mut run = Runtime::default();
        run.run(
            host.clone(),
            b"redis.call('SET', KEYS[1], ARGV[1]) return redis.call('MGET', {KEYS[1], KEYS[2]})",
            keys(),
            vec![b"one".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap();
        let asked = host.lock().await.asked.clone();
        assert_eq!(
            asked,
            vec!["SET alpha one".to_owned(), "MGET alpha beta".to_owned(),]
        );
    }

    /// A write that found the key changed underneath the script must reach the
    /// caller as that conflict, so the gateway runs the script again instead of
    /// handing the client a reworded error. Redis's single thread gets this by
    /// never letting anyone in; here it is the conditional write's own answer.
    #[tokio::test]
    async fn a_conflict_crosses_the_interpreter_as_itself() {
        let mut host = Canned::default();
        host.conflicts.insert(b"alpha".to_vec());
        let error = evaluate(
            host,
            "redis.call('SET', KEYS[1], 'x') return 1",
            vec![b"alpha".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(error.is_conflict(), "{error:?}");
        // The conflict is not dressed up as a script failure, so its wording
        // still names the key that moved.
        assert!(error.message.contains("alpha"), "{error:?}");
    }

    /// `redis.error_reply` and `redis.status_reply` answer with a table that
    /// carries one named field and no numbered entry at all, and that table is
    /// still an error or a status when it comes back out. Reading the named
    /// field is what makes it one; counting the array would find nothing and
    /// answer with an empty list instead.
    #[tokio::test]
    async fn a_named_reply_comes_back_as_what_it_named() {
        let error = evaluate(
            Canned::default(),
            "return redis.error_reply('WRONGTYPE fake')",
            Vec::new(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("user_script: WRONGTYPE fake"),
            "{error:?}"
        );

        let status = evaluate(
            Canned::default(),
            "return redis.status_reply('QUEUED')",
            Vec::new(),
            Limits::default(),
        )
        .await
        .unwrap();
        assert_eq!(status, Reply::Simple("QUEUED".into()));

        // So does the same table when it reaches a client through `pcall`.
        let mut host = Canned::default();
        host.answers.insert(
            "get".into(),
            Reply::Error("WRONGTYPE Operation against a key".into()),
        );
        let raised = evaluate(
            host,
            "return redis.pcall('GET', KEYS[1])",
            vec![b"alpha".to_vec()],
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            raised.message.contains("WRONGTYPE Operation against a key"),
            "{raised:?}"
        );
    }

    /// A script's failure reaches a client as one line. Lua words an error with
    /// a traceback after it, and RESP ends a simple error at the line break, so
    /// sending the traceback whole would leave the client reading the rest of it
    /// as the next reply. The first line is the error itself.
    #[tokio::test]
    async fn a_failure_reaches_a_client_as_one_line() {
        let error = evaluate(
            Canned::default(),
            "return os.time()",
            Vec::new(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            !error.message.contains(['\r', '\n']),
            "the error was more than one line: {error:?}"
        );
        assert!(error.message.contains("'os'"), "{error:?}");

        // A script that raises a line break of its own is one line too.
        let error = evaluate(
            Canned::default(),
            "error('first\\nsecond')",
            Vec::new(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(
            !error.message.contains(['\r', '\n']),
            "the error was more than one line: {error:?}"
        );
        assert!(error.message.contains("first"), "{error:?}");
    }

    #[tokio::test]
    async fn a_runtime_failure_is_a_script_error() {
        let error = evaluate(
            Canned::default(),
            "local t = nil return t.field",
            keys(),
            Limits::default(),
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("user_script"), "{error:?}");
        let error = evaluate(Canned::default(), "return (", keys(), Limits::default())
            .await
            .unwrap_err();
        assert!(error.message.contains("user_script"), "{error:?}");
    }

    #[tokio::test]
    async fn one_state_serves_many_scripts_and_keeps_nothing() {
        let mut runtime = Runtime::default();
        runtime
            .run(
                Canned::default(),
                b"leaked = 42",
                keys(),
                Vec::new(),
                Limits::default(),
            )
            .await
            .unwrap();
        let answer = runtime
            .run(
                Canned::default(),
                b"return tostring(leaked)",
                keys(),
                Vec::new(),
                Limits::default(),
            )
            .await
            .unwrap();
        assert_eq!(answer, Reply::bulk("nil"));
    }

    #[test]
    fn a_script_is_named_by_the_hash_of_its_text() {
        assert_eq!(hash_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }
}
