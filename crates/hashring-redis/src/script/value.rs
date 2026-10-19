//! The values a script manipulates, and the two directions they convert.
//!
//! Lua has one number type and strings are byte strings, which is a good fit
//! for a gateway whose values are byte strings too. The conversions are the
//! interesting part: a `redis.call` reply becomes a Lua value, and whatever a
//! script returns becomes a reply, using the same rules Redis uses so that a
//! script written against Redis reads the same here.

use std::collections::BTreeMap;

use crate::resp::Reply;

/// A Lua value.
///
/// Lua has no integer type, so `Number` is a double throughout, exactly as it
/// is in Redis' Lua. Numbers that came from an integer reply stay exact for
/// every count a Redis integer can hold.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nil,
    Boolean(bool),
    Number(f64),
    String(Vec<u8>),
    Table(Table),
    /// A name in the built-ins a script is given: `redis`, and the handful of
    /// base functions. A script cannot construct one, so it never appears in
    /// an argument to a command or in a reply; it exists so that `redis.call`
    /// can be an expression like any other, which is how Lua reads it.
    Native(&'static str),
}

impl Value {
    /// Whether the value counts as true. Only `nil` and `false` do not, which
    /// is Lua's rule and the reason a missing key reads as false.
    pub fn truthy(&self) -> bool {
        !matches!(self, Self::Nil | Self::Boolean(false))
    }

    /// The value as a number, following Lua's automatic string conversion: a
    /// string that spells a number is one, and anything else is an error.
    pub fn to_number(&self) -> Result<f64, String> {
        match self {
            Self::Number(number) => Ok(*number),
            Self::String(text) => parse_number(text).ok_or_else(|| {
                format!(
                    "attempt to perform arithmetic on a string value: {}",
                    String::from_utf8_lossy(text)
                )
            }),
            other => Err(format!(
                "attempt to perform arithmetic on a {} value",
                other.type_name()
            )),
        }
    }

    /// The value as a string, following the same conversion in the other
    /// direction: a number is spelled the way Lua spells it.
    pub fn to_string_value(&self) -> Result<Vec<u8>, String> {
        match self {
            Self::String(text) => Ok(text.clone()),
            Self::Number(number) => Ok(format_number(*number).into_bytes()),
            other => Err(format!(
                "attempt to concatenate a {} value",
                other.type_name()
            )),
        }
    }

    /// The name Lua would print in a message about this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Nil => "nil",
            Self::Boolean(_) => "boolean",
            Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::Table(_) => "table",
            Self::Native(_) => "function",
        }
    }

    /// The slot `key` names, when the value has one. An assignment through
    /// this reaches the value a name holds rather than a copy of it.
    pub fn get_mut(&mut self, key: &Value) -> Option<&mut Value> {
        match self {
            Self::Table(table) => table.get_mut(key),
            _ => None,
        }
    }
}

/// Lua's number to string: an integer without a point, a fraction with one, and
/// large magnitudes in the exponent form Lua uses.
pub fn format_number(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// A number as Lua parses one, which is the same grammar its `tonumber` and
/// its arithmetic use.
fn parse_number(text: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(text).ok()?.trim();
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).ok().map(|value| value as f64);
    }
    text.parse::<f64>().ok().filter(|value| value.is_finite())
}

/// A Lua table, split into the part `t[1]`, `t[2]`, … addresses and the part
/// named fields address.
///
/// Only the two parts exist here; there are no metatables, no weak references
/// and no mixed keys, because no script in a cache or a rate limiter needs one
/// and each would be more to get wrong than to leave out.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Table {
    array: Vec<Value>,
    named: BTreeMap<Vec<u8>, Value>,
}

impl Table {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn array(values: Vec<Value>) -> Self {
        Self {
            array: values,
            named: BTreeMap::new(),
        }
    }

    /// The value at `key`, where `key` is what an index expression evaluated
    /// to. An index past the end of the array part is `nil`, as it is in Lua.
    pub fn get(&self, key: &Value) -> Value {
        match key {
            Value::Number(number) => match array_index(*number) {
                Some(index) if index >= 1 && (index as usize) <= self.array.len() => {
                    self.array[index as usize - 1].clone()
                }
                // A numeric key that is not in the array part is still a legal
                // key in Lua, and a script that stores one expects to read it
                // back, so the named part holds it under its number's text.
                Some(index) => self
                    .named
                    .get(index.to_string().as_bytes())
                    .cloned()
                    .unwrap_or(Value::Nil),
                None => Value::Nil,
            },
            Value::String(name) => self.named.get(name).cloned().unwrap_or(Value::Nil),
            // Every other key type is a distinct identity in Lua, which is
            // more than this subset needs.
            _ => Value::Nil,
        }
    }

    /// The slot at `key` as the script may write it, so an assignment such as
    /// `t[1] = value` reaches the table the name refers to rather than a copy
    /// of it. A key that is not there yet is not a slot here; the caller grows
    /// the array part or inserts into the named part instead.
    pub fn get_mut(&mut self, key: &Value) -> Option<&mut Value> {
        match key {
            Value::Number(number) => {
                let index = array_index(*number)?;
                (index >= 1 && (index as usize) <= self.array.len())
                    .then(|| &mut self.array[index as usize - 1])
            }
            Value::String(name) => self.named.get_mut(name),
            _ => None,
        }
    }

    pub fn set(&mut self, key: Value, value: Value) {
        match key {
            Value::Number(number) => match array_index(number) {
                Some(index) if index >= 1 && (index as usize) <= self.array.len() => {
                    self.array[index as usize - 1] = value;
                }
                Some(index) if index >= 1 => {
                    self.grow(index as usize);
                    self.array[index as usize - 1] = value;
                }
                Some(_) => {}
                None => {
                    if let Value::String(name) = key {
                        self.named.insert(name, value);
                    }
                }
            },
            Value::String(name) => {
                self.named.insert(name, value);
            }
            _ => {}
        }
    }

    /// Fills the array part with nils up to `length`, so an assignment past
    /// its end does not silently become a named entry.
    fn grow(&mut self, length: usize) {
        if self.array.len() < length {
            self.array.resize(length, Value::Nil);
        }
    }

    /// The `#` operator: the border of the array part.
    pub fn len(&self) -> i64 {
        self.array.len() as i64
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty() && self.named.is_empty()
    }

    pub fn array_part(&self) -> &[Value] {
        &self.array
    }

    pub fn named_part(&self) -> &BTreeMap<Vec<u8>, Value> {
        &self.named
    }
}

/// A table index, which is a whole number from one upwards.
fn array_index(number: f64) -> Option<i64> {
    if number.fract() == 0.0 && number >= 1.0 && number <= i64::MAX as f64 {
        Some(number as i64)
    } else {
        None
    }
}

/// A reply the gateway produced, as the script sees it.
pub fn from_reply(reply: &Reply) -> Value {
    match reply {
        Reply::Integer(value) => Value::Number(*value as f64),
        Reply::Double(value) => Value::Number(*value),
        Reply::Bulk(Some(value)) => Value::String(value.clone()),
        Reply::Simple(value) => Value::String(value.clone().into_bytes()),
        // Redis hands a script `false` where a client would get a nil, because
        // that is what a Lua `if` can test.
        Reply::Bulk(None) | Reply::Nil => Value::Boolean(false),
        Reply::Boolean(value) => Value::Boolean(*value),
        Reply::Array(items) => Value::Table(Table::array(items.iter().map(from_reply).collect())),
        Reply::Set(items) => Value::Table(Table::array(
            items
                .iter()
                .map(|item| Value::String(item.clone()))
                .collect(),
        )),
        Reply::Map(entries) => {
            let mut table = Table::new();
            for (key, value) in entries {
                table.set(Value::String(key.clone()), from_reply(value));
            }
            Value::Table(table)
        }
        Reply::Error(_) => Value::Nil,
    }
}

/// What a script returned, as a reply the client sees.
///
/// The rules are Redis': a number without a fractional part becomes an integer
/// reply, `false` becomes a nil, and a table becomes an array unless it has
/// named fields, which makes it a map. Two shapes are not data at all: the
/// table `redis.error_reply` and `redis.status_reply` build, which Redis
/// turns back into an error and a status reply.
pub fn to_reply(value: &Value) -> Result<Reply, String> {
    Ok(match value {
        Value::Nil | Value::Boolean(false) => Reply::Nil,
        Value::Boolean(true) => Reply::Integer(1),
        Value::Number(number) => match whole(*number) {
            Some(value) => Reply::Integer(value),
            None => Reply::bulk(format_number(*number)),
        },
        Value::String(text) => Reply::bulk(text.clone()),
        Value::Table(table) if table.array_part().is_empty() => {
            match table.get(&Value::String(b"err".to_vec())) {
                Value::String(message) => Reply::Error(message_as_text(&message)),
                _ => match table.get(&Value::String(b"ok".to_vec())) {
                    Value::String(status) => Reply::Simple(status_as_text(&status)),
                    _ => Reply::Map(
                        table
                            .named_part()
                            .iter()
                            .map(|(key, value)| Ok((key.clone(), to_reply(value)?)))
                            .collect::<Result<Vec<_>, String>>()?,
                    ),
                },
            }
        }
        Value::Table(table) if table.named_part().is_empty() => Reply::Array(
            table
                .array_part()
                .iter()
                .map(to_reply)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Table(_) => {
            return Err("Lua table with both array and named fields is not a valid reply".into());
        }
        Value::Native(name) => {
            return Err(format!("{name} is a function and cannot be returned"));
        }
    })
}

/// A status reply is one word; Redis refuses a status that is not one.
fn message_as_text(text: &[u8]) -> String {
    String::from_utf8_lossy(text).into_owned()
}

fn status_as_text(text: &[u8]) -> String {
    let text = String::from_utf8_lossy(text).into_owned();
    if text.contains(['\r', '\n']) {
        "ERR status reply cannot contain newline".into()
    } else {
        text
    }
}

/// An integer reply for a whole number that fits, and `None` for anything else.
fn whole(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    if value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Some(value as i64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_nil_and_false_are_false() {
        assert!(!Value::Nil.truthy());
        assert!(!Value::Boolean(false).truthy());
        assert!(Value::Boolean(true).truthy());
        assert!(Value::Number(0.0).truthy());
        assert!(Value::String(Vec::new()).truthy());
    }

    #[test]
    fn a_string_that_spells_a_number_is_one() {
        assert_eq!(Value::String(b"20".to_vec()).to_number().unwrap(), 20.0);
        assert_eq!(Value::String(b" 2.5 ".to_vec()).to_number().unwrap(), 2.5);
        assert!(Value::String(b"twenty".to_vec()).to_number().is_err());
        assert!(Value::Nil.to_number().is_err());
    }

    #[test]
    fn a_number_keeps_its_point_or_not() {
        assert_eq!(format_number(20.0), "20");
        assert_eq!(format_number(2.5), "2.5");
        assert_eq!(format_number(1e20), "100000000000000000000");
    }

    #[test]
    fn an_array_part_indexes_from_one() {
        let mut table = Table::array(vec![Value::Number(1.0), Value::Number(2.0)]);
        assert_eq!(table.get(&Value::Number(1.0)), Value::Number(1.0));
        assert_eq!(table.get(&Value::Number(3.0)), Value::Nil);
        assert_eq!(table.len(), 2);
        table.set(Value::Number(4.0), Value::Number(4.0));
        assert_eq!(table.len(), 4);
        assert_eq!(table.get(&Value::Number(4.0)), Value::Number(4.0));
    }

    #[test]
    fn a_reply_becomes_the_value_a_script_expects() {
        assert_eq!(from_reply(&Reply::Integer(7)), Value::Number(7.0));
        assert_eq!(
            from_reply(&Reply::bulk("OK")),
            Value::String(b"OK".to_vec())
        );
        // A missing key is false, which is what `if not redis.call('get', k)` tests.
        assert_eq!(from_reply(&Reply::Nil), Value::Boolean(false));
        assert_eq!(
            from_reply(&Reply::Array(vec![Reply::Integer(1), Reply::Integer(2)])),
            Value::Table(Table::array(vec![Value::Number(1.0), Value::Number(2.0)]))
        );
    }

    #[test]
    fn a_value_becomes_the_reply_redis_would_send() {
        assert_eq!(to_reply(&Value::Nil).unwrap(), Reply::Nil);
        assert_eq!(to_reply(&Value::Boolean(false)).unwrap(), Reply::Nil);
        assert_eq!(to_reply(&Value::Boolean(true)).unwrap(), Reply::Integer(1));
        // A whole number is an integer reply, which is what a client parses as
        // a number rather than as text.
        assert_eq!(to_reply(&Value::Number(42.0)).unwrap(), Reply::Integer(42));
        assert_eq!(to_reply(&Value::Number(42.5)).unwrap(), Reply::bulk("42.5"));
        assert_eq!(
            to_reply(&Value::Table(Table::array(vec![Value::Number(1.0)]))).unwrap(),
            Reply::Array(vec![Reply::Integer(1)])
        );
    }

    #[test]
    fn a_table_with_named_fields_is_a_map_and_a_mixed_one_is_refused() {
        let mut named = Table::new();
        named.set(Value::String(b"left".to_vec()), Value::Number(3.0));
        assert_eq!(
            to_reply(&Value::Table(named)).unwrap(),
            Reply::Map(vec![(b"left".to_vec(), Reply::Integer(3))])
        );
        let mut mixed = Table::array(vec![Value::Number(1.0)]);
        mixed.set(Value::String(b"left".to_vec()), Value::Number(3.0));
        assert!(to_reply(&Value::Table(mixed)).is_err());
    }

    #[test]
    fn the_tables_redis_replies_with_come_back_as_replies() {
        let mut error = Table::new();
        error.set(
            Value::String(b"err".to_vec()),
            Value::String(b"boom".to_vec()),
        );
        assert_eq!(
            to_reply(&Value::Table(error)).unwrap(),
            Reply::Error("boom".into())
        );
        let mut status = Table::new();
        status.set(
            Value::String(b"ok".to_vec()),
            Value::String(b"PONG".to_vec()),
        );
        assert_eq!(
            to_reply(&Value::Table(status)).unwrap(),
            Reply::Simple("PONG".into())
        );
    }
}
