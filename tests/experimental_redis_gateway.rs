#![cfg(feature = "redis-lua-experimental")]
//! Lua-specific behavior through the shared gateway server and real cluster.

use std::time::Duration;

#[allow(dead_code)]
#[path = "support/redis_gateway.rs"]
mod gateway_support;
use gateway_support::*;

#[tokio::test]
async fn lua_commands_use_shared_authentication_and_reject_undeclared_write_targets() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start_with_gateway(
        &directory.path().join("coordinator.redb"),
        Some("secret"),
        "redis-lua-experimental-gateway",
    );
    let mut unauthenticated = cluster.connect(&cluster.url(None)).await;
    let error = redis::cmd("EVAL")
        .arg("return 1")
        .arg(0)
        .query_async::<i64>(&mut unauthenticated)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Authentication required"),
        "{error}"
    );
    let mut connection = cluster.client().await;
    let info: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("EVAL")
        .arg("SCRIPT")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(info.len(), 2);
    assert!(
        info.iter()
            .all(|value| matches!(value, redis::Value::Array(_)))
    );
    let _: () = redis::cmd("SET")
        .arg("declared")
        .arg("original")
        .query_async(&mut connection)
        .await
        .unwrap();
    for command in ["RENAME", "RENAMENX", "MSET"] {
        let script = if command == "MSET" {
            "return redis.call('MSET', KEYS[1], 'changed', 'undeclared', 'bad')".to_owned()
        } else {
            format!("return redis.call('{command}', KEYS[1], 'undeclared')")
        };
        let error = redis::cmd("EVAL")
            .arg(script)
            .arg(1)
            .arg("declared")
            .query_async::<redis::Value>(&mut connection)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("declare"), "{error}");
        let values: (Option<String>, Option<String>) = redis::cmd("MGET")
            .arg("declared")
            .arg("undeclared")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(values, (Some("original".into()), None));
    }
    // MSETNX uses the same private store as other commands, and later calls
    // see its staged state before anything is committed to the cluster.
    let value: String = redis::cmd("EVAL")
        .arg("redis.call('DEL', KEYS[1]); redis.call('MSETNX', KEYS[1], 'fresh'); return redis.call('GET', KEYS[1])")
        .arg(1).arg("declared").query_async(&mut connection).await.unwrap();
    assert_eq!(value, "fresh");
    // A command error preserves earlier successful commands, in one commit.
    let error = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], 'before-error'); return redis.call('INCR', KEYS[1])")
        .arg(1)
        .arg("declared")
        .query_async::<redis::Value>(&mut connection)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not an integer"), "{error}");
    let value: String = redis::cmd("GET")
        .arg("declared")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, "before-error");
}

#[tokio::test]
async fn scripts_publish_once_and_retry_whole_attempts_against_other_writers() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start_with_gateway(
        &directory.path().join("coordinator.redb"),
        None,
        "redis-lua-experimental-gateway",
    );
    let mut connection = cluster.client().await;
    let _: () = redis::cmd("SET")
        .arg("atomic")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    let (_other_process, url) = cluster.second_gateway();
    let mut other = cluster.connect(&url).await;
    let mut observer = cluster.connect(&url).await;
    let script_writer = tokio::spawn(async move {
        for _ in 0..40 {
            let _: i64 = redis::cmd("EVAL")
                .arg("local n = tonumber(redis.call('GET', KEYS[1])); redis.call('SET', KEYS[1], 99999999); local s = 0; for i = 1, 100000 do s = s + i end; redis.call('SET', KEYS[1], n + 1); return redis.call('INCR', KEYS[1])")
                .arg(1).arg("atomic").query_async(&mut connection).await.unwrap();
        }
    });
    let ordinary_writer = tokio::spawn(async move {
        for _ in 0..200 {
            let _: i64 = redis::cmd("INCR")
                .arg("atomic")
                .query_async(&mut other)
                .await
                .unwrap();
        }
    });
    while !script_writer.is_finished() || !ordinary_writer.is_finished() {
        let value: i64 = redis::cmd("GET")
            .arg("atomic")
            .query_async(&mut observer)
            .await
            .unwrap();
        assert!(
            value <= 280,
            "an intermediate script write became visible: {value}"
        );
    }
    script_writer.await.unwrap();
    ordinary_writer.await.unwrap();
    let value: i64 = redis::cmd("GET")
        .arg("atomic")
        .query_async(&mut observer)
        .await
        .unwrap();
    assert_eq!(value, 280, "a script did not act as one increment by two");
}

#[tokio::test]
async fn a_script_is_run_the_way_a_client_library_sends_it() {
    let _guard = test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::start_with_gateway(
        &directory.path().join("coordinator.redb"),
        None,
        "redis-lua-experimental-gateway",
    );
    let mut connection = cluster.client().await;

    // What a script returns, in the shapes a client library reads back.
    let answer: i64 = redis::cmd("EVAL")
        .arg("return 1 + 2 * 3")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(answer, 7);
    let list: Vec<i64> = redis::cmd("EVAL")
        .arg("return {1, 2}")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(list, vec![1, 2]);
    let nothing: Option<i64> = redis::cmd("EVAL")
        .arg("local x = 1")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(nothing, None);

    // KEYS and ARGV carry the bytes the command named, in order, and a script
    // sees its own writes the way Redis' single thread makes it see them.
    let echoed: String = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])")
        .arg(1)
        .arg("echoed:key")
        .arg("an argument")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(echoed, "an argument");

    // The claim a cache client sends. The read and the write are one
    // operation, so the second caller is told nobody claimed it.
    const CLAIM: &str = "if redis.call('GET', KEYS[1]) == '1' then return 0 end \
                         redis.call('SET', KEYS[1], '1', 'EX', ARGV[1]) return 1";
    let first: i64 = redis::cmd("EVAL")
        .arg(CLAIM)
        .arg(1)
        .arg("claimed:key")
        .arg(60)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(first, 1);
    let second: i64 = redis::cmd("EVAL")
        .arg(CLAIM)
        .arg(1)
        .arg("claimed:key")
        .arg(60)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(second, 0, "the claim was not exclusive to its caller");
    let stored: String = redis::cmd("GET")
        .arg("claimed:key")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(stored, "1");

    // Two gateways racing for one key still give one claim. Neither holds a
    // lock the other can see, so it is the cluster's conditional write, and
    // the script being run again when it finds the key changed, that makes
    // this one claim rather than two.
    let (_second_process, second_url) = cluster.second_gateway();
    let urls = [cluster.url(None), second_url];
    let mut racers = Vec::new();
    for _ in 0..4 {
        for url in &urls {
            let url = url.clone();
            racers.push(tokio::spawn(async move {
                let mut connection = redis::Client::open(url)
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    .unwrap();
                redis::cmd("EVAL")
                    .arg(CLAIM)
                    .arg(1)
                    .arg("raced:key")
                    .arg(60)
                    .query_async::<i64>(&mut connection)
                    .await
                    .unwrap()
            }));
        }
    }
    let mut claimed = 0;
    for racer in racers {
        claimed += tokio::time::timeout(Duration::from_secs(30), racer)
            .await
            .expect("a claim stalled")
            .unwrap();
    }
    assert_eq!(claimed, 1, "a claim was not exclusive");

    // The rate limiter's script, which is the other reason clients send one:
    // an increment and its deadline in one round trip.
    const LIMITER: &str = "redis.call('set', KEYS[1], 0, 'EX', ARGV[2], 'NX') \
                           local consumed = redis.call('incrby', KEYS[1], ARGV[1]) \
                           local ttl = redis.call('pttl', KEYS[1]) \
                           if ttl == -1 then \
                             redis.call('expire', KEYS[1], ARGV[2]) \
                             ttl = 1000 * ARGV[2] \
                           end \
                           return {consumed, ttl}";
    let consumed: Vec<i64> = redis::cmd("EVAL")
        .arg(LIMITER)
        .arg(1)
        .arg("limited:key")
        .arg(1)
        .arg(20)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(consumed.len(), 2, "{consumed:?}");
    assert_eq!(consumed[0], 1, "{consumed:?}");
    assert!(consumed[1] > 0, "{consumed:?}");

    // SCRIPT LOAD answers the hash a client sends next, and EVALSHA runs it.
    let hash: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(CLAIM)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        hash.len(),
        40,
        "a script's hash is forty hex characters: {hash}"
    );
    let loaded: i64 = redis::cmd("EVALSHA")
        .arg(&hash)
        .arg(1)
        .arg("sha:key")
        .arg(60)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(loaded, 1);
    let known: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&hash)
        .arg("0".repeat(40))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(known, vec![1, 0]);

    // NOSCRIPT is the code a client watches for to send the source instead,
    // so it has to arrive as an error and not as a wrong answer.
    let error = redis::cmd("EVALSHA")
        .arg("0".repeat(40))
        .arg(0)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("No matching script"),
        "unexpected error: {error}"
    );
    let _: String = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut connection)
        .await
        .unwrap();
    let error = redis::cmd("EVALSHA")
        .arg(&hash)
        .arg(0)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("No matching script"),
        "unexpected error: {error}"
    );

    // A script may only touch the keys it declared, which is the rule that
    // lets the gateway hold locks for the whole run.
    let error = redis::cmd("EVAL")
        .arg("return redis.call('GET', 'not:declared')")
        .arg(1)
        .arg("declared:key")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("declare"),
        "unexpected error: {error}"
    );
    // An argument that is neither a string nor a number is refused before the
    // command is built, as Redis refuses it.
    let error = redis::cmd("EVAL")
        .arg("return redis.call('GET', KEYS[2])")
        .arg(1)
        .arg("declared:key")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("must be strings or integers"),
        "unexpected error: {error}"
    );

    // A script that declares more than one key is refused: the gateway makes a
    // script atomic over one key, and a script that touches several keys may
    // reach nodes that own them separately, which no per-command precondition
    // can stitch into one run. This experiment intentionally supports one key.
    let error = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], 'a') redis.call('SET', KEYS[2], 'b') return 1")
        .arg(2)
        .arg("multi:one")
        .arg("multi:two")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("at most one key"),
        "unexpected error: {error}"
    );

    // A script may not change the connection, as in Redis.
    let error = redis::cmd("EVAL")
        .arg("return redis.call('SELECT', 1)")
        .arg(0)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not allowed from a script"),
        "unexpected error: {error}"
    );

    // A runaway script is stopped by its step budget rather than held.
    let error = redis::cmd("EVAL")
        .arg("while true do end")
        .arg(0)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("maximum number of steps"),
        "unexpected error: {error}"
    );

    // None of that left the gateway unable to answer anything else.
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");
    let still: i64 = redis::cmd("EVAL")
        .arg("return 40 + 2")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(still, 42);
}
