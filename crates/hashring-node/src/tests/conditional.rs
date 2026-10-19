//! Writes the owner applies only if a precondition holds.
//!
//! The check runs under the write lock that applies the mutation, which is the
//! whole point: a caller that read a version and computes a new value from it
//! must not overwrite a write that landed in between.

use super::*;
use proto::{Precondition, precondition::Expect};

fn absent() -> Option<Precondition> {
    Some(Precondition {
        expect: Some(Expect::Absent(proto::Empty {})),
    })
}

fn at_version(version: &RecordVersion) -> Option<Precondition> {
    Some(Precondition {
        expect: Some(Expect::Version(version.clone())),
    })
}

fn put(
    key: &[u8],
    value: &[u8],
    request_id: &str,
    precondition: Option<Precondition>,
) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        topology_epoch: 1,
        request_id: request_id.into(),
        precondition,
        ..Default::default()
    }
}

async fn write(service: &DataNodeService, request: PutRequest) -> PutResponse {
    service
        .put(Request::new(request))
        .await
        .unwrap()
        .into_inner()
}

async fn delete(
    service: &DataNodeService,
    key: &[u8],
    request_id: &str,
    precondition: Option<Precondition>,
) -> DeleteResponse {
    service
        .delete(Request::new(DeleteRequest {
            key: key.to_vec(),
            topology_epoch: 1,
            request_id: request_id.into(),
            precondition,
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn read(service: &DataNodeService, key: &[u8]) -> GetResponse {
    service
        .get(Request::new(GetRequest {
            key: key.to_vec(),
            topology_epoch: 1,
            request_id: "read".into(),
        }))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn an_absent_condition_creates_a_key_once() {
    let service = service();
    let first = write(&service, put(b"key", b"one", "first", absent())).await;
    assert!(first.error.is_none() && !first.condition_failed);
    assert!(first.version.is_some());

    let second = write(&service, put(b"key", b"two", "second", absent())).await;
    assert!(second.condition_failed);
    assert!(second.error.is_none() && second.version.is_none());
    assert_eq!(read(&service, b"key").await.value, b"one");
    // A failed condition writes nothing, receipt included, so the same ID can
    // be sent again once the caller has recomputed.
    assert!(!service.state.read().await.dedup.contains_key("second"));
}

#[tokio::test]
async fn a_version_condition_rejects_a_write_based_on_a_stale_read() {
    let service = service();
    let created = write(&service, put(b"key", b"0", "create", None)).await;
    let read_version = created.version.unwrap();

    // Two writers read the same version; only the first one's write lands.
    let winner = write(
        &service,
        put(b"key", b"1", "winner", at_version(&read_version)),
    )
    .await;
    assert!(winner.version.is_some());
    let loser = write(
        &service,
        put(b"key", b"1", "loser", at_version(&read_version)),
    )
    .await;
    assert!(loser.condition_failed);
    assert_eq!(read(&service, b"key").await.value, b"1");
}

#[tokio::test]
async fn a_record_past_its_deadline_is_absent_to_a_condition() {
    let service = service();
    let expired = write(
        &service,
        PutRequest {
            expires_at_unix_millis: Some(now_unix_millis()),
            ..put(b"key", b"old", "expired", None)
        },
    )
    .await;
    let expired_version = expired.version.unwrap();

    let stale = write(
        &service,
        put(b"key", b"new", "stale", at_version(&expired_version)),
    )
    .await;
    assert!(
        stale.condition_failed,
        "a version that reads as absent matched"
    );
    let created = write(&service, put(b"key", b"new", "created", absent())).await;
    assert!(created.version.is_some());
}

#[tokio::test]
async fn a_retry_is_answered_from_its_receipt_before_the_condition() {
    let service = service();
    let first = write(&service, put(b"key", b"one", "first", absent())).await;
    // The key now exists, so the condition no longer holds, but this is the
    // same write retried and it already happened.
    let retried = write(&service, put(b"key", b"one", "first", absent())).await;
    assert!(!retried.condition_failed);
    assert_eq!(retried.version, first.version);

    let reused = write(&service, put(b"key", b"one", "first", None)).await;
    assert_eq!(
        reused.error.map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::MutationIdConflict)),
        "a different condition is a different write"
    );
}

#[tokio::test]
async fn a_conditional_delete_removes_only_the_version_it_read() {
    let service = service();
    let first = write(&service, put(b"key", b"one", "first", None)).await;
    let first_version = first.version.unwrap();
    write(&service, put(b"key", b"two", "second", None)).await;

    let stale = delete(&service, b"key", "stale", at_version(&first_version)).await;
    assert!(stale.condition_failed);
    assert_eq!(read(&service, b"key").await.value, b"two");

    let current = read(&service, b"key").await.version;
    let removed = delete(&service, b"key", "current", at_version(&current.unwrap())).await;
    assert!(!removed.condition_failed && removed.error.is_none());
    assert!(read(&service, b"key").await.error.is_some());

    let again = delete(&service, b"key", "absent", absent()).await;
    assert!(!again.condition_failed, "a deleted key is absent");
}

#[tokio::test]
async fn a_precondition_must_say_what_it_expects() {
    let service = service();
    let response = write(
        &service,
        put(
            b"key",
            b"value",
            "empty",
            Some(Precondition { expect: None }),
        ),
    )
    .await;
    assert_eq!(
        response.error.map(|error| ErrorCode::try_from(error.code)),
        Some(Ok(ErrorCode::InvalidArgument))
    );
}
