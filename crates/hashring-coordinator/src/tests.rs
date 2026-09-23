use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::policy::policy_readiness_verified;
use super::*;
use hashring_core::topology::Member;
use tokio::sync::Barrier;

struct ActiveWork(Arc<AtomicUsize>);

impl Drop for ActiveWork {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct MemoryRepository(Mutex<Option<ClusterState>>);

impl CoordinatorRepository for MemoryRepository {
    fn load_state(&self) -> Result<Option<ClusterState>, RepositoryError> {
        Ok(self.0.lock().unwrap().clone())
    }

    fn store_state(&self, state: &ClusterState) -> Result<(), RepositoryError> {
        *self.0.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}

fn topology() -> TopologySnapshot {
    TopologySnapshot::new(
        1,
        7,
        8,
        vec![Member {
            node_id: "n1".into(),
            endpoint: "http://127.0.0.1:5001".into(),
        }],
    )
    .unwrap()
}

#[test]
fn failover_requires_admitted_coverage_for_every_old_interval() {
    let members: Vec<_> = (1..=3)
        .map(|index| Member {
            node_id: format!("n{index}"),
            endpoint: format!("http://127.0.0.1:500{index}"),
        })
        .collect();
    let old = TopologySnapshot::new(1, 7, 4, members.clone()).unwrap();
    let target = TopologyChange::plan(
        &old,
        members
            .into_iter()
            .filter(|member| member.node_id != "n1")
            .collect(),
    )
    .unwrap()
    .target_topology;
    let repo = MemoryRepository::default();
    let mut state = load_or_initialize(&repo, Some(old.clone())).unwrap();
    for node_id in ["n1", "n2", "n3"] {
        state
            .process_instances
            .insert(node_id.into(), format!("{node_id}-process"));
    }
    for range in old.derived_ranges().unwrap() {
        let Some(follower) = range.follower_node_ids.first() else {
            continue;
        };
        state.replica_admissions.push(ReplicaAdmission {
            epoch: old.epoch,
            start_exclusive: range.start_exclusive,
            end_inclusive: range.end_inclusive,
            owner_node_id: range.owner_node_id,
            node_id: follower.clone(),
            process_instance_id: format!("{follower}-process"),
            verified_watermark: 0,
            stream_cursor: 0,
            digest: "verified".into(),
        });
    }
    let now = Instant::now();
    let grants = ["n2", "n3"]
        .into_iter()
        .map(|node_id| {
            (
                node_id.into(),
                NodeLeaseGrant {
                    process_instance_id: format!("{node_id}-process"),
                    epoch: old.epoch,
                    expires_at: now + NODE_LEASE_DURATION,
                },
            )
        })
        .collect();
    assert!(prove_failure_coverage(&state, &target, &grants, now).is_ok());
    let missing = state
        .replica_admissions
        .iter()
        .position(|admission| admission.owner_node_id == "n1")
        .expect("failed owner has at least one range");
    state.replica_admissions.remove(missing);
    assert!(prove_failure_coverage(&state, &target, &grants, now).is_err());
}

#[test]
fn a_single_failed_peer_link_never_confirms_node_failure() {
    let members: Vec<_> = (1..=3)
        .map(|index| Member {
            node_id: format!("n{index}"),
            endpoint: format!("http://127.0.0.1:500{index}"),
        })
        .collect();
    let topology = TopologySnapshot::new(1, 7, 1, members).unwrap();
    let repo = MemoryRepository::default();
    let mut state = load_or_initialize(&repo, Some(topology)).unwrap();
    let now = Instant::now();
    let mut grants = BTreeMap::new();
    for node_id in ["n1", "n2", "n3"] {
        let process = format!("{node_id}-process");
        state
            .process_instances
            .insert(node_id.into(), process.clone());
        grants.insert(
            node_id.into(),
            NodeLeaseGrant {
                process_instance_id: process,
                epoch: 1,
                expires_at: now + NODE_LEASE_DURATION,
            },
        );
    }
    let failure = PeerFailure {
        first_seen: now - Duration::from_secs(6),
        last_seen: now,
    };
    let mut reports = BTreeMap::from([(("n1".into(), "n2".into()), failure.clone())]);
    assert!(!failure_confirmed(&state, &grants, &reports, "n1", now));
    reports.insert(("n1".into(), "n3".into()), failure);
    assert!(failure_confirmed(&state, &grants, &reports, "n1", now));
    reports.clear();
    grants.remove("n1");
    assert!(failure_confirmed(&state, &grants, &reports, "n1", now));
}

#[tokio::test]
async fn confirmed_failure_fences_before_waiting_for_repair_serialization() {
    let members: Vec<_> = (1..=3)
        .map(|index| Member {
            node_id: format!("n{index}"),
            endpoint: format!("http://127.0.0.1:500{index}"),
        })
        .collect();
    let topology = TopologySnapshot::new(1, 7, 1, members).unwrap();
    let repository = Arc::new(MemoryRepository::default());
    let mut state = load_or_initialize(repository.as_ref(), Some(topology)).unwrap();
    for node_id in ["n1", "n2", "n3"] {
        state
            .process_instances
            .insert(node_id.into(), format!("{node_id}-process"));
    }
    let mut service = CoordinatorService::new(state, repository, Duration::from_secs(1));
    service.startup_at = Instant::now() - NODE_LEASE_DURATION;
    let held_repair_lock = service.execution_lock.lock().await;
    let mut interrupt = service.repair_interrupt.subscribe();
    let runner = service.clone();
    let task = tokio::spawn(async move { runner.run_failure_pass().await });
    tokio::time::timeout(Duration::from_secs(1), interrupt.changed())
        .await
        .expect("fencing should interrupt a repair without waiting for its lock")
        .unwrap();
    let state = service.state.read().await;
    assert!(state.fenced_nodes.contains("n1"));
    assert_eq!(
        state
            .active_change
            .as_ref()
            .unwrap()
            .failed_node_id
            .as_deref(),
        Some("n1")
    );
    drop(state);
    task.abort();
    drop(held_repair_lock);
}

#[test]
fn failed_owner_vnodes_promote_to_multiple_clockwise_successors() {
    let members: Vec<_> = (1..=4)
        .map(|index| Member {
            node_id: format!("n{index}"),
            endpoint: format!("http://127.0.0.1:500{index}"),
        })
        .collect();
    let old = TopologySnapshot::new(1, 7, 32, members.clone()).unwrap();
    let target = TopologyChange::plan(
        &old,
        members
            .into_iter()
            .filter(|member| member.node_id != "n1")
            .collect(),
    )
    .unwrap()
    .target_topology;
    let successors: BTreeSet<_> = old
        .derived_ranges()
        .unwrap()
        .into_iter()
        .filter(|range| range.owner_node_id == "n1")
        .map(|range| {
            target
                .owner_for_token(range.end_inclusive)
                .unwrap()
                .node_id
                .clone()
        })
        .collect();
    assert!(
        successors.len() > 1,
        "vnode failover should distribute ownership"
    );
}

#[test]
fn policy_epoch_barrier_requires_every_admitted_stream_caught_up() {
    let base = TopologySnapshot::new_with_config(
        1,
        7,
        1,
        vec![
            Member {
                node_id: "n1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            },
            Member {
                node_id: "n2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
            Member {
                node_id: "n3".into(),
                endpoint: "http://127.0.0.1:5003".into(),
            },
        ],
        hashring_core::topology::TopologyConfig::default(),
    )
    .unwrap();
    let target = TopologyChange::plan_with_policy(
        &base,
        base.members.clone(),
        WriteAckPolicy::FirstSuccessor,
    )
    .unwrap()
    .target_topology;
    let mut status = proto::ReplicaStatusResponse {
        topology_epoch: base.epoch,
        ranges: target
            .derived_ranges()
            .unwrap()
            .into_iter()
            .map(|range| proto::RangeReplicaStatus {
                start_exclusive: range.start_exclusive,
                end_inclusive: range.end_inclusive,
                owner_node_id: range.owner_node_id,
                desired_rf: 3,
                current_rf: 3,
                followers: range
                    .follower_node_ids
                    .into_iter()
                    .map(|node_id| proto::FollowerReplicaStatus {
                        node_id,
                        admitted: true,
                        lag_known: true,
                        stream_head: 1,
                        stream_cursor: 1,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    assert!(policy_readiness_verified(&target, &status));
    status.ranges[0].followers[1].stream_cursor = 0;
    assert!(!policy_readiness_verified(&target, &status));
}

#[tokio::test]
async fn bounded_range_work_overlaps_and_honors_limit() {
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let first_batch = Arc::new(Barrier::new(4));
    let work = try_map_bounded(0..6, 3, |item| {
        let running = running.clone();
        let peak = peak.clone();
        let first_batch = first_batch.clone();
        async move {
            let active = running.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = ActiveWork(running);
            peak.fetch_max(active, Ordering::SeqCst);
            if item < 3 {
                first_batch.wait().await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok::<_, ()>(item)
        }
    });

    let (_, result) = tokio::join!(first_batch.wait(), work);
    let mut result = result.unwrap();
    result.sort_unstable();
    assert_eq!(result, (0..6).collect::<Vec<_>>());
    assert_eq!(peak.load(Ordering::SeqCst), 3);
    assert_eq!(running.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bounded_range_work_drains_started_siblings_after_an_error() {
    let sibling_completed = Arc::new(AtomicUsize::new(0));
    let unscheduled_started = Arc::new(AtomicUsize::new(0));
    let work = try_map_bounded(0..3, 2, |item| {
        let sibling_completed = sibling_completed.clone();
        let unscheduled_started = unscheduled_started.clone();
        async move {
            match item {
                0 => Err("range failed"),
                1 => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    sibling_completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                _ => {
                    unscheduled_started.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        }
    });

    let result = work.await;
    assert_eq!(result, Err("range failed"));
    assert_eq!(sibling_completed.load(Ordering::SeqCst), 1);
    assert_eq!(unscheduled_started.load(Ordering::SeqCst), 0);
}

#[test]
fn initializes_once_then_loads_durable_value() {
    let repository = MemoryRepository::default();
    let expected = topology();
    assert_eq!(
        load_or_initialize(&repository, Some(expected.clone()))
            .unwrap()
            .committed,
        expected
    );
    assert_eq!(
        load_or_initialize(&repository, None).unwrap().committed,
        expected
    );
}

#[test]
fn upgrades_prior_v1_topology_change_shape() {
    let committed = topology();
    let change = TopologyChange::plan(
        &committed,
        vec![
            committed.members[0].clone(),
            Member {
                node_id: "n2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
        ],
    )
    .unwrap();
    let state = ClusterState {
        committed,
        active_change: Some(change),
        process_instances: BTreeMap::new(),
        stop_confirmations: BTreeMap::new(),
        replica_admissions: Vec::new(),
        replica_repairs: Vec::new(),
        fenced_nodes: BTreeSet::new(),
    };
    let mut old = serde_json::to_value(state).unwrap();
    old.as_object_mut().unwrap().remove("process_instances");
    old.as_object_mut().unwrap().remove("stop_confirmations");
    let active = old["active_change"].as_object_mut().unwrap();
    active.remove("stopped_node_ids");
    active.remove("stopping_node_ids");
    active.remove("stop_prepared_node_ids");
    for range in active["ranges"].as_array_mut().unwrap() {
        let range = range.as_object_mut().unwrap();
        range.remove("source_endpoint");
        range.remove("destination_endpoint");
        range.remove("source_process_instance_id");
        range.remove("destination_process_instance_id");
        range.remove("source_cleaned");
    }

    let mut restored: ClusterState = serde_json::from_value(old).unwrap();
    upgrade_persisted_state(&mut restored).unwrap();
    let restored = restored.active_change.unwrap();
    assert!(
        restored.ranges.iter().all(
            |range| !range.source_endpoint.is_empty() && !range.destination_endpoint.is_empty()
        )
    );
    assert!(restored.stopped_node_ids.is_empty());
    assert!(restored.stopping_node_ids.is_empty());
    assert!(restored.stop_prepared_node_ids.is_empty());
}

#[tokio::test]
async fn durable_stop_confirmation_survives_active_change_replacement() {
    let committed = topology();
    let change = TopologyChange::plan(
        &committed,
        vec![Member {
            node_id: "n2".into(),
            endpoint: "http://127.0.0.1:5002".into(),
        }],
    )
    .unwrap();
    let repository = Arc::new(MemoryRepository::default());
    let service = CoordinatorService::new(
        ClusterState {
            committed,
            active_change: Some(change),
            process_instances: BTreeMap::from([("n1".into(), "process-1".into())]),
            stop_confirmations: BTreeMap::new(),
            replica_admissions: Vec::new(),
            replica_repairs: Vec::new(),
            fenced_nodes: BTreeSet::new(),
        },
        repository,
        Duration::from_secs(1),
    );

    service
        .store_stop_prepared_node("n1", "process-1")
        .await
        .unwrap();
    service.store_stopped_node("n1").await.unwrap();
    {
        let mut state = service.state.write().await;
        state.active_change = None;
    }

    let response = service
        .is_stop_confirmed(Request::new(proto::StopRequest {
            node_id: "n1".into(),
            process_instance_id: "process-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.confirmed);
    let replacement = service
        .is_stop_confirmed(Request::new(proto::StopRequest {
            node_id: "n1".into(),
            process_instance_id: "process-2".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!replacement.confirmed);
}

#[tokio::test]
async fn replica_status_only_counts_verified_current_processes() {
    let repository = Arc::new(MemoryRepository::default());
    let topology = TopologySnapshot::new(
        1,
        7,
        1,
        vec![
            Member {
                node_id: "n1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            },
            Member {
                node_id: "n2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
            Member {
                node_id: "n3".into(),
                endpoint: "http://127.0.0.1:5003".into(),
            },
        ],
    )
    .unwrap();
    let mut state = load_or_initialize(repository.as_ref(), Some(topology)).unwrap();
    assert_eq!(
        state.replica_repairs.len(),
        state.committed.derived_ranges().unwrap().len() * 2
    );
    let repair = state.replica_repairs[0].clone();
    state
        .process_instances
        .insert(repair.owner_node_id.clone(), "owner-1".into());
    state
        .process_instances
        .insert(repair.node_id.clone(), "follower-1".into());
    state.replica_admissions.push(ReplicaAdmission {
        epoch: repair.epoch,
        start_exclusive: repair.start_exclusive,
        end_inclusive: repair.end_inclusive,
        owner_node_id: repair.owner_node_id.clone(),
        node_id: repair.node_id.clone(),
        process_instance_id: "follower-1".into(),
        verified_watermark: 4,
        stream_cursor: 7,
        digest: "verified".into(),
    });
    state.replica_repairs[0].phase = ReplicaRepairPhase::Complete;
    repository.store_state(&state).unwrap();
    let restored = load_or_initialize(repository.as_ref(), None).unwrap();
    assert_eq!(restored.replica_admissions, state.replica_admissions);
    let service = CoordinatorService::new(restored, repository, Duration::from_secs(1));
    let status = service
        .get_replica_status(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();
    let range = status
        .ranges
        .iter()
        .find(|range| {
            range.start_exclusive == repair.start_exclusive
                && range.end_inclusive == repair.end_inclusive
        })
        .unwrap();
    assert_eq!(range.current_rf, 2);
    assert_eq!(range.live_rf, 0);
    assert!(!range.owner_leased);
    assert!(!range.writable);
    assert_eq!(range.write_block_reason, "owner lease unavailable");
    assert!(range.under_replicated);
    assert!(range.repairing);
    assert!(
        range
            .followers
            .iter()
            .any(|follower| follower.node_id == repair.node_id && follower.admitted)
    );
    service
        .state
        .write()
        .await
        .process_instances
        .insert(repair.node_id, "follower-2".into());
    let status = service
        .get_replica_status(Request::new(proto::Empty {}))
        .await
        .unwrap()
        .into_inner();
    let range = status
        .ranges
        .iter()
        .find(|range| {
            range.start_exclusive == repair.start_exclusive
                && range.end_inclusive == repair.end_inclusive
        })
        .unwrap();
    assert_eq!(range.current_rf, 1);
    let mut state = service.state.read().await.clone();
    reconcile_replica_repairs(&mut state).unwrap();
    assert!(state.replica_admissions.is_empty());
    assert_eq!(state.replica_repairs[0].phase, ReplicaRepairPhase::Pending);
}

#[tokio::test]
async fn repair_failure_is_durably_backed_off_and_reported() {
    let repository = Arc::new(MemoryRepository::default());
    let topology = TopologySnapshot::new(
        1,
        7,
        1,
        vec![
            Member {
                node_id: "n1".into(),
                endpoint: "http://127.0.0.1:5001".into(),
            },
            Member {
                node_id: "n2".into(),
                endpoint: "http://127.0.0.1:5002".into(),
            },
            Member {
                node_id: "n3".into(),
                endpoint: "http://127.0.0.1:5003".into(),
            },
        ],
    )
    .unwrap();
    let state = load_or_initialize(repository.as_ref(), Some(topology)).unwrap();
    let task = state.replica_repairs[0].clone();
    let service = CoordinatorService::new(state, repository.clone(), Duration::from_secs(1));
    let before = unix_millis_now();
    service
        .record_repair_failure(&[task], &Status::unavailable("follower offline"))
        .await
        .unwrap();
    let restored = load_or_initialize(repository.as_ref(), None).unwrap();
    let repair = &restored.replica_repairs[0];
    assert_eq!(repair.retry_count, 1);
    assert!(repair.next_attempt_unix_millis >= before + 2_000);
    assert_eq!(repair.last_error, "follower offline");
}
