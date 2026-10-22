use super::*;

#[tokio::test]
async fn experiment_runner_preserves_reproducibility_artifacts() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let output_directory = directory.path().join("experiment");
    let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
        .args([
            "experiment",
            "--mode",
            "correctness",
            "--output",
            output_directory.to_str().unwrap(),
            "--nodes",
            "4",
            "--keys",
            "300",
            "--value-bytes",
            "32",
            "--concurrency",
            "16",
            "--virtual-nodes",
            "16",
            "--operation-timeout-ms",
            "10000",
            "--migration-timeout-ms",
            "60000",
            "--max-dedup-bytes",
            "33554432",
            "--verbose",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "experiment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["config"]["node_count"], 4);
    assert_eq!(manifest["config"]["key_count"], 300);
    assert_eq!(manifest["schema_version"], 8);
    // Every node is started with the run's tuning, so it must parse.
    assert_eq!(manifest["config"]["node"]["max_dedup_bytes"], 33_554_432);
    assert_eq!(manifest["executable_blake3"].as_str().unwrap().len(), 64);
    assert!(manifest["build"]["git_commit"].is_string());
    assert_eq!(manifest["config"]["pre_publish_delay_ms"], 250);
    assert_eq!(manifest["config"]["range_move_concurrency"], 16);
    assert_eq!(manifest["config"]["verbose"], true);
    assert_eq!(
        manifest["build"]["source_tree_blake3"],
        manifest["runtime_source"]["source_tree_blake3"]
    );
    let expected_source_reproducible = manifest["build"]["git_dirty"] == "false"
        && manifest["runtime_source"]["git_dirty"] == false
        && manifest["build"]["git_commit"] == manifest["runtime_source"]["git_commit"]
        && manifest["build"]["source_tree_blake3"]
            == manifest["runtime_source"]["source_tree_blake3"];
    assert_eq!(
        manifest["source_reproducible"],
        expected_source_reproducible
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary["success"], true);
    assert_eq!(summary["final_epoch"], 3);
    assert!(summary["measurements"]["scale_out_with_mutations"].is_object());
    assert!(summary["measurements"]["scale_in_with_mutations"].is_object());
    assert!(summary["observations"]["scale_out_transition_to_full_rf_seconds"].is_number());
    assert!(summary["observations"]["scale_in_transition_to_full_rf_seconds"].is_number());
    assert!(summary["measurements"]["scale_out_with_writes"].is_null());
    assert!(summary["measurements"]["scale_in_with_writes"].is_null());
    let events = std::fs::read_to_string(output_directory.join("events.jsonl")).unwrap();
    assert_eq!(
        events
            .lines()
            .filter(|line| line.contains("migration_mutation_overlap_observed"))
            .count(),
        1
    );
    assert!(events.contains("direct_merge_cutover_observed"));
    assert!(events.contains("untouched_moving_sentinels"));
    assert!(events.contains("deleted_moving_keys"));
    assert!(events.contains("restored_moving_keys"));
    assert!(
        output_directory
            .join("process-logs/coordinator.stderr.log")
            .is_file()
    );
    let coordinator_log =
        std::fs::read_to_string(output_directory.join("process-logs/coordinator.stdout.log"))
            .unwrap();
    assert!(coordinator_log.contains("topology activation ready"));
    assert!(!coordinator_log.contains("replica activation seeding planned"));
}

#[tokio::test]
async fn latency_experiment_measures_one_stage_per_target_rate() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let output_directory = directory.path().join("latency");
    let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
        .args([
            "experiment",
            "--mode",
            "latency",
            "--output",
            output_directory.to_str().unwrap(),
            "--nodes",
            "3",
            "--preload-keys",
            "500",
            "--target-rates",
            "200,400",
            "--warmup-seconds",
            "1",
            "--duration-seconds",
            "2",
            "--mix",
            "90:9:1",
            "--key-distribution",
            "uniform",
            "--virtual-nodes",
            "16",
            "--operation-timeout-ms",
            "10000",
            "--migration-timeout-ms",
            "60000",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "latency experiment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["config"]["mode"], "latency");
    assert_eq!(manifest["config"]["preload_keys"], 500);
    assert_eq!(manifest["config"]["key_count"], 500);
    assert_eq!(
        manifest["config"]["target_rates"],
        serde_json::json!([200, 400])
    );
    assert_eq!(manifest["config"]["mix"], "90:9:1");
    assert_eq!(manifest["config"]["key_distribution"], "uniform");

    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary["success"], true, "{}", summary["error"]);
    // One measurement per target rate, and nothing else: the preload and the
    // check that follows it are observations of a latency run.
    let measurements = summary["measurements"].as_object().unwrap();
    assert_eq!(measurements.len(), 2);
    // Every stage accounts for every request it sent. A stage that starts
    // with the counters of the stage before it reports that stage's requests
    // as its own, and a stage that misses its last window is short by a
    // second's worth of them.
    for (name, stage) in measurements {
        let sum = |field: &str| -> u64 {
            stage[field]
                .as_object()
                .map(|entries| {
                    entries
                        .values()
                        .map(|value| value.as_u64().unwrap_or_default())
                        .sum()
                })
                .unwrap_or_default()
        };
        assert_eq!(
            sum("ops") + sum("errors"),
            stage["issued"].as_u64().unwrap(),
            "stage {name} does not account for every request it issued"
        );
        assert!(
            (stage["achieved_rate"].as_f64().unwrap() - stage["target_rate"].as_f64().unwrap())
                .abs()
                < 1.0,
            "stage {name} did not run at its target rate: {} against {}",
            stage["achieved_rate"],
            stage["target_rate"]
        );
    }
    let stage = &measurements["rate_0"];
    assert_eq!(stage["target_rate"], 200);
    assert_eq!(stage["sender_saturated"], false);
    assert_eq!(stage["late"], 0);
    assert!(stage["achieved_rate"].as_f64().unwrap() > 0.0);
    assert!(stage["seconds"].as_f64().unwrap() >= 2.0);
    // Every request the driver sent is accounted for.
    let completed: u64 = stage["ops"]
        .as_object()
        .unwrap()
        .values()
        .map(|value| value.as_u64().unwrap())
        .sum();
    assert_eq!(completed, stage["issued"].as_u64().unwrap());
    assert!(stage["latency_us"]["get"]["samples"].as_u64().unwrap() > 0);
    assert!(stage["latency_us"]["get"]["p99"].as_u64().unwrap() > 0);
    // The histogram keeps the longest sample and the number of the samples
    // that were longer than its range, so a reader can tell a truncated
    // sample from one that was near the bound.
    let observed = stage["latency_us"]["get"]["max_observed"].as_u64().unwrap();
    let bucket = stage["latency_us"]["get"]["max"].as_u64().unwrap();
    assert!(bucket >= observed && bucket - observed <= bucket / 1_000 + 1);
    assert_eq!(
        stage["latency_us"]["get"]["over_range"].as_u64().unwrap(),
        0
    );
    let balance = stage["load_balance"].as_object().unwrap();
    assert_eq!(balance["per_node"].as_object().unwrap().len(), 3);
    assert!(balance["owned_keys_max_over_mean"].as_f64().unwrap() >= 1.0);
    let owned: u64 = balance["per_node"]
        .as_object()
        .unwrap()
        .values()
        .map(|node| node["owned_keys"].as_u64().unwrap())
        .sum();
    assert_eq!(owned, 500);
    // The key space the load left behind was checked.
    assert_eq!(summary["observations"]["latency_verified_keys"], 500);

    let events = std::fs::read_to_string(output_directory.join("events.jsonl")).unwrap();
    let samples: Vec<serde_json::Value> = events
        .lines()
        .filter(|line| line.contains("\"metrics_sampled\""))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    // One sample per second of every stage, the warmup included.
    assert_eq!(samples.len(), 6);
    assert_eq!(samples[0]["fields"]["stage"], "rate_0");
    assert_eq!(samples[0]["fields"]["warmup"], true);
    assert_eq!(samples[0]["fields"]["target_rate"], 200);
    assert_eq!(samples[1]["fields"]["stage"], "rate_0");
    assert_eq!(samples[1]["fields"]["warmup"], false);
    assert_eq!(samples[3]["fields"]["stage"], "rate_1");
    assert_eq!(samples[3]["fields"]["warmup"], true);
    assert_eq!(samples[3]["fields"]["target_rate"], 400);
    assert_eq!(samples[4]["fields"]["stage"], "rate_1");
    assert_eq!(samples[4]["fields"]["warmup"], false);
    assert!(samples[0]["fields"]["issued"].as_u64().unwrap() > 0);
    // The window reports its own lag and the sender's, which are two
    // different numbers.
    assert!(samples[0]["fields"].get("behind").is_some());
    assert!(samples[0]["fields"].get("sender_lag").is_some());
    // The ring is recorded, so a run that has ended can still be read.
    assert!(events.contains("topology_published"));
}

#[tokio::test]
async fn availability_experiment_records_failover_loss_and_rf_repair() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for (policy, expected_epoch) in [("owner-only", 3), ("first-successor", 4)] {
        let output_directory = directory.path().join(policy);
        let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
            .args([
                "experiment",
                "--mode",
                "availability",
                "--policy",
                policy,
                "--output",
                output_directory.to_str().unwrap(),
                "--keys",
                "90",
                "--virtual-nodes",
                "2",
                "--operation-timeout-ms",
                "10000",
                "--migration-timeout-ms",
                "90000",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{policy} experiment failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["success"], true);
        assert_eq!(summary["final_epoch"], expected_epoch);
        assert!(summary["observations"]["failover_seconds"].is_number());
        assert!(summary["observations"]["read_recovery_seconds"].is_number());
        assert!(summary["observations"]["replacement_to_full_rf_seconds"].is_number());
        assert!(summary["observations"]["idle_client_put_latency_us"].is_object());
        assert!(summary["observations"]["idle_owner_rpc_put_latency_us"].is_object());
        assert!(
            summary["observations"]["acknowledged_key_survival"]["sampled"]
                .as_u64()
                .unwrap()
                > 0
        );
        if policy == "first-successor" {
            assert_eq!(
                summary["observations"]["acknowledged_key_survival"]["lost"],
                0
            );
            assert_eq!(
                summary["observations"]["acknowledged_key_survival"]["tail_survived"],
                true
            );
        }
        let events = std::fs::read_to_string(output_directory.join("events.jsonl")).unwrap();
        assert!(events.contains("owner_killed"));
        assert!(events.contains("tail_ack_to_kill_ms"));
        assert!(events.contains("full_rf_restored"));
    }
}

#[tokio::test]
async fn scaling_control_experiment_runs_closed_and_open_loads() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for (label, load_mode, keys, open_rate) in [
        ("closed", "closed", "2000", &[] as &[&str]),
        ("open", "open", "3000", &["--scaling-open-rate", "200"]),
    ] {
        let output_directory = directory.path().join(label);
        let mut arguments = vec![
            "experiment",
            "--mode",
            "scaling",
            "--scaling-scenario",
            "control",
            "--scaling-load-mode",
            load_mode,
            "--output",
            output_directory.to_str().unwrap(),
            "--nodes",
            "3",
            "--keys",
            keys,
            "--concurrency",
            "8",
            "--scaling-write-rate",
            "20",
            "--scaling-pool-seconds",
            "30",
            "--warmup-seconds",
            "1",
            "--scaling-post-seconds",
            "2",
            "--max-in-flight",
            "256",
            "--virtual-nodes",
            "16",
        ];
        arguments.extend_from_slice(open_rate);
        let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
            .args(&arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{label} scaling control experiment failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["success"], true);
        let transition = &summary["observations"]["control_transition"];
        assert_eq!(transition["control"], true);
        assert_eq!(transition["direct_merge"], false);
        assert!(transition["work_steps_ms"].is_null());
        assert_eq!(transition["retries_by_reason"]["range_busy"], 0);
        assert_eq!(
            summary["observations"]["scaling_metrics"]["load_mode"],
            load_mode
        );
        if load_mode == "open" {
            assert!(
                summary["observations"]["scaling_metrics"]["issued"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        }
        let events = std::fs::read_to_string(output_directory.join("events.jsonl")).unwrap();
        assert!(events.contains("scaling_control_window_started"));
        assert!(events.contains("scaling_transition_complete"));
    }
}

#[tokio::test]
async fn scaling_open_loop_round_trip_drains_its_own_writes() {
    // Reads keep running across a transition, so the local drain may only
    // wait for the write batches. At this rate the reads are always in
    // flight, which is the case a drain over all requests cannot finish.
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let output_directory = directory.path().join("open-round-trip");
    let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
        .args([
            "experiment",
            "--mode",
            "scaling",
            "--scaling-scenario",
            "round-trip",
            "--scaling-load-mode",
            "open",
            "--scaling-open-rate",
            "6000",
            "--output",
            output_directory.to_str().unwrap(),
            "--nodes",
            "4",
            "--desired-replication-factor",
            "2",
            "--keys",
            "40000",
            "--concurrency",
            "8",
            "--scaling-pool-seconds",
            "8",
            "--warmup-seconds",
            "1",
            "--scaling-post-seconds",
            "2",
            "--max-in-flight",
            "256",
            "--virtual-nodes",
            "16",
            "--operation-timeout-ms",
            "30000",
            "--migration-timeout-ms",
            "120000",
            "--max-dedup-bytes",
            "33554432",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "open-loop round-trip failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary["success"], true);
    let metrics = &summary["observations"]["scaling_metrics"];
    assert_eq!(metrics["load_mode"], "open");
    assert!(metrics["issued"].as_u64().unwrap() > 0);
    assert_eq!(metrics["in_flight"], 0);
    let grow = &summary["observations"]["grow_transition"];
    assert_eq!(grow["verification"], "deferred_to_next_transition");
    let shrink = &summary["observations"]["shrink_transition"];
    assert_eq!(shrink["verification"], "passed");
    for transition in [grow, shrink] {
        assert!(transition["stop_window"]["observed_ms"].as_f64().unwrap() > 0.0);
    }
}

#[tokio::test]
async fn scaling_round_trip_defers_s1_verification_and_records_the_stop_window() {
    let _guard = process_test_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let output_directory = directory.path().join("round-trip");
    let output = Command::new(env!("CARGO_BIN_EXE_hashring-rs"))
        .args([
            "experiment",
            "--mode",
            "scaling",
            "--scaling-scenario",
            "round-trip",
            "--output",
            output_directory.to_str().unwrap(),
            "--nodes",
            "4",
            "--desired-replication-factor",
            "2",
            "--keys",
            "8000",
            "--concurrency",
            "8",
            "--scaling-write-rate",
            "200",
            "--scaling-pool-seconds",
            "8",
            "--warmup-seconds",
            "1",
            "--scaling-post-seconds",
            "2",
            "--max-in-flight",
            "256",
            "--virtual-nodes",
            "16",
            "--operation-timeout-ms",
            "30000",
            "--migration-timeout-ms",
            "120000",
            "--max-dedup-bytes",
            "33554432",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "round-trip scaling experiment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary["success"], true);

    // S1's cohorts are verified at the start of S2, so a violation cannot be
    // attributed to the wrong change.
    let grow = &summary["observations"]["grow_transition"];
    assert_eq!(grow["verification"], "deferred_to_next_transition");
    let shrink = &summary["observations"]["shrink_transition"];
    assert_eq!(shrink["verification"], "passed");
    assert_eq!(shrink["direct_merge"], true);
    assert!(
        grow["retries_by_reason"]["range_busy"].as_u64().unwrap() > 0,
        "the grow change produced no client-observed RangeBusy rejection"
    );

    // The write stop is defined by what the client saw, not by the phases.
    // Publication, completion, and the RF convergence after completion are
    // separate time points.
    for transition in [grow, shrink] {
        let window = &transition["stop_window"];
        let first = window["first_range_busy_ms"].as_f64().unwrap();
        let last = window["last_range_busy_ms"].as_f64().unwrap();
        let first_ok = window["first_write_ok_ms"].as_f64().unwrap();
        assert!(first <= last, "{window}");
        assert!(last <= first_ok, "{window}");
        assert!(window["observed_ms"].as_f64().unwrap() > 0.0, "{window}");
        let published = transition["published_ms"].as_f64().unwrap();
        let complete = transition["complete_ms"].as_f64().unwrap();
        assert!(published > 0.0, "{transition}");
        assert!(complete >= published, "{transition}");
        assert!(
            transition["full_rf_seconds"].as_f64().unwrap() >= 0.0,
            "{transition}"
        );
    }

    // The two changes never share a key, so the S2 pools cannot see a value
    // that the S1 settlement left unknown.
    let events = std::fs::read_to_string(output_directory.join("events.jsonl")).unwrap();
    assert!(events.contains("scaling_cohorts_verified"));
    assert!(events.contains("removed_node_exited"));
}
