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
    assert_eq!(manifest["schema_version"], 4);
    assert_eq!(manifest["executable_blake3"].as_str().unwrap().len(), 64);
    assert!(manifest["build"]["git_commit"].is_string());
    assert_eq!(manifest["config"]["pre_publish_delay_ms"], 250);
    assert_eq!(manifest["config"]["range_move_concurrency"], 16);
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
        assert!(summary["observations"]["client_put_latency_us"].is_object());
        assert!(summary["observations"]["owner_rpc_put_latency_us"].is_object());
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
