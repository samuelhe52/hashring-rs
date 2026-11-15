#!/usr/bin/env bash
set -euo pipefail
cd '/mnt/d/数据结构课程/实验7/项目/hashring-rs'
output=${1:-'/mnt/d/数据结构课程/实验7/项目/hashring-rs/work/dashboard-20261109/verification-results/verify-browser-1794159323716'}
exec '/mnt/d/数据结构课程/实验7/项目/hashring-rs/target/release/hashring-rs' experiment --output "$output" '--mode' 'latency' '--nodes' '3' '--keys' '2000' '--desired-replication-factor' '3' '--concurrency' '16' '--value-bytes' '128' '--seed' '1' '--virtual-nodes' '128' '--policy' 'owner-only' '--preload-keys' '2000' '--target-rates' '120,240' '--warmup-seconds' '1' '--duration-seconds' '4' '--mix' '90:9:1' '--key-distribution' 'uniform' '--max-in-flight' '4096'
