use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::future::join_all;
use hashring_client::HashringClient;
use hashring_core::{
    proto::{Empty, data_node_client::DataNodeClient},
    topology::{Member, TopologySnapshot},
    transport::configure_data_node_client,
};
use serde_json::json;
use tonic::transport::{Channel, Endpoint};

use crate::EventLog;

#[derive(Clone)]
struct Reading {
    instance: String,
    operations: u64,
    observed_at: Instant,
}

#[derive(Clone)]
pub(super) struct NodeOperationSampler {
    clients: BTreeMap<String, DataNodeClient<Channel>>,
    previous: BTreeMap<String, Reading>,
    epoch: Option<u64>,
}

impl NodeOperationSampler {
    pub(super) fn new(members: &[Member]) -> Result<Self> {
        let mut clients = BTreeMap::new();
        for member in members {
            let endpoint = Endpoint::from_shared(member.endpoint.clone())?;
            let client = configure_data_node_client(DataNodeClient::new(endpoint.connect_lazy()));
            clients.insert(member.node_id.clone(), client);
        }
        Ok(Self {
            clients,
            previous: BTreeMap::new(),
            epoch: None,
        })
    }

    pub(super) async fn sample(&mut self, topology: &TopologySnapshot) -> BTreeMap<String, f64> {
        let clients = topology
            .members
            .iter()
            .filter_map(|member| self.clients.get(&member.node_id).cloned());
        // An unavailable node leaves a gap for that node while the other requests complete.
        let current: BTreeMap<_, _> = join_all(clients.map(|mut client| async move {
            let info = tokio::time::timeout(
                Duration::from_millis(200),
                client.get_process_info(Empty {}),
            )
            .await
            .ok()?
            .ok()?
            .into_inner();
            Some((
                info.node_id,
                Reading {
                    instance: info.process_instance_id,
                    operations: info.ops_handled,
                    observed_at: Instant::now(),
                },
            ))
        }))
        .await
        .into_iter()
        .flatten()
        .collect();

        let mut rates = BTreeMap::new();
        if self.epoch == Some(topology.epoch) {
            for (id, reading) in &current {
                let Some(previous) = self
                    .previous
                    .get(id)
                    .filter(|before| before.instance == reading.instance)
                else {
                    continue;
                };
                let seconds = reading
                    .observed_at
                    .duration_since(previous.observed_at)
                    .as_secs_f64();
                if let Some(operations) = reading.operations.checked_sub(previous.operations)
                    && seconds > 0.0
                {
                    rates.insert(id.clone(), operations as f64 / seconds);
                }
            }
        }
        self.previous = current;
        self.epoch = Some(topology.epoch);
        rates
    }

    pub(super) async fn record(mut self, client: HashringClient, events: EventLog) {
        let mut clock = tokio::time::interval(Duration::from_secs(1));
        clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            clock.tick().await;
            let topology = client.topology().await;
            let rates = self.sample(&topology).await;
            if rates.is_empty() || client.epoch().await != topology.epoch {
                continue;
            }
            if events
                .record(
                    "node_load_sampled",
                    json!({
                        "epoch": topology.epoch,
                        "node_ops_per_second": rates,
                    }),
                )
                .is_err()
            {
                break;
            }
        }
    }
}
