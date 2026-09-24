use super::*;

pub(super) fn interval_contains(start: u64, end: u64, token: u64) -> bool {
    if start == end {
        true
    } else if start < end {
        token > start && token <= end
    } else {
        token > start || token <= end
    }
}

pub(super) fn intervals_overlap(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    interval_contains(a_start, a_end, b_end) || interval_contains(b_start, b_end, a_end)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ChangeIdentity {
    pub(super) change_id: String,
    pub(super) base_epoch: u64,
    pub(super) target_epoch: u64,
}

pub(super) fn change_identity(change: &TopologyChange) -> ChangeIdentity {
    ChangeIdentity {
        change_id: change.change_id.clone(),
        base_epoch: change.base_epoch,
        target_epoch: change.target_topology.epoch,
    }
}

pub(super) fn destination_instances(
    change: &TopologyChange,
) -> Result<BTreeMap<String, (String, String)>, Status> {
    let mut destinations = BTreeMap::new();
    for range in &change.ranges {
        if range.destination_process_instance_id.is_empty() {
            return Err(Status::data_loss(format!(
                "missing process instance for destination {}",
                range.destination_node_id
            )));
        }
        let identity = (
            range.destination_endpoint.clone(),
            range.destination_process_instance_id.clone(),
        );
        if destinations
            .insert(range.destination_node_id.clone(), identity.clone())
            .is_some_and(|previous| previous != identity)
        {
            return Err(Status::data_loss(format!(
                "destination {} changed process instance during migration",
                range.destination_node_id
            )));
        }
    }
    Ok(destinations)
}

pub(super) fn replace_range_progress(change: &mut TopologyChange, progress: RangeMigration) {
    if let Some(current) = change
        .ranges
        .iter_mut()
        .find(|candidate| candidate.range_id == progress.range_id)
    {
        *current = progress;
    }
}

pub(super) fn is_absence_status(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
}

pub(super) async fn try_map_bounded<I, F, Fut, T, E>(
    items: I,
    concurrency: usize,
    mut operation: F,
) -> Result<Vec<T>, E>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut items = items.into_iter();
    let mut in_flight = FuturesUnordered::new();
    for item in items.by_ref().take(concurrency) {
        in_flight.push(operation(item));
    }

    // State-changing RPCs may continue remotely if their client futures are dropped.
    // After the first error, stop launching work but drain every operation already started
    // before the caller begins abort cleanup.
    let mut values = Vec::new();
    let mut first_error = None;
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(value) => {
                values.push(value);
                if first_error.is_none()
                    && let Some(item) = items.next()
                {
                    in_flight.push(operation(item));
                }
            }
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(values),
    }
}

pub(super) async fn replay_changelog(
    source: &mut DataNodeClient<tonic::transport::Channel>,
    destination: &mut DataNodeClient<tonic::transport::Channel>,
    change: &TopologyChange,
    range: &RangeMigration,
    mut watermark: u64,
    final_watermark: Option<u64>,
    deadline: Instant,
) -> Result<u64, Status> {
    loop {
        let page = rpc_before(
            deadline,
            source.read_changelog_page(ChangelogPageRequest {
                change_id: change.change_id.clone(),
                range_id: range.range_id.clone(),
                after_watermark: watermark,
                max_bytes: MAX_MIGRATION_PAGE_BYTES as u64,
            }),
        )
        .await?
        .into_inner();
        let target = final_watermark.unwrap_or(page.current_watermark);
        let page_is_empty = page.records.is_empty();
        if let Some(last) = page.records.last() {
            watermark = last.watermark;
        }
        if !page_is_empty {
            rpc_before(
                deadline,
                destination.apply_migration_batch(ApplyMigrationBatchRequest {
                    change_id: change.change_id.clone(),
                    range_id: range.range_id.clone(),
                    snapshot_records: Vec::new(),
                    journal_records: page.records,
                }),
            )
            .await?;
        }
        if watermark >= target {
            return Ok(watermark);
        }
        if page_is_empty && watermark < target {
            return Err(Status::data_loss(
                "source changelog omitted records before its watermark",
            ));
        }
    }
}

pub(super) async fn connect_node(
    endpoint: &str,
    deadline: Instant,
) -> Result<DataNodeClient<tonic::transport::Channel>, Status> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("migration deadline exceeded"))?;
    let client = tokio::time::timeout(remaining, DataNodeClient::connect(endpoint.to_owned()))
        .await
        .map_err(|_| Status::deadline_exceeded("migration deadline exceeded"))?
        .map_err(|error| Status::unavailable(error.to_string()))?;
    Ok(client
        .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES))
}

pub(super) async fn rpc_before<T, F>(deadline: Instant, future: F) -> Result<Response<T>, Status>
where
    F: Future<Output = Result<Response<T>, Status>>,
{
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("migration deadline exceeded"))?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| Status::deadline_exceeded("migration deadline exceeded"))?
}
