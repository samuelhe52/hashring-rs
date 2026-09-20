use crate::{
    limits::MAX_CONTROL_MESSAGE_BYTES,
    proto::{self, coordinator_client::CoordinatorClient},
    topology::TopologySnapshot,
};

pub async fn fetch_topology(endpoint: &str) -> anyhow::Result<TopologySnapshot> {
    let mut client =
        configure_coordinator_client(CoordinatorClient::connect(endpoint.to_owned()).await?);
    let response = client.get_topology(proto::Empty {}).await?.into_inner();
    Ok(response.try_into()?)
}

pub fn configure_coordinator_client(
    client: CoordinatorClient<tonic::transport::Channel>,
) -> CoordinatorClient<tonic::transport::Channel> {
    client
        .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
}
