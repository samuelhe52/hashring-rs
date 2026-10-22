pub mod limits;
pub mod migration;
pub mod rpc_profile;
pub mod topology;
pub mod transport;

pub mod proto {
    tonic::include_proto!("hashring.v1");
}
