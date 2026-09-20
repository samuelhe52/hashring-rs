// tonic's public service signatures return `tonic::Status`, which intentionally
// carries enough context to exceed Clippy's default large-error threshold.
#![allow(clippy::result_large_err)]

pub mod client;
pub mod coordinator;
pub mod node;
pub mod topology;

pub mod proto {
    tonic::include_proto!("hashring.v1");
}
