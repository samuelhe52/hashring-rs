// tonic's public service signatures return `tonic::Status`, which intentionally
// carries enough context to exceed Clippy's default large-error threshold.
#![allow(clippy::result_large_err)]

pub mod client {
    pub use hashring_client::*;
}

pub mod coordinator {
    pub use hashring_coordinator::*;
}

pub mod limits {
    pub use hashring_core::limits::*;
}

pub mod migration {
    pub use hashring_core::migration::*;
}

pub mod node {
    pub use hashring_node::*;
}

pub mod proto {
    pub use hashring_core::proto::*;
}

pub mod topology {
    pub use hashring_core::topology::*;
}

#[cfg(test)]
mod compatibility_tests {
    use super::node;

    #[test]
    fn legacy_node_transport_helpers_remain_exported() {
        let _ = node::fetch_topology;
        let _ = node::configure_coordinator_client;
    }
}
