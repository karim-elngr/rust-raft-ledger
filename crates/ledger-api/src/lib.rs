//! Public service definitions, canonical financial types and the Rust backend client.
/// Maximum encoded protobuf body in the public v1 protocol, excluding the gRPC frame header.
pub const MAX_RPC_BYTES: usize = 1 << 20;

pub mod v1 {
    tonic::include_proto!("ledger.v1");
}
pub mod adapter;
pub mod client;
