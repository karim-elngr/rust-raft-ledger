//! POSIX recovery-journal and Apache Kafka adapters for the asynchronous ledger exporter.
#[cfg(feature = "kafka")]
mod event;
#[cfg(feature = "kafka")]
mod kafka;
mod posix;

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ledger.export.v1.rs"));
}
#[cfg(feature = "kafka")]
pub use kafka::KafkaPublisher;
pub use posix::PosixArchive;
