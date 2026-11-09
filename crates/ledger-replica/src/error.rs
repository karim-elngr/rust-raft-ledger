#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    SizeLimit(&'static str),
    #[error("this voter is not the leader")]
    NotLeader,
    #[error("journal verification anchor changed since comparison began")]
    JournalAnchorChanged,
    #[error("leader must commit an entry in its current term before issuing a read barrier")]
    ReadNotReady,
    #[error("the voter stopped after a persistence or apply failure; reopen it to recover")]
    Stopped,
    #[error("replica filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("RocksDB: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("Raft: {0}")]
    Consensus(#[from] raft::Error),
    #[error("Raft record encoding: {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("local record encoding: {0}")]
    Encoding(#[from] Box<bincode::ErrorKind>),
}
