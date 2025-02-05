use thiserror::Error;

#[derive(Error, Debug)]
pub enum PikachuError {
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),
    
    #[error("Chord protocol error: {0}")]
    Chord(#[from] ChordError),
    
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("Message error: {0}")]
    Message(#[from] MessageError),
}

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Peer unreachable: {0}")]
    PeerUnreachable(String),
}

#[derive(Error, Debug)]
pub enum ChordError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    
    #[error("Node already exists: {0}")]
    NodeExists(String),
    
    #[error("Invalid node ID: {0}")]
    InvalidNodeId(String),
    
    #[error("Failed to join ring: {0}")]
    JoinFailed(String),
    
    #[error("Stabilization failed: {0}")]
    StabilizationFailed(String),
    
    #[error("Finger table update failed: {0}")]
    FingerUpdateFailed(String),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Failed to store value: {0}")]
    StorageFailed(String),
    
    #[error("Data corruption: {0}")]
    DataCorruption(String),
    
    #[error("Replication failed: {0}")]
    ReplicationFailed(String),
}

#[derive(Error, Debug)]
pub enum MessageError {
    #[error("Failed to serialize message: {0}")]
    SerializationFailed(String),
    
    #[error("Failed to deserialize message: {0}")]
    DeserializationFailed(String),
    
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),
    
    #[error("Message timeout")]
    Timeout,
}