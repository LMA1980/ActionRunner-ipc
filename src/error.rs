use thiserror::Error;

#[derive(Error, Debug)]
pub enum IpcError {
    #[error("Invalid magic bytes in IPC header (expected b'K3CI')")]
    InvalidMagic,

    #[error("Unsupported IPC protocol version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u16, actual: u16 },

    #[error("IPC frame payload too large: {actual} bytes exceeds maximum allowed {max} bytes")]
    FrameTooLarge { max: usize, actual: usize },

    #[error("Header length mismatch or malformed packet: {0}")]
    MalformedHeader(String),

    #[error("Buffer too small: expected at least {expected} bytes, got {actual}")]
    BufferTooSmall { expected: usize, actual: usize },

    #[error("Apache Fory serialization error: {0}")]
    Fory(#[from] fory::Error),

    #[error("Apache Arrow IPC error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("I/O error during IPC transmission: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unexpected IPC message type: 0x{0:02x}")]
    UnexpectedMessageType(u16),

    #[error("Unknown control payload type ID: {0}")]
    UnknownControlTypeId(u32),
}
