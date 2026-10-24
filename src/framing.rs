use crate::error::IpcError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub const MAGIC: [u8; 4] = *b"K3CI";
pub const CURRENT_VERSION: u16 = 1;

// Message Types
pub const MSG_TYPE_WORKER_REGISTRATION: u16 = 0x01;
pub const MSG_TYPE_WORKER_HEARTBEAT: u16    = 0x02;
pub const MSG_TYPE_STAGE_DISPATCH: u16      = 0x03;
pub const MSG_TYPE_STAGE_COMPLETION: u16    = 0x04;
pub const MSG_TYPE_STAGE_CANCEL: u16        = 0x05;
pub const MSG_TYPE_ARROW_LOG: u16           = 0x10;
pub const MSG_TYPE_ARROW_METRIC: u16        = 0x11;

// Flags
pub const FLAG_COMPRESSED: u32    = 0x01;
pub const FLAG_END_OF_STREAM: u32 = 0x02;

pub const HEADER_SIZE: usize = std::mem::size_of::<IpcHeader>();

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(C)]
pub struct IpcHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub msg_type: u16,
    pub payload_len: u32,
    pub flags: u32,
    pub sequence: u64,
}

impl IpcHeader {
    pub fn new(msg_type: u16, payload_len: u32, flags: u32, sequence: u64) -> Self {
        Self {
            magic: MAGIC,
            version: CURRENT_VERSION,
            msg_type,
            payload_len,
            flags,
            sequence,
        }
    }

    pub fn parse(buf: &[u8]) -> Result<(&Self, &[u8]), IpcError> {
        if buf.len() < HEADER_SIZE {
            return Err(IpcError::BufferTooSmall {
                expected: HEADER_SIZE,
                actual: buf.len(),
            });
        }

        let (hdr, rest) = Self::ref_from_prefix(buf)
            .map_err(|e| IpcError::MalformedHeader(format!("{:?}", e)))?;

        if hdr.magic != MAGIC {
            return Err(IpcError::InvalidMagic);
        }

        Ok((hdr, rest))
    }
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(IpcHeader, Vec<u8>), IpcError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header_buf).await?;

    let (header, _) = IpcHeader::parse(&header_buf)?;
    let header_copy = *header;

    let payload_len = header_copy.payload_len as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok((header_copy, payload))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: &IpcHeader,
    payload: &[u8],
) -> Result<(), IpcError> {
    writer.write_all(header.as_bytes()).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    writer.flush().await?;
    Ok(())
}
