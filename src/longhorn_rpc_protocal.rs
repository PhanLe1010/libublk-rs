use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::io::{self, Read, Write};
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::mem;

pub const MAGIC_VERSION: u16 = 0x1b01;

// Define the struct equivalent to the C version
#[repr(C)]
#[derive(Default)]
pub struct MessageHeader {
    pub MagicVersion: u16,
    pub Seq: u32,
    pub Type: u32,
    pub Offset: u64,
    pub Size: u32,
    pub DataLength: u32,
}


pub fn get_message_header_size() -> usize {
    let total_size = mem::size_of::<u16>()   // MagicVersion
        + mem::size_of::<u32>()  // Seq
        + mem::size_of::<u32>()  // Type
        + mem::size_of::<u64>()  // Offset
        + mem::size_of::<u32>()  // Size
        + mem::size_of::<u32>(); // DataLength

    total_size
}

#[repr(u32)]  // Ensure it uses a 32-bit unsigned integer as its underlying type
pub enum MessageType {
    TypeRead = 0,
    TypeWrite = 1,
    TypeResponse = 2,
    TypeError = 3,
    TypeEOF = 4,
    TypeClose = 5,
    TypePing = 6,
    TypeUnmap = 7,
}

pub fn write_header_old(stream: &mut UnixStream, msg: &MessageHeader, header: &mut [u8]) -> io::Result<usize> {
    let mut offset = 0;

    // Convert to little-endian and write into the header buffer
    let MagicVersion = msg.MagicVersion.to_le();
    header[offset..offset + 2].copy_from_slice(&MagicVersion.to_ne_bytes());
    offset += 2;

    let Seq = msg.Seq.to_le();
    header[offset..offset + 4].copy_from_slice(&Seq.to_ne_bytes());
    offset += 4;

    let Type = msg.Type.to_le();
    header[offset..offset + 4].copy_from_slice(&Type.to_ne_bytes());
    offset += 4;

    let Offset = msg.Offset.to_le();
    header[offset..offset + 8].copy_from_slice(&Offset.to_ne_bytes());
    offset += 8;

    let Size = msg.Size.to_le();
    header[offset..offset + 4].copy_from_slice(&Size.to_ne_bytes());
    offset += 4;

    let DataLength = msg.DataLength.to_le();
    header[offset..offset + 4].copy_from_slice(&DataLength.to_ne_bytes());
    offset += 4;

    // Write the header to the file descriptor
    let bytes_written = stream.write_all(&header[..offset])?;
    
    Ok(offset)
}

pub fn write_header(stream: &mut UnixStream, msg: &MessageHeader, header: &mut [u8]) -> io::Result<usize> {
    let mut offset = 0;

    // Use LittleEndian to directly write to the buffer
    LittleEndian::write_u16(&mut header[offset..offset + 2], msg.MagicVersion);
    offset += 2;

    LittleEndian::write_u32(&mut header[offset..offset + 4], msg.Seq);
    offset += 4;

    LittleEndian::write_u32(&mut header[offset..offset + 4], msg.Type);
    offset += 4;

    LittleEndian::write_u64(&mut header[offset..offset + 8], msg.Offset);
    offset += 8;

    LittleEndian::write_u32(&mut header[offset..offset + 4], msg.Size);
    offset += 4;

    LittleEndian::write_u32(&mut header[offset..offset + 4], msg.DataLength);
    offset += 4;

    // Write the header to the UnixStream
    stream.write_all(&header[..offset])?;

    Ok(offset)
}

pub fn read_header(stream: &mut UnixStream, msg: &mut MessageHeader, header: &mut [u8], header_size: usize) -> io::Result<usize> {
    let mut offset = 0;

    // Read the full header from the socket connection
    stream.read_exact(header)?;

    // Parse MagicVersion
    msg.MagicVersion = LittleEndian::read_u16(&header[offset..]);
    offset += std::mem::size_of::<u16>();

    if msg.MagicVersion != MAGIC_VERSION {
        eprintln!(
            "Wrong magic version: 0x{:x}, expected 0x{:x}",
            msg.MagicVersion, MAGIC_VERSION
        );
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Wrong magic version"));
    }

    // Parse Seq
    msg.Seq = LittleEndian::read_u32(&header[offset..]);
    offset += std::mem::size_of::<u32>();

    // Parse Type
    msg.Type = LittleEndian::read_u32(&header[offset..]);
    offset += std::mem::size_of::<u32>();

    // Parse Offset
    let offset_val = LittleEndian::read_u64(&header[offset..]);
    msg.Offset = offset_val;
    offset += std::mem::size_of::<u64>();

    // Parse Size
    msg.Size = LittleEndian::read_u32(&header[offset..]);
    offset += std::mem::size_of::<u32>();

    // Parse DataLength
    msg.DataLength = LittleEndian::read_u32(&header[offset..]);
    offset += std::mem::size_of::<u32>();

    Ok(offset)
}
