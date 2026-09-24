//! The content a publish or a delivery carries: a header frame that says
//! how long the body is, what type it is and whether it persists, then
//! the body frames it announces, each within the frame size the
//! connection settled on.
//!
//! Only the basic class carries content here, and the two properties Xmip
//! writes are content-type and delivery-mode; the other twelve a header
//! may carry are skipped when read and never written.

use std::io::BufRead;

use codec::cursor::Cursor;
use codec::writer::ByteWriter;
use transport::error::{Result, protocol_error};

use crate::wire::{AmqpWrite, Frame, Kind, read_frame};

/// The most a content header may announce before it is refused.
pub const MAX_BODY: usize = 64 * 1024 * 1024;

/// The class a content header names.
const CLASS_BASIC: u16 = 60;
/// content-type at bit 15 and delivery-mode at bit 12 of the flags.
const PROPERTY_FLAGS: u16 = 0x9000;
/// delivery-mode 1: the broker may keep the message in memory only.
const TRANSIENT: u8 = 1;
/// delivery-mode 2: the broker writes the message to disk.
const PERSISTENT: u8 = 2;
const CONTENT_TYPE: &str = "application/octet-stream";

/// `body` as the frames that carry it on `channel`: a content header
/// saying its size, its type and whether it persists, then body frames
/// that fit `frame_max` with their envelope.
#[must_use]
pub fn frames(channel: u16, body: &[u8], frame_max: usize, persistent: bool) -> Vec<Frame> {
    let mut header = Vec::with_capacity(40);
    header
        .u16_be(CLASS_BASIC)
        .u16_be(0)
        .u64_be(u64::try_from(body.len()).unwrap_or(u64::MAX))
        .u16_be(PROPERTY_FLAGS)
        .short_string(CONTENT_TYPE)
        .byte(if persistent { PERSISTENT } else { TRANSIENT });
    let mut frames = vec![Frame::new(Kind::Header, channel, header)];
    let chunk = frame_max.saturating_sub(8).max(1);
    frames.extend(
        body.chunks(chunk)
            .map(|part| Frame::new(Kind::Body, channel, part.to_vec())),
    );
    frames
}

/// The content that follows a publish or a deliver: the size the header
/// announces, then that many bytes of body frames.
///
/// # Errors
/// A header that is not there, a body over [`MAX_BODY`], or one that
/// broke off.
pub fn read(reader: &mut impl BufRead) -> Result<Vec<u8>> {
    let size = match read_frame(reader)? {
        Some(frame) if frame.kind == Kind::Header => body_size_of(&frame)?,
        _ => return Err(protocol_error("content without its header")),
    };
    if size > MAX_BODY {
        return Err(protocol_error(format!(
            "a body of {size} bytes, over the {MAX_BODY} limit"
        )));
    }
    let mut body = Vec::with_capacity(size);
    while body.len() < size {
        match read_frame(reader)? {
            Some(frame) if frame.kind == Kind::Body => body.extend_from_slice(&frame.payload),
            Some(frame) if frame.kind == Kind::Heartbeat => {}
            _ => return Err(protocol_error("a body that broke off")),
        }
    }
    Ok(body)
}

/// The body size a content header announces, after its class and weight.
fn body_size_of(frame: &Frame) -> Result<usize> {
    let mut cursor = Cursor::new(&frame.payload);
    cursor.u16_be()?;
    cursor.u16_be()?;
    usize::try_from(cursor.u64_be()?).map_err(|_| protocol_error("a body too long to hold"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::basic_ack;

    #[test]
    fn content_is_a_header_and_the_body_frames_it_announces() {
        let body = vec![0x2a; 1000];
        let frames = frames(1, &body, 408, true);
        assert_eq!(frames.len(), 4, "a header and three bodies of 400");
        assert_eq!(frames[0].kind, Kind::Header);
        assert_eq!(&frames[0].payload[..4], &[0, 60, 0, 0]);
        assert_eq!(&frames[0].payload[4..12], &1000u64.to_be_bytes());
        assert_eq!(&frames[0].payload[12..14], &PROPERTY_FLAGS.to_be_bytes());
        assert_eq!(*frames[0].payload.last().expect("mode"), PERSISTENT);
        assert_eq!(frames[1].payload.len(), 400);
        let mut wire = Vec::new();
        for frame in &frames {
            wire.extend(frame.encode());
        }
        assert_eq!(read(&mut wire.as_slice()).expect("content"), body);
        let empty = super::frames(1, b"", 16, false);
        assert_eq!(empty.len(), 1, "an empty body is a header alone");
        assert_eq!(*empty[0].payload.last().expect("mode"), TRANSIENT);
        assert!(
            read(&mut empty[0].encode().as_slice())
                .expect("empty")
                .is_empty()
        );
    }

    #[test]
    fn what_is_not_content_is_refused() {
        let method = basic_ack(1).frame(1).encode();
        assert!(read(&mut method.as_slice()).is_err(), "no header");
        let mut cut = Vec::new();
        for frame in &frames(1, b"abc", 16, true)[..1] {
            cut.extend(frame.encode());
        }
        assert!(read(&mut cut.as_slice()).is_err(), "broke off");
        let mut huge = Vec::new();
        huge.u16_be(CLASS_BASIC)
            .u16_be(0)
            .u64_be(u64::MAX)
            .u16_be(0);
        let bytes = Frame::new(Kind::Header, 1, huge).encode();
        assert!(read(&mut bytes.as_slice()).is_err(), "over the limit");
    }
}
