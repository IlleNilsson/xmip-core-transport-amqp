//! The content a publish or a delivery carries: a header frame that says
//! how long the body is and what its properties are, then the body frames
//! it announces, each within the frame size the connection settled on.
//!
//! Only the basic class carries content here, and of the fourteen
//! properties a header may carry Xmip writes and reads three:
//! content-type, the headers field table and delivery-mode
//! ([`Properties`]). The rest are skipped when read and never written. The
//! headers table is where the event capability's wire event attributes
//! travel (`event_wire.rs`); until 2026-09-26 a header said only
//! `application/octet-stream` and delivery-mode, and nothing of what a
//! delivery's header said was read.

use std::io::BufRead;

use codec::cursor::Cursor;
use codec::writer::ByteWriter;
use net::MAX_BODY;
use transport::ceiling;
use transport::error::{Result, protocol_error};

use crate::wire::{Amqp, AmqpWrite, Frame, Kind, read_frame, table};

/// The class a content header names.
const CLASS_BASIC: u16 = 60;
/// The property flags, first bit highest: content-type is bit 15,
/// content-encoding 14, headers 13, delivery-mode 12.
const HAS_CONTENT_TYPE: u16 = 1 << 15;
const HAS_CONTENT_ENCODING: u16 = 1 << 14;
const HAS_HEADERS: u16 = 1 << 13;
const HAS_DELIVERY_MODE: u16 = 1 << 12;
/// delivery-mode 1: the broker may keep the message in memory only.
const TRANSIENT: u8 = 1;
/// delivery-mode 2: the broker writes the message to disk.
const PERSISTENT: u8 = 2;
/// What a Stream is published as: bytes.
pub const OCTETS: &str = "application/octet-stream";

/// The properties of one message that Xmip writes and reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties {
    pub content_type: Option<String>,
    /// The headers field table, each value as text: written as long
    /// strings, read from the types [`table`] reads.
    pub headers: Vec<(String, String)>,
    /// delivery-mode 2 rather than 1.
    pub persistent: bool,
}

impl Properties {
    /// A Stream's: bytes, persistent or not, no headers.
    #[must_use]
    pub fn octets(persistent: bool) -> Self {
        Self {
            content_type: Some(OCTETS.to_string()),
            headers: Vec::new(),
            persistent,
        }
    }

    /// The property flags and the properties, as a content header writes
    /// them after the body size.
    fn write(&self, out: &mut Vec<u8>) {
        let mut flags = HAS_DELIVERY_MODE;
        if self.content_type.is_some() {
            flags |= HAS_CONTENT_TYPE;
        }
        if !self.headers.is_empty() {
            flags |= HAS_HEADERS;
        }
        out.u16_be(flags);
        if let Some(content_type) = &self.content_type {
            out.short_string(content_type);
        }
        if !self.headers.is_empty() {
            let entries: Vec<(&str, &str)> = self
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect();
            out.string_table(&entries);
        }
        out.byte(if self.persistent {
            PERSISTENT
        } else {
            TRANSIENT
        });
    }

    /// The properties a content header carries after its body size.
    fn read(cursor: &mut Cursor<'_>) -> Result<Self> {
        let flags = cursor.u16_be()?;
        let mut properties = Self::default();
        if flags & HAS_CONTENT_TYPE != 0 {
            properties.content_type = Some(cursor.short_string()?);
        }
        if flags & HAS_CONTENT_ENCODING != 0 {
            cursor.short_string()?;
        }
        if flags & HAS_HEADERS != 0 {
            properties.headers = table(cursor.long_string()?)?;
        }
        if flags & HAS_DELIVERY_MODE != 0 {
            properties.persistent = cursor.byte()? == PERSISTENT;
        }
        Ok(properties)
    }
}

/// `body` as the frames that carry it on `channel`: a content header
/// saying its size and its `properties`, then body frames that fit
/// `frame_max` with their envelope.
#[must_use]
pub fn frames(channel: u16, properties: &Properties, body: &[u8], frame_max: usize) -> Vec<Frame> {
    let mut header = Vec::with_capacity(40);
    header
        .u16_be(CLASS_BASIC)
        .u16_be(0)
        .u64_be(u64::try_from(body.len()).unwrap_or(u64::MAX));
    properties.write(&mut header);
    let mut frames = vec![Frame::new(Kind::Header, channel, header)];
    let chunk = frame_max.saturating_sub(8).max(1);
    frames.extend(
        body.chunks(chunk)
            .map(|part| Frame::new(Kind::Body, channel, part.to_vec())),
    );
    frames
}

/// The content that follows a publish or a deliver: the properties and
/// the size the header announces, then that many bytes of body frames.
///
/// # Errors
/// A header that is not there or ends inside its properties, a body over
/// [`MAX_BODY`], or one that broke off.
pub fn read(reader: &mut impl BufRead) -> Result<(Properties, Vec<u8>)> {
    let (properties, size) = match read_frame(reader)? {
        Some(frame) if frame.kind == Kind::Header => header_of(&frame)?,
        _ => return Err(protocol_error("content without its header")),
    };
    ceiling::within(size, MAX_BODY, "Xmip reads in one body")?;
    let mut body = Vec::with_capacity(size);
    while body.len() < size {
        match read_frame(reader)? {
            Some(frame) if frame.kind == Kind::Body => body.extend_from_slice(&frame.payload),
            Some(frame) if frame.kind == Kind::Heartbeat => {}
            _ => return Err(protocol_error("a body that broke off")),
        }
    }
    Ok((properties, body))
}

/// The properties and the body size a content header announces, after its
/// class and weight.
fn header_of(frame: &Frame) -> Result<(Properties, usize)> {
    let mut cursor = Cursor::new(&frame.payload);
    cursor.u16_be()?;
    cursor.u16_be()?;
    let size =
        usize::try_from(cursor.u64_be()?).map_err(|_| protocol_error("a body too long to hold"))?;
    Ok((Properties::read(&mut cursor)?, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::basic_ack;

    fn wire(frames: &[Frame]) -> Vec<u8> {
        frames.iter().flat_map(Frame::encode).collect()
    }

    #[test]
    fn content_is_a_header_and_the_body_frames_it_announces() {
        let body = vec![0x2a; 1000];
        let frames = frames(1, &Properties::octets(true), &body, 408);
        assert_eq!(frames.len(), 4, "a header and three bodies of 400");
        assert_eq!(frames[0].kind, Kind::Header);
        assert_eq!(&frames[0].payload[..4], &[0, 60, 0, 0]);
        assert_eq!(&frames[0].payload[4..12], &1000u64.to_be_bytes());
        let flags = HAS_CONTENT_TYPE | HAS_DELIVERY_MODE;
        assert_eq!(&frames[0].payload[12..14], &flags.to_be_bytes());
        assert_eq!(*frames[0].payload.last().expect("mode"), PERSISTENT);
        assert_eq!(frames[1].payload.len(), 400);
        let (properties, taken) = read(&mut wire(&frames).as_slice()).expect("content");
        assert_eq!((properties, taken), (Properties::octets(true), body));
        let empty = super::frames(1, &Properties::octets(false), b"", 16);
        assert_eq!(empty.len(), 1, "an empty body is a header alone");
        assert_eq!(*empty[0].payload.last().expect("mode"), TRANSIENT);
        let (_, body) = read(&mut empty[0].encode().as_slice()).expect("empty");
        assert!(body.is_empty());
    }

    #[test]
    fn the_content_type_and_the_headers_table_travel_with_the_body() {
        let properties = Properties {
            content_type: Some("application/cloudevents+json; charset=UTF-8".to_string()),
            headers: vec![
                ("cloudEvents_id".to_string(), "a b%".to_string()),
                (
                    "cloudEvents_type".to_string(),
                    "se.xmip.send.failure".to_string(),
                ),
            ],
            persistent: true,
        };
        let frames = frames(1, &properties, b"{}", 4096);
        let (taken, body) = read(&mut wire(&frames).as_slice()).expect("content");
        assert_eq!((taken, body), (properties, b"{}".to_vec()));
        let bare = Properties::default();
        let (taken, _) =
            read(&mut wire(&super::frames(1, &bare, b"x", 64)).as_slice()).expect("bare");
        assert_eq!(taken, bare, "no content type, no headers, transient");
    }

    #[test]
    fn what_is_not_content_is_refused() {
        let method = basic_ack(1).frame(1).encode();
        assert!(read(&mut method.as_slice()).is_err(), "no header");
        let cut = wire(&frames(1, &Properties::octets(true), b"abc", 16)[..1]);
        assert!(read(&mut cut.as_slice()).is_err(), "broke off");
        let mut huge = Vec::new();
        huge.u16_be(CLASS_BASIC)
            .u16_be(0)
            .u64_be(u64::MAX)
            .u16_be(0);
        let bytes = Frame::new(Kind::Header, 1, huge).encode();
        assert!(read(&mut bytes.as_slice()).is_err(), "over the limit");
        let mut short = Vec::new();
        short
            .u16_be(CLASS_BASIC)
            .u16_be(0)
            .u64_be(0)
            .u16_be(HAS_HEADERS);
        let bytes = Frame::new(Kind::Header, 1, short).encode();
        assert!(
            read(&mut bytes.as_slice()).is_err(),
            "headers announced, not there"
        );
    }
}
