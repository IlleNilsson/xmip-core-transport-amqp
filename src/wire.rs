//! AMQP 0-9-1 on the wire: the frame envelope, and the value encodings
//! every method argument is built from.
//!
//! A frame is a type octet, a channel short, a length long, that many
//! bytes of payload, and the frame-end octet `0xCE`. Everything inside is
//! big-endian, read off codec's byte cursor and written with codec's byte
//! writer. What is AMQP's own is here: strings come in two lengths — a
//! short one counted by an octet, a long one counted by a long — and a
//! field table is a long length followed by short-string keys each tagged
//! with the type of the value that follows. [`Amqp`] reads them off the
//! cursor, [`AmqpWrite`] writes them.
//!
//! Nothing here knows what a method means; that is `method.rs`.

use std::io::BufRead;

use codec::cursor::Cursor;
use codec::writer::ByteWriter;
use net::ceiling;
use transport::error::{Result, classify, protocol_error};

/// The octet every frame ends with.
const FRAME_END: u8 = 0xCE;

/// The largest frame Xmip will read, and what it asks for in tune-ok.
pub const MAX_FRAME: usize = 1024 * 1024;

/// The protocol header a connection opens with: `AMQP`, then 0, 0, 9, 1.
pub const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

/// What a frame carries, by its type octet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A class and method id, and the arguments they define. Type 1.
    Method,
    /// A class, a body size and the properties. Type 2.
    Header,
    /// Part of a message body. Type 3.
    Body,
    /// A heartbeat. Type 8.
    Heartbeat,
}

impl Kind {
    /// The type octet on the wire.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Method => 1,
            Self::Header => 2,
            Self::Body => 3,
            Self::Heartbeat => 8,
        }
    }

    /// What `octet` says the frame carries.
    #[must_use]
    pub const fn of(octet: u8) -> Option<Self> {
        match octet {
            1 => Some(Self::Method),
            2 => Some(Self::Header),
            3 => Some(Self::Body),
            8 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

/// One frame: what it carries, on which channel, and its payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub kind: Kind,
    pub channel: u16,
    pub payload: Vec<u8>,
}

impl Frame {
    /// A frame of `kind` on `channel`.
    #[must_use]
    pub fn new(kind: Kind, channel: u16, payload: Vec<u8>) -> Self {
        Self {
            kind,
            channel,
            payload,
        }
    }

    /// The frame as bytes, frame-end included.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.payload.len() + 8);
        out.byte(self.kind.octet())
            .u16_be(self.channel)
            .u32_be(len32(self.payload.len()))
            .bytes(&self.payload)
            .byte(FRAME_END);
        out
    }
}

/// Read one frame, or `None` when the peer closed between frames.
///
/// # Errors
/// A type octet AMQP does not define, a frame over [`MAX_FRAME`], a frame
/// that does not end in `0xCE`, or a connection that broke.
pub fn read_frame(reader: &mut impl BufRead) -> Result<Option<Frame>> {
    let Some(head) = net::read::header::<7>(reader, "a frame header")? else {
        return Ok(None);
    };
    let mut cursor = Cursor::new(&head);
    let octet = cursor.byte()?;
    let kind = Kind::of(octet)
        .ok_or_else(|| protocol_error(format!("a frame type AMQP does not define: {octet}")))?;
    let channel = cursor.u16_be()?;
    let size = cursor.u32_be()? as usize;
    ceiling::within(size, MAX_FRAME, "Xmip reads in one frame")?;
    let mut payload = vec![0u8; size + 1];
    reader
        .read_exact(&mut payload)
        .map_err(|e| classify("reading a frame payload", &e))?;
    if payload.pop() != Some(FRAME_END) {
        return Err(protocol_error("a frame that does not end in 0xCE"));
    }
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

/// A length as the wire writes it, saturating rather than wrapping: a
/// payload longer than a `u32` cannot be framed and is refused where it is
/// built.
#[must_use]
pub fn len32(length: usize) -> u32 {
    u32::try_from(length).unwrap_or(u32::MAX)
}

/// AMQP's own shapes, read off codec's cursor in the order a method
/// defines them. Octets, shorts, longs and long longs are the cursor's
/// `byte`, `u16_be`, `u32_be` and `u64_be`.
pub trait Amqp<'a> {
    /// A short string — an octet of length, then the bytes — as text.
    ///
    /// # Errors
    /// Where the payload has run out or the bytes are not UTF-8.
    fn short_string(&mut self) -> Result<String>;

    /// A long string — a long of length, then the bytes — as bytes.
    ///
    /// # Errors
    /// Where the payload has run out.
    fn long_string(&mut self) -> Result<&'a [u8]>;

    /// A field table, skipped: Xmip reads none of what a broker puts in
    /// its server properties, and skipping it is the whole of the need.
    ///
    /// # Errors
    /// Where the payload has run out.
    fn skip_table(&mut self) -> Result<()>;
}

impl<'a> Amqp<'a> for Cursor<'a> {
    fn short_string(&mut self) -> Result<String> {
        let length = self.byte()? as usize;
        text(self.take(length)?)
    }

    fn long_string(&mut self) -> Result<&'a [u8]> {
        let length = self.u32_be()? as usize;
        Ok(self.take(length)?)
    }

    fn skip_table(&mut self) -> Result<()> {
        let length = self.u32_be()? as usize;
        Ok(self.skip(length)?)
    }
}

/// AMQP's own shapes, written beside codec's [`ByteWriter`]; every method
/// returns the buffer so calls chain with the writer's.
pub trait AmqpWrite {
    /// A short string: an octet of length, then the bytes. Longer than 255
    /// bytes is truncated, which only a malformed name could be.
    fn short_string(&mut self, value: &str) -> &mut Self;

    /// A long string: a long of length, then the bytes.
    fn long_string(&mut self, value: &[u8]) -> &mut Self;

    /// An empty field table — a long zero. [`table`] reads the ones a
    /// broker sends.
    fn empty_table(&mut self) -> &mut Self;

    /// A field table of long-string values: the properties each side
    /// presents.
    fn string_table(&mut self, entries: &[(&str, &str)]) -> &mut Self;
}

impl AmqpWrite for Vec<u8> {
    fn short_string(&mut self, value: &str) -> &mut Self {
        let bytes = value.as_bytes();
        let length = u8::try_from(bytes.len()).unwrap_or(u8::MAX);
        self.byte(length).bytes(&bytes[..length as usize])
    }

    fn long_string(&mut self, value: &[u8]) -> &mut Self {
        self.u32_be(len32(value.len())).bytes(value)
    }

    fn empty_table(&mut self) -> &mut Self {
        self.u32_be(0)
    }

    fn string_table(&mut self, entries: &[(&str, &str)]) -> &mut Self {
        let mut inner = Vec::new();
        for (key, value) in entries {
            inner
                .short_string(key)
                .byte(b'S')
                .long_string(value.as_bytes());
        }
        self.long_string(&inner)
    }
}

fn text(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| protocol_error("a string that is not UTF-8"))
}

/// A field table's common types, read as text so a diagnostic can print
/// them: `S` long string, `I` long int, `t` boolean, `F` nested table, `V`
/// nothing. Anything else ends the reading, because the length of an
/// unknown type is unknown.
///
/// # Errors
/// Where the table ends mid-value.
pub fn table(bytes: &[u8]) -> Result<Vec<(String, String)>> {
    let mut cursor = Cursor::new(bytes);
    let mut pairs = Vec::new();
    while !cursor.is_empty() {
        let key = cursor.short_string()?;
        let value = match cursor.byte()? {
            b'S' => text(cursor.long_string()?)?,
            b'I' => cursor.i32_be()?.to_string(),
            b't' => (cursor.byte()? != 0).to_string(),
            b'F' => format!("{:?}", table(cursor.long_string()?)?),
            b'V' => String::new(),
            _ => break,
        };
        pairs.push((key, value));
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips_through_its_envelope() {
        let frame = Frame::new(Kind::Method, 1, vec![0, 10, 0, 11]);
        let bytes = frame.encode();
        assert_eq!(bytes[0], 1);
        assert_eq!(&bytes[1..3], &[0, 1]);
        assert_eq!(&bytes[3..7], &[0, 0, 0, 4]);
        assert_eq!(*bytes.last().expect("frame end"), FRAME_END);
        let back = read_frame(&mut bytes.as_slice())
            .expect("read")
            .expect("one");
        assert_eq!(back, frame);
        assert!(read_frame(&mut &b""[..]).expect("closed").is_none());
        assert_eq!(Kind::of(8), Some(Kind::Heartbeat));
        assert_eq!(Kind::Body.octet(), 3);
    }

    #[test]
    fn what_is_not_amqp_is_refused() {
        assert!(
            read_frame(&mut &b"AMQP\x00\x00\x09\x01"[..]).is_err(),
            "type"
        );
        let mut bad_end = Frame::new(Kind::Body, 1, vec![7]).encode();
        *bad_end.last_mut().expect("last") = 0;
        assert!(read_frame(&mut bad_end.as_slice()).is_err(), "frame end");
        let huge = [3u8, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(read_frame(&mut &huge[..]).is_err(), "over the limit");
        let short = [1u8, 0, 1, 0, 0, 0, 4, 0];
        assert!(read_frame(&mut &short[..]).is_err(), "ends mid-payload");
    }

    #[test]
    fn every_value_encoding_round_trips() {
        let mut out = Vec::new();
        out.byte(7)
            .u16_be(4096)
            .u32_be(131_072)
            .u64_be(u64::MAX)
            .short_string("orders")
            .long_string(b"a longer one")
            .empty_table();
        let mut cursor = Cursor::new(&out);
        assert_eq!(cursor.byte().expect("octet"), 7);
        assert_eq!(cursor.u16_be().expect("short"), 4096);
        assert_eq!(cursor.u32_be().expect("long"), 131_072);
        assert_eq!(cursor.u64_be().expect("longlong"), u64::MAX);
        assert_eq!(cursor.short_string().expect("short string"), "orders");
        assert_eq!(cursor.long_string().expect("long string"), b"a longer one");
        cursor.skip_table().expect("table");
        assert!(cursor.is_empty());
        let error = cursor.long_string().expect_err("nothing left");
        assert!(error.message.contains("runs past"), "{}", error.message);
        assert!(!error.retryable);
        assert_eq!(len32(usize::MAX), u32::MAX);
        let mut long_name = Vec::new();
        long_name.short_string(&"x".repeat(300));
        assert_eq!(long_name.len(), 256, "a short string stops at 255 bytes");
    }

    #[test]
    fn a_field_table_reads_the_types_a_broker_sends() {
        let mut inner = Vec::new();
        inner.short_string("publish").byte(b't').byte(1);
        let mut out = Vec::new();
        out.short_string("product")
            .byte(b'S')
            .long_string(b"RabbitMQ")
            .short_string("channel_max")
            .byte(b'I')
            .i32_be(2047)
            .short_string("capabilities")
            .byte(b'F')
            .long_string(&inner)
            .short_string("nothing")
            .byte(b'V');
        let pairs = table(&out).expect("table");
        assert_eq!(pairs[0], ("product".into(), "RabbitMQ".into()));
        assert_eq!(pairs[1], ("channel_max".into(), "2047".into()));
        assert!(pairs[2].1.contains("publish"));
        assert_eq!(pairs[3], ("nothing".into(), String::new()));
        let mut unknown = Vec::new();
        unknown.short_string("x").byte(b'?');
        assert!(table(&unknown).expect("stops").is_empty());
        let mut written = Vec::new();
        written.string_table(&[("product", "xmip")]);
        let written = Cursor::new(&written).long_string().expect("table");
        assert_eq!(
            table(written).expect("read back"),
            vec![("product".to_string(), "xmip".to_string())]
        );
    }
}
