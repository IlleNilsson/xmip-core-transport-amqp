//! AMQP 0-9-1 on the wire: the frame — type, channel, size, payload, and
//! the `0xCE` that ends it — and the argument types a method's payload is
//! written in.

use std::io::Read;

use transport::error::{Result, classify, protocol_error};

/// The protocol header a client opens with.
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";
/// The frame end every frame closes with.
pub const FRAME_END: u8 = 0xce;
/// The frame size this transport proposes and accepts.
pub const FRAME_MAX: u32 = 131_072;

pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;

/// One frame, as its type says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Method {
        channel: u16,
        class: u16,
        method: u16,
        arguments: Vec<u8>,
    },
    Header {
        channel: u16,
        class: u16,
        body_size: u64,
    },
    Body {
        channel: u16,
        bytes: Vec<u8>,
    },
    Heartbeat,
}

impl Frame {
    /// A method frame on `channel`, its arguments already written.
    #[must_use]
    pub fn method(channel: u16, class: u16, method: u16, arguments: Vec<u8>) -> Self {
        Frame::Method {
            channel,
            class,
            method,
            arguments,
        }
    }
}

/// `frame` as bytes on the wire.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let (kind, channel, payload): (u8, u16, Vec<u8>) = match frame {
        Frame::Method {
            channel,
            class,
            method,
            arguments,
        } => {
            let mut payload = Vec::with_capacity(arguments.len() + 4);
            payload.extend_from_slice(&class.to_be_bytes());
            payload.extend_from_slice(&method.to_be_bytes());
            payload.extend_from_slice(arguments);
            (1, *channel, payload)
        }
        Frame::Header {
            channel,
            class,
            body_size,
        } => {
            let mut payload = Vec::with_capacity(14);
            payload.extend_from_slice(&class.to_be_bytes());
            payload.extend_from_slice(&0u16.to_be_bytes());
            payload.extend_from_slice(&body_size.to_be_bytes());
            payload.extend_from_slice(&0u16.to_be_bytes());
            (2, *channel, payload)
        }
        Frame::Body { channel, bytes } => (3, *channel, bytes.clone()),
        Frame::Heartbeat => (8, 0, Vec::new()),
    };
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.push(kind);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(&payload);
    out.push(FRAME_END);
    out
}

/// Read one frame, or `None` when the peer closed between frames.
///
/// # Errors
/// A frame that breaks off, a frame type AMQP does not have, or a payload
/// not closed by the frame end.
pub fn read(reader: &mut impl Read) -> Result<Option<Frame>> {
    let mut head = [0u8; 7];
    let first = reader
        .read(&mut head[..1])
        .map_err(|e| classify("reading a frame header", &e))?;
    if first == 0 {
        return Ok(None);
    }
    if !matches!(head[0], 1 | 2 | 3 | 8) {
        return Err(protocol_error(format!(
            "frame type {} does not exist; the peer does not speak AMQP",
            head[0]
        )));
    }
    reader
        .read_exact(&mut head[1..])
        .map_err(|e| classify("reading a frame header", &e))?;
    let channel = u16::from_be_bytes([head[1], head[2]]);
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]);
    let mut payload = vec![0u8; size as usize];
    reader
        .read_exact(&mut payload)
        .map_err(|e| classify("reading a frame payload", &e))?;
    let mut end = [0u8; 1];
    reader
        .read_exact(&mut end)
        .map_err(|e| classify("reading the frame end", &e))?;
    if end[0] != FRAME_END {
        return Err(protocol_error("a frame not closed by the frame end"));
    }
    let frame = match head[0] {
        1 => {
            let mut args = Reader::new(&payload);
            let class = args.short()?;
            let method = args.short()?;
            Frame::Method {
                channel,
                class,
                method,
                arguments: args.rest().to_vec(),
            }
        }
        2 => {
            let mut fields = Reader::new(&payload);
            let class = fields.short()?;
            fields.short()?;
            let body_size = fields.longlong()?;
            Frame::Header {
                channel,
                class,
                body_size,
            }
        }
        3 => Frame::Body {
            channel,
            bytes: payload,
        },
        8 => Frame::Heartbeat,
        other => return Err(protocol_error(format!("frame type {other} does not exist"))),
    };
    Ok(Some(frame))
}

/// Writes method arguments in AMQP's types.
#[derive(Default)]
pub struct Writer {
    out: Vec<u8>,
    bits: Option<(usize, u8)>,
}

impl Writer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn octet(&mut self, value: u8) -> &mut Self {
        self.bits = None;
        self.out.push(value);
        self
    }

    pub fn short(&mut self, value: u16) -> &mut Self {
        self.bits = None;
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn long(&mut self, value: u32) -> &mut Self {
        self.bits = None;
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn longlong(&mut self, value: u64) -> &mut Self {
        self.bits = None;
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// A short string, at most 255 bytes; longer is cut.
    pub fn shortstr(&mut self, value: &str) -> &mut Self {
        self.bits = None;
        let bytes = &value.as_bytes()[..value.len().min(255)];
        self.out.push(u8::try_from(bytes.len()).unwrap_or(255));
        self.out.extend_from_slice(bytes);
        self
    }

    pub fn longstr(&mut self, value: &[u8]) -> &mut Self {
        self.bits = None;
        self.long(u32::try_from(value.len()).unwrap_or(u32::MAX));
        self.out.extend_from_slice(value);
        self
    }

    /// A field table of long-string values.
    pub fn table(&mut self, entries: &[(&str, &str)]) -> &mut Self {
        self.bits = None;
        let mut inner = Writer::new();
        for (key, value) in entries {
            inner.shortstr(key).octet(b'S').longstr(value.as_bytes());
        }
        self.longstr(&inner.finish())
    }

    /// One bit; consecutive bits pack into one octet, as the grammar says.
    pub fn bit(&mut self, value: bool) -> &mut Self {
        match self.bits {
            Some((at, count)) if count < 8 => {
                if value {
                    self.out[at] |= 1 << count;
                }
                self.bits = Some((at, count + 1));
            }
            _ => {
                self.out.push(u8::from(value));
                self.bits = Some((self.out.len() - 1, 1));
            }
        }
        self
    }

    #[must_use]
    pub fn finish(&self) -> Vec<u8> {
        self.out.clone()
    }
}

/// Reads method arguments in AMQP's types.
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    bits: Option<(u8, u8)>,
}

impl<'a> Reader<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            at: 0,
            bits: None,
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        self.bits = None;
        let end = self.at + count;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| protocol_error("an argument that runs past the frame"))?;
        self.at = end;
        Ok(slice)
    }

    /// # Errors
    /// Past the end.
    pub fn octet(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// # Errors
    /// Past the end.
    pub fn short(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// # Errors
    /// Past the end.
    pub fn long(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// # Errors
    /// Past the end.
    pub fn longlong(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut array = [0u8; 8];
        array.copy_from_slice(b);
        Ok(u64::from_be_bytes(array))
    }

    /// # Errors
    /// Past the end.
    pub fn shortstr(&mut self) -> Result<String> {
        let length = usize::from(self.octet()?);
        Ok(String::from_utf8_lossy(self.take(length)?).into_owned())
    }

    /// # Errors
    /// Past the end.
    pub fn longstr(&mut self) -> Result<&'a [u8]> {
        let length = self.long()? as usize;
        self.take(length)
    }

    /// A field table, skipped: its length is all this transport needs.
    ///
    /// # Errors
    /// Past the end.
    pub fn table(&mut self) -> Result<()> {
        self.longstr().map(|_| ())
    }

    /// # Errors
    /// Past the end.
    pub fn bit(&mut self) -> Result<bool> {
        match self.bits {
            Some((octet, count)) if count < 8 => {
                self.bits = Some((octet, count + 1));
                Ok(octet & (1 << count) != 0)
            }
            _ => {
                let octet = self.take(1)?[0];
                self.bits = Some((octet, 1));
                Ok(octet & 1 != 0)
            }
        }
    }

    #[must_use]
    pub fn rest(&self) -> &'a [u8] {
        &self.bytes[self.at.min(self.bytes.len())..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let mut arguments = Writer::new();
        arguments
            .short(0)
            .shortstr("orders")
            .bit(true)
            .bit(false)
            .bit(true)
            .table(&[("x-key", "value")]);
        let frames = [
            Frame::method(1, CLASS_BASIC, 40, arguments.finish()),
            Frame::Header {
                channel: 1,
                class: CLASS_BASIC,
                body_size: 3,
            },
            Frame::Body {
                channel: 1,
                bytes: vec![1, 2, 3],
            },
            Frame::Heartbeat,
        ];
        let mut wire = Vec::new();
        for frame in &frames {
            wire.extend(encode(frame));
        }
        let mut reader = wire.as_slice();
        for frame in &frames {
            assert_eq!(read(&mut reader).expect("read").as_ref(), Some(frame));
        }
        assert!(read(&mut reader).expect("closed").is_none());
        let Frame::Method { arguments, .. } = &frames[0] else {
            panic!("method");
        };
        let mut args = Reader::new(arguments);
        assert_eq!(args.short().expect("short"), 0);
        assert_eq!(args.shortstr().expect("str"), "orders");
        assert!(args.bit().expect("bit"));
        assert!(!args.bit().expect("bit"));
        assert!(args.bit().expect("bit"));
        args.table().expect("table");
        assert!(args.rest().is_empty());
        assert!(args.octet().is_err(), "past the end");
    }

    #[test]
    fn what_is_not_a_frame_is_refused() {
        assert!(
            read(&mut &[9u8, 0, 0, 0, 0, 0, 0, FRAME_END][..]).is_err(),
            "type 9"
        );
        assert!(
            read(&mut &[1u8, 0, 0, 0, 0, 0, 0, 0][..]).is_err(),
            "no frame end"
        );
        assert!(read(&mut &[1u8, 0, 0][..]).is_err(), "cut off");
        assert_eq!(encode(&Frame::Heartbeat), [8, 0, 0, 0, 0, 0, 0, FRAME_END]);
    }
}
