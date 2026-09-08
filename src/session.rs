//! The broker's side of one connection: what a Receive Location that
//! accepts publishers directly runs, and what a test puts at the far end.
//!
//! Not a broker. One session serves one client over one channel with no
//! exchange, no bindings and no queue store: what is published is handed up
//! as a Stream, what is given is delivered to the one consumer. A Location
//! that needs routing, persistence and fan-out talks to a broker through
//! [`crate::Client`].

use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::frame::{
    CLASS_BASIC, CLASS_CHANNEL, CLASS_CONNECTION, CLASS_QUEUE, FRAME_MAX, Frame, PROTOCOL_HEADER,
    Reader, Writer, encode, read,
};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published; here is the Stream.
    Published(Arrived),
    /// The client declared this queue.
    Declared(String),
    /// The client started consuming this queue.
    Consuming(String),
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    user: String,
    consumer_tag: Option<String>,
    delivery_tag: u64,
}

impl Session {
    /// Accept one client on `listener` and complete its handshake.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client did not
    /// open with the protocol header and the handshake.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut session = Self {
            reader,
            writer,
            peer,
            user: String::new(),
            consumer_tag: None,
            delivery_tag: 0,
        };
        let mut header = [0u8; 8];
        std::io::Read::read_exact(&mut session.reader, &mut header)
            .map_err(|e| classify("reading the protocol header", &e))?;
        if header != PROTOCOL_HEADER {
            session.write_raw(&PROTOCOL_HEADER)?;
            return Err(protocol_error("the client did not open with AMQP 0-9-1"));
        }
        let mut start = Writer::new();
        start
            .octet(0)
            .octet(9)
            .table(&[("product", "xmip")])
            .longstr(b"PLAIN")
            .longstr(b"en_US");
        session.send(&Frame::method(0, CLASS_CONNECTION, 10, start.finish()))?;
        let start_ok = session.expect(0, CLASS_CONNECTION, 11, "connection.start-ok")?;
        let mut fields = Reader::new(&start_ok);
        fields.table()?;
        fields.shortstr()?;
        let response = fields.longstr()?;
        session.user = String::from_utf8_lossy(response)
            .split('\0')
            .nth(1)
            .unwrap_or("")
            .to_string();
        let mut tune = Writer::new();
        tune.short(1).long(FRAME_MAX).short(0);
        session.send(&Frame::method(0, CLASS_CONNECTION, 30, tune.finish()))?;
        session.expect(0, CLASS_CONNECTION, 31, "connection.tune-ok")?;
        session.expect(0, CLASS_CONNECTION, 40, "connection.open")?;
        let mut open_ok = Writer::new();
        open_ok.shortstr("");
        session.send(&Frame::method(0, CLASS_CONNECTION, 41, open_ok.finish()))?;
        Ok(session)
    }

    /// Who connected, as the PLAIN response named them.
    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    /// The next message the client publishes, or `None` when it closed.
    /// Channels, declarations and consumers are answered on the way.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_event()? {
                Some(Event::Published(arrived)) => return Ok(Some(arrived)),
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the client sent what this session does not serve.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let Some(frame) = read(&mut self.reader)? else {
                return Ok(None);
            };
            let Frame::Method {
                channel,
                class,
                method,
                arguments,
            } = frame
            else {
                if frame == Frame::Heartbeat {
                    self.send(&Frame::Heartbeat)?;
                }
                continue;
            };
            let mut fields = Reader::new(&arguments);
            match (class, method) {
                (CLASS_CHANNEL, 10) => {
                    let mut ok = Writer::new();
                    ok.longstr(b"");
                    self.send(&Frame::method(channel, CLASS_CHANNEL, 11, ok.finish()))?;
                }
                (CLASS_CHANNEL, 40) => {
                    self.send(&Frame::method(channel, CLASS_CHANNEL, 41, Vec::new()))?;
                }
                (CLASS_QUEUE, 10) => {
                    fields.short()?;
                    let queue = fields.shortstr()?;
                    let mut ok = Writer::new();
                    ok.shortstr(&queue).long(0).long(0);
                    self.send(&Frame::method(channel, CLASS_QUEUE, 11, ok.finish()))?;
                    return Ok(Some(Event::Declared(queue)));
                }
                (CLASS_BASIC, 20) => {
                    fields.short()?;
                    let queue = fields.shortstr()?;
                    let tag = fields.shortstr()?;
                    let tag = if tag.is_empty() {
                        "xmip.1".to_string()
                    } else {
                        tag
                    };
                    let mut ok = Writer::new();
                    ok.shortstr(&tag);
                    self.send(&Frame::method(channel, CLASS_BASIC, 21, ok.finish()))?;
                    self.consumer_tag = Some(tag);
                    return Ok(Some(Event::Consuming(queue)));
                }
                (CLASS_BASIC, 40) => {
                    fields.short()?;
                    let exchange = fields.shortstr()?;
                    let routing_key = fields.shortstr()?;
                    let body = self.body()?;
                    let origin = format!("amqp://{}/{exchange}/{routing_key}", self.peer);
                    return Ok(Some(Event::Published(Arrived::new(origin, body))));
                }
                (CLASS_BASIC, 80) => {}
                (CLASS_CONNECTION, 50) => {
                    self.send(&Frame::method(0, CLASS_CONNECTION, 51, Vec::new()))?;
                    return Ok(None);
                }
                (class, method) => {
                    return Err(protocol_error(format!(
                        "method {class}.{method} is not served here"
                    )));
                }
            }
        }
    }

    /// Deliver `body` to the consumer under `routing_key`.
    ///
    /// # Errors
    /// Where no consumer was started, or the client went away.
    pub fn deliver(&mut self, routing_key: &str, body: &[u8]) -> Result<()> {
        let tag = self
            .consumer_tag
            .clone()
            .ok_or_else(|| protocol_error("no consumer to deliver to"))?;
        self.delivery_tag += 1;
        let mut deliver = Writer::new();
        deliver
            .shortstr(&tag)
            .longlong(self.delivery_tag)
            .bit(false)
            .shortstr("")
            .shortstr(routing_key);
        self.send(&Frame::method(1, CLASS_BASIC, 60, deliver.finish()))?;
        self.send(&Frame::Header {
            channel: 1,
            class: CLASS_BASIC,
            body_size: body.len() as u64,
        })?;
        for chunk in body.chunks(FRAME_MAX as usize - 8) {
            self.send(&Frame::Body {
                channel: 1,
                bytes: chunk.to_vec(),
            })?;
        }
        Ok(())
    }

    fn body(&mut self) -> Result<Vec<u8>> {
        let Some(Frame::Header { body_size, .. }) = read(&mut self.reader)? else {
            return Err(protocol_error("a publish without its content header"));
        };
        let mut body = Vec::with_capacity(usize::try_from(body_size).unwrap_or(0));
        while (body.len() as u64) < body_size {
            match read(&mut self.reader)? {
                Some(Frame::Body { bytes, .. }) => body.extend_from_slice(&bytes),
                _ => return Err(protocol_error("a body that broke off")),
            }
        }
        Ok(body)
    }

    fn expect(&mut self, channel: u16, class: u16, method: u16, what: &str) -> Result<Vec<u8>> {
        match read(&mut self.reader)? {
            Some(Frame::Method {
                channel: c,
                class: cl,
                method: m,
                arguments,
            }) if c == channel && cl == class && m == method => Ok(arguments),
            Some(other) => Err(protocol_error(format!(
                "{other:?} where {what} was expected"
            ))),
            None => Err(protocol_error(format!("the client closed before {what}"))),
        }
    }

    fn send(&mut self, frame: &Frame) -> Result<()> {
        self.write_raw(&encode(frame))
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| classify("writing a frame", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a frame", &e))
    }
}
