//! The client's side of one connection to a broker: the handshake, one
//! channel, publish, consume, deliver.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::frame::{
    CLASS_BASIC, CLASS_CHANNEL, CLASS_CONNECTION, CLASS_QUEUE, FRAME_MAX, Frame, PROTOCOL_HEADER,
    Reader, Writer, encode, read,
};

/// What a Location presents when it connects.
#[derive(Clone, Debug)]
pub struct Login {
    pub user: String,
    pub password: String,
    pub virtual_host: String,
}

impl Default for Login {
    fn default() -> Self {
        Self {
            user: "guest".to_string(),
            password: "guest".to_string(),
            virtual_host: "/".to_string(),
        }
    }
}

/// One open connection with channel 1 open on it.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    broker: String,
    frame_max: usize,
    consumer_tag: Option<String>,
}

impl Client {
    /// Connect to `broker` and complete the connection and channel handshake.
    ///
    /// # Errors
    /// Where the broker could not be reached, refused the login, or did not
    /// speak AMQP 0-9-1.
    pub fn connect(broker: &str, login: &Login, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(broker, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            broker: broker.to_string(),
            frame_max: FRAME_MAX as usize,
            consumer_tag: None,
        };
        client.write_raw(&PROTOCOL_HEADER)?;
        client.expect(0, CLASS_CONNECTION, 10, "connection.start")?;
        let mut start_ok = Writer::new();
        let response = format!("\0{}\0{}", login.user, login.password);
        start_ok
            .table(&[("product", "xmip")])
            .shortstr("PLAIN")
            .longstr(response.as_bytes())
            .shortstr("en_US");
        client.send(&Frame::method(0, CLASS_CONNECTION, 11, start_ok.finish()))?;
        let tune = client.expect(0, CLASS_CONNECTION, 30, "connection.tune")?;
        let mut fields = Reader::new(&tune);
        let channel_max = fields.short()?;
        let frame_max = fields.long()?;
        let frame_max = if frame_max == 0 {
            FRAME_MAX
        } else {
            frame_max.min(FRAME_MAX)
        };
        client.frame_max = frame_max as usize;
        let mut tune_ok = Writer::new();
        tune_ok.short(channel_max).long(frame_max).short(0);
        client.send(&Frame::method(0, CLASS_CONNECTION, 31, tune_ok.finish()))?;
        let mut open = Writer::new();
        open.shortstr(&login.virtual_host).shortstr("").bit(false);
        client.send(&Frame::method(0, CLASS_CONNECTION, 40, open.finish()))?;
        client.expect(0, CLASS_CONNECTION, 41, "connection.open-ok")?;
        let mut channel_open = Writer::new();
        channel_open.shortstr("");
        client.send(&Frame::method(1, CLASS_CHANNEL, 10, channel_open.finish()))?;
        client.expect(1, CLASS_CHANNEL, 11, "channel.open-ok")?;
        Ok(client)
    }

    /// Declare `queue`, durable; how many messages it holds.
    ///
    /// # Errors
    /// Where the broker refused the declaration.
    pub fn declare_queue(&mut self, queue: &str) -> Result<u32> {
        let mut declare = Writer::new();
        declare
            .short(0)
            .shortstr(queue)
            .bit(false)
            .bit(true)
            .bit(false)
            .bit(false)
            .bit(false)
            .table(&[]);
        self.send(&Frame::method(1, CLASS_QUEUE, 10, declare.finish()))?;
        let ok = self.expect(1, CLASS_QUEUE, 11, "queue.declare-ok")?;
        let mut fields = Reader::new(&ok);
        fields.shortstr()?;
        fields.long()
    }

    /// Publish `body` to `exchange` under `routing_key`, the body split at
    /// the negotiated frame size.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn publish(&mut self, exchange: &str, routing_key: &str, body: &[u8]) -> Result<()> {
        let mut publish = Writer::new();
        publish
            .short(0)
            .shortstr(exchange)
            .shortstr(routing_key)
            .bit(false)
            .bit(false);
        self.send(&Frame::method(1, CLASS_BASIC, 40, publish.finish()))?;
        self.send(&Frame::Header {
            channel: 1,
            class: CLASS_BASIC,
            body_size: body.len() as u64,
        })?;
        for chunk in body.chunks(self.frame_max - 8) {
            self.send(&Frame::Body {
                channel: 1,
                bytes: chunk.to_vec(),
            })?;
        }
        Ok(())
    }

    /// Consume `queue`, acknowledging each delivery once taken.
    ///
    /// # Errors
    /// Where the broker refused the consumer.
    pub fn consume(&mut self, queue: &str) -> Result<()> {
        let mut consume = Writer::new();
        consume
            .short(0)
            .shortstr(queue)
            .shortstr("")
            .bit(false)
            .bit(false)
            .bit(false)
            .bit(false)
            .table(&[]);
        self.send(&Frame::method(1, CLASS_BASIC, 20, consume.finish()))?;
        let ok = self.expect(1, CLASS_BASIC, 21, "basic.consume-ok")?;
        self.consumer_tag = Some(Reader::new(&ok).shortstr()?);
        Ok(())
    }

    /// The next delivery, acknowledged, or `None` when the broker closed.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_delivery(&mut self) -> Result<Option<Arrived>> {
        loop {
            let Some(frame) = read(&mut self.reader)? else {
                return Ok(None);
            };
            match frame {
                Frame::Method {
                    class: CLASS_BASIC,
                    method: 60,
                    arguments,
                    ..
                } => {
                    let mut fields = Reader::new(&arguments);
                    fields.shortstr()?;
                    let delivery_tag = fields.longlong()?;
                    fields.bit()?;
                    let exchange = fields.shortstr()?;
                    let routing_key = fields.shortstr()?;
                    let body = self.body()?;
                    let mut ack = Writer::new();
                    ack.longlong(delivery_tag).bit(false);
                    self.send(&Frame::method(1, CLASS_BASIC, 80, ack.finish()))?;
                    let origin = format!(
                        "amqp://{}/{exchange}/{routing_key}?delivery-tag={delivery_tag}",
                        self.broker
                    );
                    return Ok(Some(Arrived::new(origin, body)));
                }
                Frame::Method {
                    class: CLASS_CONNECTION,
                    method: 50,
                    ..
                } => {
                    self.send(&Frame::method(0, CLASS_CONNECTION, 51, Vec::new()))?;
                    return Ok(None);
                }
                Frame::Heartbeat => self.send(&Frame::Heartbeat)?,
                _ => {}
            }
        }
    }

    /// Say goodbye and close.
    pub fn close(mut self) {
        let mut close = Writer::new();
        close.short(200).shortstr("bye").short(0).short(0);
        let _ = self.send(&Frame::method(0, CLASS_CONNECTION, 50, close.finish()));
        let _ = self.expect(0, CLASS_CONNECTION, 51, "connection.close-ok");
    }

    /// A content header and the body frames it announces.
    fn body(&mut self) -> Result<Vec<u8>> {
        let Some(Frame::Header { body_size, .. }) = read(&mut self.reader)? else {
            return Err(protocol_error("a delivery without its content header"));
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

    /// The next method frame, which must be `class.method` on `channel`.
    fn expect(&mut self, channel: u16, class: u16, method: u16, what: &str) -> Result<Vec<u8>> {
        loop {
            match read(&mut self.reader)? {
                Some(Frame::Method {
                    channel: c,
                    class: cl,
                    method: m,
                    arguments,
                }) if c == channel && cl == class && m == method => return Ok(arguments),
                Some(Frame::Method {
                    class: CLASS_CONNECTION,
                    method: 50,
                    arguments,
                    ..
                }) => {
                    let mut fields = Reader::new(&arguments);
                    let code = fields.short()?;
                    let text = fields.shortstr()?;
                    let _ = self.send(&Frame::method(0, CLASS_CONNECTION, 51, Vec::new()));
                    return Err(protocol_error(format!(
                        "the broker closed while {what} was awaited: {code} {text}"
                    )));
                }
                Some(Frame::Heartbeat) => self.send(&Frame::Heartbeat)?,
                Some(_) => {}
                None => {
                    return Err(protocol_error(format!("the broker closed before {what}")));
                }
            }
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
