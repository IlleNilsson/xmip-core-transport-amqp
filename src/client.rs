//! The client's side of one connection to a broker: the handshake with
//! PLAIN, one channel, declare, publish, consume, deliver, acknowledge.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::content;
use crate::method::{
    self, BASIC_CONSUME_OK, BASIC_DELIVER, CHANNEL_CLOSE, CHANNEL_CLOSE_OK, CHANNEL_OPEN_OK,
    CONNECTION_CLOSE, CONNECTION_CLOSE_OK, CONNECTION_OPEN_OK, CONNECTION_START, CONNECTION_TUNE,
    CONNECTION_TUNE_OK, Id, Method, QUEUE_DECLARE_OK,
};
use crate::wire::{Frame, Kind, MAX_FRAME, PROTOCOL_HEADER, len32, read_frame};

/// What a Location presents when it connects, and what a [`crate::Session`]
/// expects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Login {
    pub user: String,
    pub password: String,
    pub virtual_host: String,
}

impl Login {
    /// `user` and `password` on the default virtual host `/`.
    #[must_use]
    pub fn new(user: &str, password: &str) -> Self {
        Self {
            user: user.to_string(),
            password: password.to_string(),
            virtual_host: "/".to_string(),
        }
    }

    /// The same login on `virtual_host`.
    #[must_use]
    pub fn on(mut self, virtual_host: &str) -> Self {
        self.virtual_host = virtual_host.to_string();
        self
    }
}

impl Default for Login {
    /// What a fresh broker accepts from the local machine.
    fn default() -> Self {
        Self::new("guest", "guest")
    }
}

/// One basic.deliver as the broker sent it: where it was published, the
/// tag to acknowledge it by, and the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub body: Vec<u8>,
}

/// One open connection with channel 1 open on it.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    frame_max: usize,
    closed: bool,
}

impl Client {
    /// Connect to `broker` and complete the connection and channel
    /// handshake as `login`.
    ///
    /// # Errors
    /// Where the broker could not be reached, refused the login or the
    /// virtual host, or did not speak AMQP 0-9-1.
    pub fn connect(broker: &str, login: &Login, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(broker, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            frame_max: MAX_FRAME,
            closed: false,
        };
        client.write(PROTOCOL_HEADER)?;
        client.expect(0, CONNECTION_START, "connection.start")?;
        client.say(
            0,
            &method::connection_start_ok(&login.user, &login.password),
        )?;
        let tune = client.expect(0, CONNECTION_TUNE, "connection.tune")?;
        let (channel_max, proposed) = method::tune_of(&tune)?;
        let frame_max = match proposed {
            0 => len32(MAX_FRAME),
            other => other.min(len32(MAX_FRAME)),
        };
        client.frame_max = frame_max as usize;
        client.say(0, &method::tune(CONNECTION_TUNE_OK, channel_max, frame_max))?;
        client.say(0, &method::connection_open(&login.virtual_host))?;
        client.expect(0, CONNECTION_OPEN_OK, "connection.open-ok")?;
        client.say(1, &method::channel_open())?;
        client.expect(1, CHANNEL_OPEN_OK, "channel.open-ok")?;
        Ok(client)
    }

    /// The frame size settled on in tune.
    #[must_use]
    pub const fn frame_max(&self) -> usize {
        self.frame_max
    }

    /// Declare `queue`, durable; how many messages it holds.
    ///
    /// # Errors
    /// Where the broker refused the declaration or went away.
    pub fn declare(&mut self, queue: &str) -> Result<u32> {
        self.say(1, &method::queue_declare(queue))?;
        let ok = self.expect(1, QUEUE_DECLARE_OK, "queue.declare-ok")?;
        Ok(method::declare_ok_of(&ok)?.1)
    }

    /// Publish `body` to `exchange` under `routing_key`, with its content
    /// header — persistent or not — and the body split at the negotiated
    /// frame size.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        persistent: bool,
    ) -> Result<()> {
        self.say(1, &method::basic_publish(exchange, routing_key))?;
        for frame in content::frames(1, body, self.frame_max, persistent) {
            self.write(&frame.encode())?;
        }
        Ok(())
    }

    /// Consume `queue`, each delivery acknowledged by [`Client::ack`]; the
    /// consumer tag the broker confirmed.
    ///
    /// # Errors
    /// Where the broker refused the consumer or went away.
    pub fn consume(&mut self, queue: &str) -> Result<String> {
        self.say(1, &method::basic_consume(queue, ""))?;
        let ok = self.expect(1, BASIC_CONSUME_OK, "basic.consume-ok")?;
        method::tag_of(&ok)
    }

    /// Declare `queue` durable, consume it, and take what is delivered,
    /// each acknowledged, until it has been quiet for the timeout or the
    /// broker closes. A quiet queue is an empty vector, not an error.
    ///
    /// # Errors
    /// Where the broker refused the queue or the consumer, or the
    /// connection broke other than by going quiet.
    pub fn drain(&mut self, queue: &str) -> Result<Vec<Delivery>> {
        self.declare(queue)?;
        self.consume(queue)?;
        let mut deliveries = Vec::new();
        loop {
            match self.next_delivery() {
                Ok(Some(delivery)) => {
                    self.ack(delivery.delivery_tag)?;
                    deliveries.push(delivery);
                }
                Ok(None) => return Ok(deliveries),
                Err(error) if error.retryable => return Ok(deliveries),
                Err(error) => return Err(error),
            }
        }
    }

    /// The next delivery, or `None` when the broker closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the broker closed the channel.
    pub fn next_delivery(&mut self) -> Result<Option<Delivery>> {
        loop {
            let Some(frame) = read_frame(&mut self.reader)? else {
                self.closed = true;
                return Ok(None);
            };
            match frame.kind {
                Kind::Heartbeat => self.heartbeat()?,
                Kind::Method => {
                    let method = Method::of(&frame)?;
                    if method.is(BASIC_DELIVER) {
                        return self.delivery(&method).map(Some);
                    }
                    if method.is(CONNECTION_CLOSE) {
                        let _ = self.say(0, &Method::bare(CONNECTION_CLOSE_OK));
                        self.closed = true;
                        return Ok(None);
                    }
                    self.closed_by(frame.channel, &method, "a delivery")?;
                }
                Kind::Header | Kind::Body => {}
            }
        }
    }

    /// basic.ack the delivery under `delivery_tag`.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn ack(&mut self, delivery_tag: u64) -> Result<()> {
        self.say(1, &method::basic_ack(delivery_tag))
    }

    /// Close the channel and the connection, each answered; nothing to do
    /// where the broker already closed.
    ///
    /// # Errors
    /// Where the broker went away before answering.
    pub fn close(mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.say(1, &method::close(CHANNEL_CLOSE, 200, "bye"))?;
        self.expect(1, CHANNEL_CLOSE_OK, "channel.close-ok")?;
        self.say(0, &method::close(CONNECTION_CLOSE, 200, "bye"))?;
        self.expect(0, CONNECTION_CLOSE_OK, "connection.close-ok")?;
        Ok(())
    }

    /// A deliver and the content after it.
    fn delivery(&mut self, method: &Method) -> Result<Delivery> {
        let (delivery_tag, exchange, routing_key) = method::deliver_of(method)?;
        let body = content::read(&mut self.reader)?;
        Ok(Delivery {
            exchange,
            routing_key,
            delivery_tag,
            body,
        })
    }

    /// The next method, which must be `id` on `channel`; anything else on
    /// the way is skipped, a close is answered and is the failure.
    fn expect(&mut self, channel: u16, id: Id, what: &str) -> Result<Method> {
        loop {
            let Some(frame) = read_frame(&mut self.reader)? else {
                return Err(protocol_error(format!("the broker closed before {what}")));
            };
            match frame.kind {
                Kind::Heartbeat => self.heartbeat()?,
                Kind::Method => {
                    let method = Method::of(&frame)?;
                    if frame.channel == channel && method.is(id) {
                        return Ok(method);
                    }
                    self.closed_by(frame.channel, &method, what)?;
                }
                Kind::Header | Kind::Body => {}
            }
        }
    }

    /// A close from the broker while `what` was awaited: answered, and
    /// the failure it is. Anything else passes.
    fn closed_by(&mut self, channel: u16, method: &Method, what: &str) -> Result<()> {
        let (reply, which) = if method.is(CONNECTION_CLOSE) {
            (CONNECTION_CLOSE_OK, "connection")
        } else if method.is(CHANNEL_CLOSE) {
            (CHANNEL_CLOSE_OK, "channel")
        } else {
            return Ok(());
        };
        let (code, text) = method::close_of(method)?;
        let _ = self.say(channel, &Method::bare(reply));
        Err(protocol_error(format!(
            "the broker closed the {which} while {what} was awaited: {code} {text}"
        )))
    }

    fn heartbeat(&mut self) -> Result<()> {
        self.write(&Frame::new(Kind::Heartbeat, 0, Vec::new()).encode())
    }

    fn say(&mut self, channel: u16, method: &Method) -> Result<()> {
        self.write(&method.frame(channel).encode())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| classify("writing a frame", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a frame", &e))
    }
}
