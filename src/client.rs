//! The client's side of one connection to a broker: the handshake with
//! PLAIN, one channel, declare, publish, consume, deliver, acknowledge —
//! and publish confirmed, where the channel is in confirm mode
//! (`confirm.rs`) and a publish returns once the broker took it.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, TransportError, classify, protocol_error};
use transport::pool::{Pooled, alive};
use transport::{Login, socket};

use crate::confirm::{self, CONFIRM_SELECT_OK, Confirmed};
use crate::content::{self, Properties};
use crate::method::{
    self, BASIC_CONSUME_OK, BASIC_DELIVER, CHANNEL_CLOSE, CHANNEL_CLOSE_OK, CHANNEL_OPEN_OK,
    CONNECTION_CLOSE, CONNECTION_CLOSE_OK, CONNECTION_OPEN_OK, CONNECTION_START, CONNECTION_TUNE,
    CONNECTION_TUNE_OK, Id, Method, QUEUE_DECLARE_OK,
};
use crate::wire::{Frame, Kind, MAX_FRAME, PROTOCOL_HEADER, len32, read_frame};

/// What a Location presents when it connects, and what a [`crate::Session`]
/// expects: the login, and the virtual host it opens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials {
    pub login: Login,
    pub virtual_host: String,
}

impl Credentials {
    /// `user` and `password` on the default virtual host `/`.
    #[must_use]
    pub fn new(user: &str, password: &str) -> Self {
        Self {
            login: Login::new(user, password),
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

impl Default for Credentials {
    /// What a fresh broker accepts from the local machine.
    fn default() -> Self {
        Self::new("guest", "guest")
    }
}

/// One basic.deliver as the broker sent it: where it was published, the
/// tag to acknowledge it by, its properties and the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub properties: Properties,
    pub body: Vec<u8>,
}

/// One open connection with channel 1 open on it.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    frame_max: usize,
    closed: bool,
    /// In confirm mode, how many publishes the channel has numbered.
    confirming: Option<u64>,
}

impl Client {
    /// Connect to `broker` and complete the connection and channel
    /// handshake presenting `credentials`.
    ///
    /// # Errors
    /// Where the broker could not be reached, refused the login or the
    /// virtual host, or did not speak AMQP 0-9-1.
    pub fn connect(
        broker: &str,
        credentials: &Credentials,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let login = &credentials.login;
        let stream = socket::connect_tcp(broker, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            frame_max: MAX_FRAME,
            closed: false,
            confirming: None,
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
        client.say(0, &method::connection_open(&credentials.virtual_host))?;
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
        self.publish_with(exchange, routing_key, &Properties::octets(persistent), body)
    }

    /// Publish `body` to `exchange` under `routing_key` with `properties`
    /// in its content header, the body split at the negotiated frame size.
    ///
    /// # Errors
    /// Where the broker went away.
    fn publish_with(
        &mut self,
        exchange: &str,
        routing_key: &str,
        properties: &Properties,
        body: &[u8],
    ) -> Result<()> {
        // One write for the method, its header and its body: three would
        // each wait on the one before under Nagle's algorithm.
        let mut frames = method::basic_publish(exchange, routing_key)
            .frame(1)
            .encode();
        for frame in content::frames(1, properties, body, self.frame_max) {
            frames.extend(frame.encode());
        }
        self.write(&frames)?;
        if let Some(numbered) = self.confirming.as_mut() {
            *numbered += 1;
        }
        Ok(())
    }

    /// Put the channel in confirm mode, once: every publish after it is
    /// numbered and answered by the broker.
    ///
    /// # Errors
    /// Where the broker refused confirms or went away.
    pub fn confirm(&mut self) -> Result<()> {
        if self.confirming.is_none() {
            self.say(1, &confirm::select())?;
            self.expect(1, CONFIRM_SELECT_OK, "confirm.select-ok")?;
            self.confirming = Some(0);
        }
        Ok(())
    }

    /// [`Client::publish_with`], in confirm mode, returning once the broker
    /// took it: at least once.
    ///
    /// # Errors
    /// Where the broker did not take it (`basic.nack`, retryable), closed
    /// the channel, or went away.
    pub fn publish_confirmed(
        &mut self,
        exchange: &str,
        routing_key: &str,
        properties: &Properties,
        body: &[u8],
    ) -> Result<()> {
        self.confirm()?;
        self.publish_with(exchange, routing_key, properties, body)?;
        let number = self.confirming.unwrap_or_default();
        loop {
            let Some(frame) = read_frame(&mut self.reader)? else {
                self.closed = true;
                return Err(protocol_error("the broker closed before confirming"));
            };
            match frame.kind {
                Kind::Heartbeat => self.heartbeat()?,
                Kind::Method => {
                    let method = Method::of(&frame)?;
                    match Confirmed::of(&method)? {
                        Some(said) if said.answers(number) && said.taken => return Ok(()),
                        Some(said) if said.answers(number) => {
                            return Err(TransportError::retryable(
                                "the broker did not take the publish (basic.nack)",
                            ));
                        }
                        Some(_) => {}
                        None => self.closed_by(frame.channel, &method, "a confirm")?,
                    }
                }
                Kind::Header | Kind::Body => {}
            }
        }
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

    /// This client with `queue` declared durable and consumed: what a
    /// Receive Location keeps between receives, and drains with
    /// [`Client::next_acked`] and `transport::pool::delivered`.
    ///
    /// # Errors
    /// Where the broker refused the queue or the consumer, or went away.
    pub fn consuming(mut self, queue: &str) -> Result<Self> {
        self.declare(queue)?;
        self.consume(queue)?;
        Ok(self)
    }

    /// The next delivery, acknowledged, or `None` when the broker closed.
    ///
    /// # Errors
    /// As [`Client::next_delivery`], or where the acknowledgement could not
    /// be written.
    pub fn next_acked(&mut self) -> Result<Option<Delivery>> {
        let Some(delivery) = self.next_delivery()? else {
            return Ok(None);
        };
        self.ack(delivery.delivery_tag)?;
        Ok(Some(delivery))
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
        let (properties, body) = content::read(&mut self.reader)?;
        Ok(Delivery {
            exchange,
            routing_key,
            delivery_tag,
            properties,
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

impl Pooled for Client {
    /// While the broker has closed neither the connection nor the channel.
    /// Tune-ok asks for no heartbeat, so an idle connection is not closed
    /// for silence.
    fn usable(&mut self) -> bool {
        !self.closed && alive(&self.writer)
    }
}
