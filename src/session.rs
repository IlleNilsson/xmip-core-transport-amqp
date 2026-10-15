//! The broker's side of one connection: what a Receive Location that
//! accepts publishers directly runs, what a test puts at the far end, and
//! what the playground stands up in place of a broker.
//!
//! Not a broker. One session serves one client over one channel and keeps
//! its queues in memory: what is published is stored under the queue it
//! names, or delivered at once where the client consumes that queue, and
//! is handed up either way; what is given is delivered to the consumer or
//! kept for one. No exchanges, no bindings, nothing on disk: a publish to
//! an exchange is kept under `exchange/routing-key`. A Location that needs
//! those talks to a broker through [`crate::Client`].
//!
//! A client that puts its channel in confirm mode has each publish
//! answered `basic.ack` once it is kept — or `basic.nack` while
//! [`Session::nacking`], which is how a far end shows a publisher a broker
//! that could not take a message, and the publisher's retry.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpStream};

use transport::error::{Result, classify, protocol_error};

use crate::confirm::{self, CONFIRM_SELECT};
use crate::content::{self, Properties};
use crate::method::{
    self, BASIC_ACK, BASIC_CONSUME, BASIC_PUBLISH, BASIC_REJECT, CHANNEL_CLOSE, CHANNEL_CLOSE_OK,
    CHANNEL_OPEN, CONNECTION_CLOSE, CONNECTION_CLOSE_OK, Method, QUEUE_DECLARE,
};
use crate::wire::{Frame, Kind, MAX_FRAME, len32, read_frame};

mod opening;

/// One basic.publish as the client sent it, its properties and its body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish {
    pub exchange: String,
    pub routing_key: String,
    pub properties: Properties,
    pub body: Vec<u8>,
}

impl Publish {
    /// The queue a session keeps it under: the routing key through the
    /// default exchange, `exchange/routing-key` through any other.
    #[must_use]
    pub fn queue(&self) -> String {
        if self.exchange.is_empty() {
            self.routing_key.clone()
        } else {
            format!("{}/{}", self.exchange, self.routing_key)
        }
    }
}

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published this.
    Published(Publish),
    /// The client declared this queue.
    Declared(String),
    /// The client started consuming this queue.
    Consuming(String),
    /// The client acknowledged this delivery tag.
    Acked(u64),
    /// The client rejected this delivery tag, and asked it requeued or not.
    Rejected { delivery_tag: u64, requeue: bool },
}

/// What the session holds: messages per queue, not yet delivered.
pub type Queues = BTreeMap<String, Vec<Vec<u8>>>;

/// The one consumer a session serves: its tag, queue and channel.
struct Consumer {
    tag: String,
    queue: String,
    channel: u16,
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    user: String,
    virtual_host: String,
    queues: Queues,
    consumer: Option<Consumer>,
    delivery_tag: u64,
    /// In confirm mode, how many publishes the channel has numbered.
    confirming: Option<u64>,
    nacking: u32,
}

impl Session {
    /// Answer the next `times` confirmed publishes `basic.nack`, keeping
    /// nothing of them.
    #[must_use]
    pub const fn nacking(mut self, times: u32) -> Self {
        self.nacking = times;
        self
    }

    /// Hold these queues, the way one session hands its state to the next.
    #[must_use]
    pub fn with_queues(mut self, queues: Queues) -> Self {
        self.queues = queues;
        self
    }

    /// Where the client connected from.
    #[must_use]
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Who connected, as the PLAIN response named them.
    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    /// The virtual host the client opened.
    #[must_use]
    pub fn virtual_host(&self) -> &str {
        &self.virtual_host
    }

    /// What is queued and not yet delivered.
    #[must_use]
    pub const fn queues(&self) -> &Queues {
        &self.queues
    }

    /// The queues, for the next session to carry on with.
    #[must_use]
    pub fn into_queues(self) -> Queues {
        self.queues
    }

    /// The next message the client publishes, or `None` when it closed.
    /// Channels, declarations, consumers and acks are answered on the way.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Publish>> {
        loop {
            match self.next_event()? {
                Some(Event::Published(publish)) => return Ok(Some(publish)),
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
            let Some(frame) = read_frame(&mut self.reader)? else {
                return Ok(None);
            };
            let method = match frame.kind {
                Kind::Method => Method::of(&frame)?,
                Kind::Heartbeat => {
                    self.write(&Frame::new(Kind::Heartbeat, 0, Vec::new()).encode())?;
                    continue;
                }
                Kind::Header | Kind::Body => {
                    return Err(protocol_error("content with no publish before it"));
                }
            };
            let channel = frame.channel;
            let event = match method.id {
                CHANNEL_OPEN => {
                    self.say(channel, &method::channel_open_ok())?;
                    continue;
                }
                CHANNEL_CLOSE => {
                    self.say(channel, &Method::bare(CHANNEL_CLOSE_OK))?;
                    self.consumer = None;
                    continue;
                }
                QUEUE_DECLARE => {
                    let queue = method::queue_of(&method)?;
                    let held = self.queues.get(&queue).map_or(0, Vec::len);
                    self.say(channel, &method::queue_declare_ok(&queue, len32(held)))?;
                    Event::Declared(queue)
                }
                BASIC_CONSUME => {
                    let (queue, tag) = method::consume_of(&method)?;
                    let tag = if tag.is_empty() {
                        format!("xmip.{}", self.peer.port())
                    } else {
                        tag
                    };
                    self.say(channel, &method::basic_consume_ok(&tag))?;
                    self.consumer = Some(Consumer {
                        tag,
                        queue: queue.clone(),
                        channel,
                    });
                    for body in self.queues.remove(&queue).unwrap_or_default() {
                        self.deliver(&queue, &body)?;
                    }
                    Event::Consuming(queue)
                }
                CONFIRM_SELECT => {
                    self.say(channel, &Method::bare(confirm::CONFIRM_SELECT_OK))?;
                    self.confirming = Some(0);
                    continue;
                }
                BASIC_PUBLISH => {
                    let (exchange, routing_key) = method::publish_of(&method)?;
                    let (properties, body) = content::read(&mut self.reader)?;
                    let publish = Publish {
                        exchange,
                        routing_key,
                        properties,
                        body,
                    };
                    if !self.confirmed(channel)? {
                        continue;
                    }
                    self.deliver(&publish.queue(), &publish.body)?;
                    Event::Published(publish)
                }
                BASIC_ACK => Event::Acked(method::delivery_tag_of(&method)?),
                BASIC_REJECT => {
                    let (delivery_tag, requeue) = method::reject_of(&method)?;
                    Event::Rejected {
                        delivery_tag,
                        requeue,
                    }
                }
                CONNECTION_CLOSE => {
                    self.say(0, &Method::bare(CONNECTION_CLOSE_OK))?;
                    return Ok(None);
                }
                _ => {
                    let label = method.label();
                    self.close(540, "NOT_IMPLEMENTED")?;
                    return Err(protocol_error(format!("method {label} is not served here")));
                }
            };
            return Ok(Some(event));
        }
    }

    /// In confirm mode, answer the publish just read: `basic.nack` while
    /// nacking, and whether it was taken; outside it, taken.
    fn confirmed(&mut self, channel: u16) -> Result<bool> {
        let Some(numbered) = self.confirming.as_mut() else {
            return Ok(true);
        };
        *numbered += 1;
        let number = *numbered;
        if self.nacking > 0 {
            self.nacking -= 1;
            self.say(channel, &confirm::basic_nack(number))?;
            return Ok(false);
        }
        self.say(channel, &method::basic_ack(number))?;
        Ok(true)
    }

    /// Deliver `body` on `queue`, through the default exchange: as a
    /// basic.deliver where the client consumes that queue, else into the
    /// queue for when it does.
    ///
    /// # Errors
    /// Where the client went away.
    pub fn deliver(&mut self, queue: &str, body: &[u8]) -> Result<()> {
        let Some((tag, channel)) = self
            .consumer
            .as_ref()
            .filter(|consumer| consumer.queue == queue)
            .map(|consumer| (consumer.tag.clone(), consumer.channel))
        else {
            self.queues
                .entry(queue.to_string())
                .or_default()
                .push(body.to_vec());
            return Ok(());
        };
        self.delivery_tag += 1;
        let deliver = method::basic_deliver(&tag, self.delivery_tag, "", queue);
        self.say(channel, &deliver)?;
        for frame in content::frames(channel, &Properties::octets(true), body, MAX_FRAME) {
            self.write(&frame.encode())?;
        }
        Ok(())
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
