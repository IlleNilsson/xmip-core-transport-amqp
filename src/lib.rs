#![forbid(unsafe_code)]

//! Streams that arrive as AMQP messages. One basic.publish or basic.deliver
//! is one Stream, the exchange and routing key kept beside it.
//!
//! AMQP 0-9-1 is the enterprise broker's protocol — `RabbitMQ` and its kin,
//! on port 5672: exchanges route by key to queues, consumers acknowledge. A
//! Receive Location connects, declares its queue and consumes it,
//! acknowledging each delivery once it is a Stream; a Send Location connects
//! and publishes to an exchange under a routing key. Either may instead
//! accept clients directly through [`Session`], one client's worth of broker
//! on one channel — the shape a producer that publishes straight to Xmip
//! needs, and no more.
//!
//! What is here is the handshake with PLAIN, one channel, durable queues,
//! publish with a content header, consume and acknowledge — the one AMQP
//! 0-9-1 in the estate: `wire` is the frame and its value encodings,
//! `method` the methods, `content` the header and body frames, `client`
//! and `session` the two ends. The `rabbitmq` technology speaks through
//! them. Publisher confirms are `confirm`'s, since 2026-09-26. Transactions,
//! TLS and AMQP 1.0 — a different protocol under the same name — are the
//! next layers.
//!
//! The origin URI carries what the frame knew:
//! `amqp://broker/exchange/routing.key?delivery-tag=1`.
//!
//! The event capability rides this transport (ADR-0065 clause 3):
//! [`event_wire`] publishes a `WireEvent` confirmed — the content
//! type in the basic properties, the `cloudEvents_` attributes in the
//! headers field table, 0-9-1's equivalent of the 1.0 binding's
//! application-properties — and reads one back off a delivery.

pub mod client;
pub mod confirm;
pub mod content;
pub mod event_wire;
pub mod method;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Credentials, Delivery};
use content::Properties;
pub use method::Method;
pub use session::{Event, Publish, Queues, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, Pool, Transport};
pub use wire::Frame;
use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

#[derive(Clone)]
pub struct AmqpTransport {
    broker: String,
    exchange: String,
    routing_key: String,
    queue: String,
    credentials: Credentials,
    timeout: Option<Duration>,
    /// The connections a send publishes on, each with its channel open and
    /// in confirm mode: connected once per broker and kept.
    publishers: Pool<Client>,
}

impl AmqpTransport {
    /// Speak to the broker at `broker`: consume `queue`, publish to
    /// `exchange` under `routing_key`.
    #[must_use]
    pub fn new(broker: impl Into<String>, exchange: &str, routing_key: &str, queue: &str) -> Self {
        Self {
            broker: broker.into(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            queue: queue.to_string(),
            credentials: Credentials::default(),
            timeout: None,
            publishers: Pool::new(),
        }
    }

    /// Present these when connecting.
    #[must_use]
    pub fn logging_in(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// Give up on a peer that stops mid-frame, and stop receiving when the
    /// broker has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the broker.
    ///
    /// # Errors
    /// Where the broker refused or could not be reached.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.broker, &self.credentials, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.broker)
    }

    /// Accept one client on an already-bound listener, expecting this
    /// transport's login.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the handshake failed,
    /// or the client was refused.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, &self.credentials, self.timeout)
    }

    /// Where a target names the broker, exchange and key itself —
    /// `amqp://host:5672/exchange/routing.key` — or is a routing key alone
    /// on this transport's broker and exchange.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str, &'a str) {
        let Some(rest) = target.strip_prefix("amqp://") else {
            return (&self.broker, &self.exchange, target);
        };
        let (broker, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let (exchange, key) = rest.split_once('/').unwrap_or(("", rest));
        (
            broker,
            exchange,
            if key.is_empty() {
                &self.routing_key
            } else {
                key
            },
        )
    }
}

impl Transport for AmqpTransport {
    fn name(&self) -> &'static str {
        "amqp"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Declare and consume the queue and take what is delivered, each
    /// acknowledged, until the broker is quiet for the timeout or closes.
    /// A quiet queue is an empty vector, not an error.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        let deliveries = client.drain(&self.queue)?;
        client.close()?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let origin = format!(
                    "amqp://{}/{}/{}?delivery-tag={}",
                    self.broker, delivery.exchange, delivery.routing_key, delivery.delivery_tag
                );
                Arrived::new(origin, delivery.body)
            })
            .collect())
    }

    /// Publish on the connection kept for the broker, connected on the
    /// first send to it, and return once the broker confirms it took the
    /// message: the confirm, not a close, is what says it arrived.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, exchange, key) = self.resolve(target);
        let properties = Properties::octets(false);
        self.publishers.exchange(
            broker,
            || Client::connect(broker, &self.credentials, self.timeout),
            |client| client.publish_confirmed(exchange, key, &properties, bytes),
        )
    }
}

impl Configured for AmqpTransport {
    /// The address is the broker, `host:5672`. The user and password are
    /// the Location's credentials, not settings: a secret never is.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "queue",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The queue a Receive Location declares and consumes.",
                applies: Applies::Receive,
            },
            Setting {
                name: "exchange",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The exchange a Send Location publishes to; the broker's default \
                          exchange when left out.",
                applies: Applies::Send,
            },
            Setting {
                name: "routing_key",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The routing key published under when a send target names none.",
                applies: Applies::Send,
            },
            Setting {
                name: "virtual_host",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The broker's virtual host connected to; the default virtual host \
                          when left out.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a peer that stops mid-frame is waited on, and how long a \
                          receive waits on a quiet broker; unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &xcore::settings::Read) -> Result<Self> {
        let text = |name| settings.optional_text(name).unwrap_or_default();
        // The user and password come through the Location's credentials.
        let credentials = match settings.optional_text("virtual_host") {
            Some(virtual_host) => Credentials::default().on(virtual_host),
            None => Credentials::default(),
        };
        let transport = Self::new(
            address,
            text("exchange"),
            text("routing_key"),
            text("queue"),
        )
        .logging_in(credentials);
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl AmqpTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, an exchange, a routing key and a queue each called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe", "probe", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for AmqpTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        // The confirm goes out as the publish is read; the client keeps its
        // connection for the next.
        let publish = session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client closed without publishing"))?;
        let origin = format!(
            "amqp://{}/{}/{}",
            session.peer(),
            publish.exchange,
            publish.routing_key
        );
        Ok(Arrived::new(origin, publish.body))
    }
}

impl Loopback for AmqpTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A client to `address`, publishing to this transport's exchange under
    /// its routing key, confirmed before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            broker: address.to_string(),
            ..self.clone()
        }
        .send(&self.routing_key, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::{edge_payloads, sized_payloads};
    use xcore::settings::Given;

    fn node() -> AmqpTransport {
        AmqpTransport::new("127.0.0.1:0", "orders", "order.placed", "orders.in")
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn amqp_declares_its_settings_and_reads_through_them() {
        assert_eq!(AmqpTransport::SETTINGS.problems(), Vec::<String>::new());
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let sent = AmqpTransport::open(
            "broker:5672",
            Applies::Send,
            &[
                text("exchange", "orders"),
                text("routing_key", "order.placed"),
                text("virtual_host", "trade"),
                text("timeout", "2s"),
            ],
        )
        .expect("built");
        assert_eq!(sent.broker, "broker:5672");
        assert_eq!(sent.exchange, "orders");
        assert_eq!(sent.routing_key, "order.placed");
        assert_eq!(sent.credentials.virtual_host, "trade");
        assert_eq!(sent.timeout, Some(Duration::from_secs(2)));
        let received = AmqpTransport::open("broker:5672", Applies::Receive, &[text("queue", "in")])
            .expect("built");
        assert_eq!(received.queue, "in");
        assert_eq!(received.credentials.virtual_host, "/");
        let Err(refused) = AmqpTransport::open("broker:5672", Applies::Receive, &[]) else {
            panic!("a Receive Location's queue is required");
        };
        assert!(refused.message.contains("\"queue\""), "{refused}");
    }

    #[test]
    fn a_client_publishes_to_a_session_in_frames() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let long = vec![0x2a; 300_000];
        let sent = long.clone();
        let sender = std::thread::spawn(move || {
            let near = AmqpTransport::new(address.clone(), "orders", "order.placed", "q")
                .timing_out_after(Duration::from_secs(2));
            near.send("order.placed", b"first")?;
            near.send(&format!("amqp://{address}/other/key.two"), &sent)
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.user(), "guest");
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.body, b"first");
        assert_eq!(
            (first.exchange.as_str(), first.queue()),
            ("orders", "orders/order.placed".into())
        );
        // The same broker, so the same connection: connected once.
        let second = session.next_publish().expect("second").expect("one");
        assert_eq!(second.body, long, "many frames, one body");
        assert_eq!(second.queue(), "other/key.two");
        assert!(session.next_publish().expect("closed").is_none());
        sender.join().expect("thread").expect("sending");
    }

    #[test]
    fn a_thousand_publishes_connect_once_and_a_connection_the_broker_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = node().timing_out_after(Duration::from_secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = AmqpTransport::new(address, "orders", "order.placed", "q")
            .timing_out_after(Duration::from_secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("order.placed", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a publish.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("order.placed", b"after the close")
        });
        // One login for every publish: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let publish = session.next_publish().expect("publish").expect("one");
            assert_eq!(publish.body, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        let last = again.next_publish().expect("publish").expect("one");
        assert_eq!(last.body, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.publishers.opened(), 2);
    }

    #[test]
    fn a_session_delivers_to_a_consumer_and_is_acknowledged() {
        // The far end outwaits the client: the client gives up on a quiet
        // broker after two seconds and closes, and the close must find the
        // session still listening.
        let far_end = node().timing_out_after(Duration::from_secs(6));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            AmqpTransport::new(address, "orders", "order.placed", "orders.in")
                .timing_out_after(Duration::from_secs(2))
                .receive()
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_event().expect("declared"),
            Some(Event::Declared("orders.in".into()))
        );
        assert_eq!(
            session.next_event().expect("consuming"),
            Some(Event::Consuming("orders.in".into()))
        );
        session.deliver("orders.in", b"one").expect("one");
        session.deliver("orders.in", b"two").expect("two");
        // Two acknowledgements, then the client's close.
        assert_eq!(session.next_event().expect("ack"), Some(Event::Acked(1)));
        assert_eq!(session.next_event().expect("ack"), Some(Event::Acked(2)));
        assert!(session.next_event().expect("close").is_none());
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"one");
        assert!(
            arrived[1]
                .origin_uri
                .ends_with("//orders.in?delivery-tag=2"),
            "the default exchange, the queue as the key"
        );
    }

    #[test]
    fn a_session_delivers_what_it_is_given_while_the_client_listens() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            let near = AmqpTransport::new(address, "", "prices", "prices")
                .timing_out_after(Duration::from_secs(2));
            let mut client = near.connect()?;
            let tag = client.consume("prices")?;
            let first = client.next_delivery()?.expect("first");
            client.ack(first.delivery_tag)?;
            let second = client.next_delivery()?;
            Ok::<_, transport::TransportError>((tag, first, second))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_event().expect("consuming"),
            Some(Event::Consuming("prices".to_string()))
        );
        session.deliver("prices", b"42").expect("delivered");
        assert_eq!(session.next_event().expect("acked"), Some(Event::Acked(1)));
        drop(session);
        let (tag, first, second) = receiver.join().expect("thread").expect("listening");
        assert!(tag.starts_with("xmip."));
        assert_eq!(first.body, b"42");
        assert_eq!(
            (first.exchange.as_str(), first.routing_key.as_str()),
            ("", "prices")
        );
        assert_eq!(first.delivery_tag, 1);
        assert!(second.is_none(), "the broker closed");
    }

    #[test]
    fn a_quiet_queue_is_nothing_received_and_a_wrong_login_is_refused() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let near = address.clone();
        let receiver = std::thread::spawn(move || {
            AmqpTransport::new(near, "orders", "order.placed", "orders.in")
                .timing_out_after(Duration::from_millis(300))
                .receive()
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        while session.next_event().expect("serving").is_some() {}
        let arrived = receiver.join().expect("thread").expect("quiet");
        assert!(arrived.is_empty());
        let stranger = std::thread::spawn(move || {
            AmqpTransport::new(address, "e", "k", "q")
                .logging_in(Credentials::new("guest", "wrong"))
                .timing_out_after(Duration::from_secs(2))
                .connect()
                .err()
                .expect("refused")
        });
        let refused = far_end.accept_one(&listener).err().expect("refused");
        assert!(refused.message.contains("403"), "{refused}");
        let error = stranger.join().expect("thread");
        assert!(!error.retryable, "{error}");
    }

    #[test]
    fn a_peer_that_does_not_speak_amqp_is_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut header = [0u8; 8];
            std::io::Read::read_exact(&mut stream, &mut header).expect("header");
            std::io::Write::write_all(&mut stream, b"220 mail\r\n").expect("write");
            // Stay open until the client has read the greeting and gone.
            let _ = std::io::Read::read(&mut stream, &mut header);
        });
        let Err(error) = AmqpTransport::new(address, "e", "k", "q")
            .timing_out_after(Duration::from_secs(2))
            .connect()
        else {
            panic!("connected");
        };
        assert!(!error.retryable, "{error}");
        assert!(node().claims().is_none());
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = AmqpTransport::loopback();
        let arrived = loopback.round(b"published").expect("round");
        assert_eq!(arrived.bytes, b"published");
        assert!(arrived.origin_uri.starts_with("amqp://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/probe/probe"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = AmqpTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
