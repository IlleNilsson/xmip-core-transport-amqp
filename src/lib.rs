#![forbid(unsafe_code)]

//! Streams that arrive as AMQP messages. One basic.publish or basic.deliver
//! is one Stream, the exchange and routing key kept beside it.
//!
//! AMQP 0-9-1 is the enterprise broker's protocol — `RabbitMQ` and its kin,
//! on port 5672: exchanges route by key to queues, consumers acknowledge. A
//! Receive Location connects, declares its queue and consumes it,
//! acknowledging each delivery after the runtime's receive cycle —
//! `basic.ack` when accepted, `basic.reject` without requeue when refused,
//! with requeue when the cycle failed ([`acknowledging`]); a Send Location
//! connects and publishes to an exchange under a routing key. Either may instead
//! accept clients directly through [`Session`], one client's worth of broker
//! on one channel — the shape a producer that publishes straight to Xmip
//! needs, and no more.
//!
//! What is here is the handshake with PLAIN, one channel, durable queues,
//! publish with a content header, consume and acknowledge — the one AMQP
//! 0-9-1 in the estate: `wire` is the frame and its value encodings,
//! `method` the methods, `content` the header and body frames, `client`
//! and `session` the two ends, and [`presenting`] who a Location logs in
//! as, the password the secret it names holds, read as it connects. The
//! `rabbitmq` technology speaks through them. Publisher confirms are `confirm`'s, since 2026-09-26. Transactions,
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
//!
//! **Built with its `declaration` feature alone** the crate is
//! [`declaration`], what a Location of it is configured with, and
//! [`presenting`]'s `user` and `virtual_host`, which `rabbitmq` declares
//! too, and none of the code above, which is behind the default feature:
//! what the surfaces' library carries (ADR-0064, amendment 2026-11-26).

#[cfg(feature = "default")]
pub mod acknowledging;
#[cfg(feature = "default")]
pub mod client;
#[cfg(feature = "default")]
pub mod confirm;
#[cfg(feature = "default")]
pub mod content;
#[cfg(feature = "declaration")]
pub mod declaration;
#[cfg(feature = "default")]
pub mod event_wire;
#[cfg(feature = "default")]
pub mod method;
#[cfg(feature = "declaration")]
pub mod presenting;
#[cfg(feature = "default")]
pub mod session;
#[cfg(feature = "default")]
pub mod wire;

#[cfg(feature = "default")]
use std::net::TcpListener;
#[cfg(feature = "default")]
use std::time::Duration;

#[cfg(feature = "default")]
pub use acknowledging::acknowledging;
#[cfg(feature = "default")]
pub use client::{Client, Credentials, Delivery};
#[cfg(feature = "default")]
use content::Properties;
#[cfg(feature = "default")]
use context::property::AMQP_USER_ID;
#[cfg(feature = "default")]
pub use method::Method;
#[cfg(feature = "default")]
use net::Target;
#[cfg(feature = "default")]
pub use presenting::Presenting;
#[cfg(feature = "default")]
pub use session::{Event, Publish, Queues, Session};
#[cfg(feature = "default")]
use transport::error::{Result, protocol_error};
#[cfg(feature = "default")]
use transport::listening::{Accepting, Listening};
#[cfg(feature = "default")]
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
#[cfg(feature = "default")]
use transport::{ArrivalIdentity, destination, pool::delivered, socket};
#[cfg(feature = "default")]
use transport::{Arrived, Configured, Directions, Headers, Login, Pool, Taken, Transport};
#[cfg(feature = "default")]
pub use wire::Frame;
#[cfg(feature = "default")]
use xcore::settings::Settings;

#[cfg(feature = "default")]
#[derive(Clone)]
pub struct AmqpTransport {
    broker: String,
    exchange: String,
    routing_key: String,
    queue: String,
    /// Who it logs in as, and the secret its Location names, whose
    /// password its login presents, read at each connection.
    presenting: Presenting,
    timeout: Option<Duration>,
    /// The connections a send publishes on, each with its channel open and
    /// in confirm mode: connected once per broker and kept.
    publishers: Pool<Client>,
    /// The connection a receive takes from, its queue declared and
    /// consumed on the first receive and kept consuming.
    consumers: Pool<Client>,
}

#[cfg(feature = "default")]
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
            presenting: Presenting::default(),
            timeout: None,
            publishers: Pool::new(),
            consumers: Pool::new(),
        }
    }

    /// Log in as `login` when connecting, and expect it when accepting.
    #[must_use]
    pub fn logging_in(mut self, login: Login) -> Self {
        self.presenting = self.presenting.logging_in(login);
        self
    }

    /// Connect to the broker.
    ///
    /// # Errors
    /// Where the Location's secret could not be presented, or the broker
    /// refused or could not be reached.
    pub fn connect(&self) -> Result<Client> {
        self.connect_to(&self.broker)
    }

    /// Connect to `broker`, the password the Location's secret holds read
    /// now where it names one: each new connection reads it, a kept one
    /// never again.
    fn connect_to(&self, broker: &str) -> Result<Client> {
        Client::connect(broker, &self.presenting.presented("amqp")?, self.timeout)
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
        Session::accept(listener, self.presenting.expected(), self.timeout)
    }

    /// Where a target names the broker, exchange and key itself —
    /// `amqp://host:5672/exchange/routing.key` — or is a routing key alone
    /// on this transport's broker and exchange, or empty and this
    /// transport's exchange and key (`transport::destination::named`).
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str, &'a str) {
        let Some(target) = destination::named(target) else {
            return (&self.broker, &self.exchange, &self.routing_key);
        };
        let Some(named) = Target::under(Self::SCHEMES, target) else {
            return (&self.broker, &self.exchange, target);
        };
        let (exchange, key) = named.path().split_once('/').unwrap_or(("", named.path()));
        let key = destination::named(key).unwrap_or(&self.routing_key);
        (named.authority(), exchange, key)
    }
}

#[cfg(feature = "default")]
impl Transport for AmqpTransport {
    fn name(&self) -> &'static str {
        "amqp"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered(
            "the acknowledgement goes on the session the receive reads from",
        )
    }

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Waits("the broker is drained until quiet for the timeout")
    }

    /// Take what is delivered until the broker is quiet for the timeout or
    /// closes, on the consumer the first receive declared and kept: what
    /// the broker delivered between two receives waits in the socket. A
    /// quiet queue is an empty vector, not an error. Nothing is
    /// acknowledged here: each delivery's [`acknowledging`] answers it on
    /// that consumer after the receive cycle — `basic.ack` when accepted,
    /// `basic.reject` without requeue when refused, with requeue when the
    /// cycle failed.
    fn receive_up_to(&self, most: usize) -> Result<Vec<Arrived>> {
        let deliveries = self.consumers.exchange(
            self.broker.as_str(),
            || self.connect()?.consuming(&self.queue),
            |client| delivered(client, most, Client::next_delivery),
        )?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let origin = format!(
                    "amqp://{}/{}/{}?delivery-tag={}",
                    self.broker, delivery.exchange, delivery.routing_key, delivery.delivery_tag
                );
                let acknowledgement =
                    acknowledging(&self.consumers, &self.broker, delivery.delivery_tag);
                let user = delivery
                    .properties
                    .user_id
                    .map(|user| (AMQP_USER_ID.to_string(), user));
                Arrived::whole(origin, delivery.body, acknowledgement)
                    .detected()
                    .with_headers(Headers::of("amqp").text(delivery.properties.headers))
                    .observing_all(user)
            })
            .collect())
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Gathered("a publish is one message the broker confirms whole")
    }

    /// Publish on the connection kept for the broker, connected on the
    /// first send to it, and return once the broker confirms it took the
    /// message: the confirm, not a close, is what says it arrived.
    /// The key goes in the message-id property, which a consumer, or a
    /// broker's deduplication plugin, recognizes a repeated publish by.
    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        let bytes: &[u8] = &outgoing.gather()?;
        self.publish(target, bytes, outgoing.key())
    }
}

#[cfg(feature = "default")]
impl AmqpTransport {
    /// The one send: `bytes` published to `target` and confirmed, under
    /// `key` as its message-id where there is one.
    fn publish(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let (broker, exchange, routing_key) = self.resolve(target);
        let properties = Properties::octets(false).keyed(key);
        self.publishers.exchange(
            broker,
            || self.connect_to(broker),
            |client| client.publish_confirmed(exchange, routing_key, &properties, bytes),
        )
    }
}

#[cfg(feature = "default")]
impl Configured for AmqpTransport {
    const SETTINGS: &'static Settings = declaration::SETTINGS;
    const SCHEMES: &'static [&'static str] = declaration::SCHEMES;

    /// The exchange and routing key settings: the address names only the
    /// broker.
    fn own_target(_address: &str) -> &str {
        destination::OWN
    }

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    fn configured(address: &str, settings: &xcore::settings::Read) -> Result<Self> {
        let text = |name| settings.optional_text(name).unwrap_or_default();
        Ok(Self {
            presenting: Presenting::configured(settings),
            ..Self::new(
                address,
                text("exchange"),
                text("routing_key"),
                text("queue"),
            )
        })
    }

    /// The password the Location's secret holds, beside its `user`, read
    /// at each connection; a secret named without a user is refused.
    fn secured_by(mut self, secret: transport::Secured) -> Result<Self> {
        self.presenting = self.presenting.secured_by(secret)?;
        Ok(self)
    }
}

#[cfg(feature = "default")]
impl AmqpTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, an exchange, a routing key and a queue each called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe", "probe", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

#[cfg(feature = "default")]
impl Accepting for AmqpTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
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
        Ok(Taken::new(origin, publish.body))
    }
}

#[cfg(feature = "default")]
impl Loopback for AmqpTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "the broker delivers it: its headers say who sent it, the peer is the broker",
        )
    }

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
    use xcore::settings::{Applies, Given};

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
        assert_eq!(sent.presenting.expected().virtual_host, "trade");
        assert_eq!(sent.timeout, Some(Duration::from_secs(2)));
        let given = [text("queue", "in"), text("user", "orders")];
        let received = AmqpTransport::open("broker:5672", Applies::Receive, &given).expect("built");
        assert_eq!(received.queue, "in");
        let expected = received.presenting.expected();
        assert_eq!(expected.virtual_host, "/");
        assert_eq!(
            (
                expected.login.user.as_str(),
                expected.login.password.as_str()
            ),
            ("orders", ""),
            "the password is the Location's secret, none named here"
        );
        assert_eq!(
            sent.presenting.expected().login,
            Credentials::default().login,
            "no user named: the broker's default guest"
        );
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
    fn a_send_location_publishes_to_its_exchange_and_key_and_a_target_naming_another_wins() {
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let location = format!("amqp://{address}");
        let sender = std::thread::spawn(move || {
            let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
            let given = [
                text("exchange", "orders"),
                text("routing_key", "order.placed"),
                text("timeout", "2s"),
            ];
            let near = AmqpTransport::open(&location, Applies::Send, &given)?;
            near.send(AmqpTransport::own_target(&location), b"own")?;
            near.send("order.cancelled", b"other")
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let own = session.next_publish().expect("own").expect("one");
        assert_eq!(own.queue(), "orders/order.placed");
        let other = session.next_publish().expect("other").expect("one");
        assert_eq!(other.queue(), "orders/order.cancelled");
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
            for n in 0..SENDS {
                sending.send("order.placed", n.to_string().as_bytes())?;
            }
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
    fn a_delivery_is_acked_when_accepted_rejected_when_refused_and_requeued_when_failed() {
        // The far end outwaits the client: the client gives up on a quiet
        // broker after two seconds, answers each delivery and closes, and
        // the close must find the session still listening.
        let far_end = node().timing_out_after(Duration::from_secs(6));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            let near = AmqpTransport::new(address, "orders", "order.placed", "orders.in")
                .timing_out_after(Duration::from_secs(2));
            let mut arrived = near.receive()?.into_iter();
            let one = arrived.next().expect("one");
            assert!(one.defers());
            let one = one.taken()?;
            arrived
                .next()
                .expect("two")
                .refused(transport::Refusal::Unacceptable)?;
            let three = arrived.next().expect("three");
            let origin = three.origin_uri.clone();
            three.failed()?;
            Ok::<_, transport::TransportError>((one, origin))
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
        session.deliver("orders.in", b"three").expect("three");
        // Nothing answered until the cycle: then the ack, the reject the
        // broker does not deliver again, the reject with requeue, and the
        // client's close.
        assert_eq!(session.next_event().expect("ack"), Some(Event::Acked(1)));
        assert_eq!(
            session.next_event().expect("refused"),
            Some(Event::Rejected {
                delivery_tag: 2,
                requeue: false
            })
        );
        assert_eq!(
            session.next_event().expect("failed"),
            Some(Event::Rejected {
                delivery_tag: 3,
                requeue: true
            })
        );
        assert!(session.next_event().expect("close").is_none());
        let (one, origin) = receiver.join().expect("thread").expect("receiving");
        assert_eq!(one.bytes, b"one");
        assert!(
            origin.ends_with("//orders.in?delivery-tag=3"),
            "the default exchange, the queue as the key"
        );
    }

    #[test]
    fn a_verdict_on_a_consumer_the_broker_closed_opens_no_other() {
        let consumers = Pool::<Client>::new();
        let error = acknowledging(&consumers, "127.0.0.1:1", 1)
            .acknowledge(transport::Verdict::Accepted)
            .expect_err("no consumer kept");
        assert!(error.message.contains("delivers it again"), "{error}");
        assert_eq!(consumers.opened(), 0);
    }

    #[test]
    fn five_receives_consume_once_and_a_consumer_the_broker_closed_is_replaced() {
        let far_end = node().timing_out_after(Duration::from_secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = AmqpTransport::new(address, "orders", "order.placed", "orders.in")
            .timing_out_after(Duration::from_millis(100));
        let receiving = near.clone();
        let (taken, told) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            while arrived.len() < 6 {
                let now = receiving.receive()?;
                let any = !now.is_empty();
                for one in now {
                    arrived.push(one.taken()?.bytes);
                }
                // Told once each is acked: told before, the broker could
                // close the consumer first, and the ack find none to go on.
                if any {
                    taken.send(()).expect("told");
                }
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let consuming = |session: &mut Session| {
            let declared = session.next_event().expect("declared");
            assert_eq!(declared, Some(Event::Declared("orders.in".to_string())));
            let consuming = session.next_event().expect("consuming");
            assert_eq!(consuming, Some(Event::Consuming("orders.in".to_string())));
        };
        // One login, declaration and consumer for every receive.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        consuming(&mut session);
        for round in 0..5u8 {
            session.deliver("orders.in", &[round]).expect("delivered");
            told.recv().expect("taken");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        consuming(&mut again);
        again.deliver("orders.in", &[5]).expect("delivered");
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived, (0..6u8).map(|n| vec![n]).collect::<Vec<_>>());
        assert_eq!(near.consumers.opened(), 2);
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
                .logging_in(Login::new("guest", "wrong"))
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
    fn a_wrong_login_is_refused_over_what_the_client_sent_after() {
        // The connection.close is written with megabytes the client sent
        // after its start-ok unread, which a bare drop resets away on
        // Windows.
        let far_end = node();
        let (listener, address) = far_end.bind().expect("binding");
        let serving = std::thread::spawn(move || far_end.accept_one(&listener).err());
        let mut head = wire::PROTOCOL_HEADER.to_vec();
        head.extend(
            method::connection_start_ok("guest", "wrong")
                .frame(0)
                .encode(),
        );
        let answer = transport::unread::answered_over_unread(&address, &head, 0xCE, |stream| {
            let mut reader = std::io::BufReader::new(stream);
            wire::read_frame(&mut reader)
                .and_then(|_start| wire::read_frame(&mut reader))
                .and_then(|close| {
                    let close = close.ok_or_else(|| protocol_error("closed unanswered"))?;
                    method::close_of(&method::Method::of(&close)?)
                })
        });
        let (code, _) = answer.expect("the connection.close");
        assert_eq!(code, 403);
        let refused = serving.join().expect("the far end").expect("refused");
        assert!(refused.message.contains("403"), "{refused}");
    }

    /// A Send Location of the broker at `address` logging in as `orders`,
    /// its password the secret `partner-x.amqp` that `kept` holds.
    fn secured(address: &str, kept: &std::sync::Arc<transport::secured::Kept>) -> AmqpTransport {
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let given = [
            text("exchange", "orders"),
            text("user", "orders"),
            text("timeout", "2s"),
        ];
        AmqpTransport::open(&format!("amqp://{address}"), Applies::Send, &given)
            .expect("built")
            .secured_by(kept.secured("partner-x.amqp"))
            .expect("a user is named")
    }

    #[test]
    fn the_password_is_the_locations_secret_read_at_each_connection() {
        use transport::secured::Kept;
        use xcore::SecretKind::Password;
        let kept = std::sync::Arc::new(Kept::default());
        let drawn = Kept::drawn();
        let far_end = node().logging_in(Login::new("orders", &drawn));
        let (listener, address) = far_end.bind().expect("binding");
        let near = secured(&address, &kept);
        let unset = near.send("order.placed", b"x").expect_err("not set");
        assert!(unset.retryable, "asked again at the next connection");
        assert!(
            unset.message.contains("'partner-x.amqp' is not set"),
            "{unset}"
        );

        kept.set("partner-x.amqp", Password, &format!("{drawn}-wrong"));
        let sending = near.clone();
        let sender = std::thread::spawn(move || sending.send("order.placed", b"x").err());
        let refused = far_end.accept_one(&listener).err().expect("refused");
        assert!(refused.message.contains("403"), "{refused}");
        let wrong = sender.join().expect("thread").expect("refused");
        assert!(wrong.message.contains("403 ACCESS_REFUSED"), "{wrong}");
        assert!(!wrong.retryable, "{wrong}");

        kept.set("partner-x.amqp", Password, &drawn);
        let sending = near.clone();
        let sender = std::thread::spawn(move || sending.send("order.placed", b"right"));
        let mut session = far_end.accept_one(&listener).expect("admitted");
        assert_eq!(session.user(), "orders");
        let published = session.next_publish().expect("published").expect("one");
        assert_eq!(published.body, b"right");
        sender.join().expect("thread").expect("sent");
    }

    #[test]
    fn a_secret_named_without_a_user_is_refused_as_the_node_starts() {
        let kept = std::sync::Arc::new(transport::secured::Kept::default());
        let built = AmqpTransport::open("broker:5672", Applies::Send, &[]).expect("built");
        let Err(refused) = built.secured_by(kept.secured("partner-x.amqp")) else {
            panic!("no user is named");
        };
        assert!(!refused.retryable);
        assert!(refused.message.contains("\"user\" setting"), "{refused}");
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
