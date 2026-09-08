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
//! publish, consume and acknowledge. Publisher confirms, transactions, TLS
//! and AMQP 1.0 — a different protocol under the same name — are the next
//! layers.
//!
//! The origin URI carries what the frame knew:
//! `amqp://broker/exchange/routing.key?delivery-tag=1`.

pub mod client;
pub mod frame;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Login};
pub use frame::Frame;
pub use session::{Event, Session};
use transport::error::Result;
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct AmqpTransport {
    broker: String,
    exchange: String,
    routing_key: String,
    queue: String,
    login: Login,
    timeout: Option<Duration>,
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
            login: Login::default(),
            timeout: None,
        }
    }

    /// Present these when connecting.
    #[must_use]
    pub fn logging_in(mut self, login: Login) -> Self {
        self.login = login;
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
        Client::connect(&self.broker, &self.login, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.broker)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the handshake failed.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
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

    /// Consume the queue and take what is delivered until the broker is
    /// quiet for the timeout, or closes.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.declare_queue(&self.queue)?;
        client.consume(&self.queue)?;
        let mut arrived = Vec::new();
        loop {
            match client.next_delivery() {
                Ok(Some(message)) => arrived.push(message),
                Ok(None) => break,
                Err(error) if error.retryable && !arrived.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        client.close();
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, exchange, key) = self.resolve(target);
        let mut client = Client::connect(broker, &self.login, self.timeout)?;
        client.publish(exchange, key, bytes)?;
        client.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> AmqpTransport {
        AmqpTransport::new("127.0.0.1:0", "orders", "order.placed", "orders.in")
            .timing_out_after(Duration::from_secs(2))
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
        assert_eq!(first.bytes, b"first");
        assert!(first.origin_uri.ends_with("/orders/order.placed"));
        assert!(session.next_publish().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("second");
        let second = session.next_publish().expect("second").expect("one");
        assert_eq!(second.bytes, long, "three frames, one body");
        assert!(second.origin_uri.ends_with("/other/key.two"));
        assert!(session.next_publish().expect("closed").is_none());
        sender.join().expect("thread").expect("sending");
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
        session.deliver("order.placed", b"one").expect("one");
        session.deliver("order.placed", b"two").expect("two");
        // Two acknowledgements, then the client's close.
        assert!(session.next_event().expect("close").is_none());
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"one");
        assert!(
            arrived[1]
                .origin_uri
                .ends_with("/order.placed?delivery-tag=2")
        );
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
}
