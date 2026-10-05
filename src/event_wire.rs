//! Wire events over AMQP 0-9-1, as the standard they follow (`CloudEvents`
//! 1.0) binds them: the wire the event capability forwards a
//! subscription over (ADR-0065 clause 3), and the read side a receiving
//! Xmip turns a delivery back into an event with.
//!
//! What goes on the message is decided once, in `xmip-core-event`'s
//! binding: this file only puts a [`Carried`] where AMQP carries it — the
//! body as the message body, the content type as the basic class's
//! content-type property, and every `cloudEvents_` attribute in the
//! headers field table — and takes it back off a delivery the same way.
//!
//! **0-9-1, not 1.0.** The standard's AMQP protocol binding is written
//! for AMQP 1.0, where the attributes are application-properties. This
//! transport speaks 0-9-1 — `RabbitMQ` and its kin — whose message has no
//! application-properties; its headers field table is where an application
//! puts its own named values, and it is the 1.0 section's equivalent here.
//! The names and the prefix are the binding's own, unchanged, so a 1.0
//! reader bridged from a 0-9-1 broker finds them where it looks.
//!
//! Each event is one publish on a channel in confirm mode, persistent, and
//! [`Wire::carry`] answers `Ok` only once the broker's `basic.ack` names
//! it: at least once, the resilience guards deciding each attempt. A
//! `basic.nack` is retryable.
//!
//! **The identity presented** is the [`Credentials`] configured for the
//! Party, as ADR-0019 clause 3 has a Send side present the identity
//! configured for the Party it reaches: the PLAIN user and password this
//! transport connects with, on the virtual host it names. The connections
//! to each Party are the capability's [`Pool`], kept open between events;
//! one that fails is dropped, and the event goes again on a new one.

use std::collections::BTreeMap;
use std::time::Duration;

use event::binding::Carried;
use event::forward::Wire;
use transport::Pool;
use xcore::Failure;
use xcore::PartyId;

use crate::client::{Client, Credentials};
use crate::content::Properties;

/// Where one Party's events are published, and the credentials presented
/// there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exchange {
    /// The broker, `host:port`.
    pub broker: String,
    /// The exchange: empty for the default one, which routes by queue name.
    pub exchange: String,
    pub routing_key: String,
    pub credentials: Credentials,
}

impl Exchange {
    /// `exchange` under `routing_key` on the broker at `broker`, presenting
    /// the default credentials.
    #[must_use]
    pub fn new(broker: impl Into<String>, exchange: &str, routing_key: &str) -> Self {
        Self {
            broker: broker.into(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            credentials: Credentials::default(),
        }
    }

    /// Present `credentials` rather than the default.
    #[must_use]
    pub fn presenting(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }
}

/// The AMQP wire: each Party's exchange, and the connections kept to each.
pub struct EventWire {
    exchanges: BTreeMap<PartyId, Exchange>,
    timeout: Option<Duration>,
    connections: Pool<Client, PartyId>,
}

impl EventWire {
    /// A wire configured for no Party yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            exchanges: BTreeMap::new(),
            timeout: None,
            connections: Pool::new(),
        }
    }

    /// Carry `party`'s events to `exchange`.
    #[must_use]
    pub fn to(mut self, party: PartyId, exchange: Exchange) -> Self {
        self.exchanges.insert(party, exchange);
        self
    }

    /// Give up on a broker that does not answer within `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Default for EventWire {
    fn default() -> Self {
        Self::new()
    }
}

impl Wire for EventWire {
    /// One persistent publish on the Party's exchange, confirmed.
    fn carry(&self, party: PartyId, carried: &Carried) -> Result<(), Failure> {
        let to = self.exchanges.get(&party).ok_or_else(|| {
            Failure::permanent(format!("no AMQP exchange is configured for Party {party}"))
        })?;
        let properties = properties(carried);
        self.connections
            .exchange(
                &party,
                || Client::connect(&to.broker, &to.credentials, self.timeout),
                |client| {
                    client.publish_confirmed(
                        &to.exchange,
                        &to.routing_key,
                        &properties,
                        &carried.body,
                    )
                },
            )
            .map_err(Failure::from)
    }
}

/// The properties `carried` is published with: its content type, its
/// attributes as the headers table, persistent.
#[must_use]
pub fn properties(carried: &Carried) -> Properties {
    Properties {
        content_type: carried.content_type.clone(),
        headers: carried.headers.clone(),
        persistent: true,
        message_id: None,
    }
}

/// What a message with `properties` and `body` carries, for the binding
/// to read a `WireEvent` from.
#[must_use]
pub fn carried(properties: &Properties, body: &[u8]) -> Carried {
    Carried {
        content_type: properties.content_type.clone(),
        headers: properties.headers.clone(),
        body: body.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_carried_event_is_its_properties_and_body_and_back() {
        let carried = Carried {
            content_type: Some("application/json".to_string()),
            headers: vec![("cloudEvents_id".to_string(), "a b%".to_string())],
            body: b"{}".to_vec(),
        };
        let properties = properties(&carried);
        assert!(properties.persistent);
        assert_eq!(super::carried(&properties, b"{}"), carried);
    }

    #[test]
    fn a_party_with_no_exchange_is_refused_for_good() {
        let wire = EventWire::new().to(PartyId::new(1), Exchange::new("127.0.0.1:1", "", "q"));
        let refused = wire
            .carry(PartyId::new(2), &Carried::default())
            .expect_err("no exchange");
        assert!(!refused.retryable);
        assert!(refused.message.contains("no AMQP exchange"), "{refused}");
        let unreachable = wire
            .timing_out_after(Duration::from_secs(1))
            .carry(PartyId::new(1), &Carried::default())
            .expect_err("nothing listens");
        assert!(unreachable.retryable, "{unreachable}");
    }
}
