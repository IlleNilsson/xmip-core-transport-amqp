//! The event capability over AMQP 0-9-1 (ADR-0065 clause 3): a
//! subscription forwarded through this transport's wire to its own far
//! end, read back as the Event that was published — in both modes of the
//! binding, presenting the Party's login, at least once through a broker
//! that refuses; and the AMQP binding's own example, under either prefix.
//! How near real time it is (the owner, 2026-09-26) is held in the
//! module's timing program, `tests/measured`.

use std::fs;
use std::time::Duration;

use event::Event;
use event::binding::{Carried, Mode, Rules};
use resilience::Guard;
use retry::Retry;
use xcluster::delivery::forward::Forwarder;
use xmip_core_transport_amqp::event_wire::{AmqpEventRules, EARLIER_PREFIX, PREFIX};

#[path = "event_wire/fixture.rs"]
mod fixture;

use fixture::{TIMEOUT, allowing, audit_at, far_end, published, read_back, subscribed, wire};

/// AMQP binding section 3.1.4, the binary-mode example, under the prefix
/// of 1.0.2 and the one before it; matched exactly, so no other is read.
#[test]
fn the_amqp_binary_example_reads_with_either_prefix() {
    for prefix in [PREFIX, EARLIER_PREFIX, "cloudevents_"] {
        let carried = Carried {
            content_type: Some("application/json; charset=utf-8".to_string()),
            headers: [
                ("specversion", "1.0"),
                ("type", "com.example.someevent"),
                ("time", "2018-04-05T03:56:24Z"),
                ("id", "1234-1234-1234"),
                ("source", "/mycontext/subcontext"),
            ]
            .map(|(name, value)| (format!("{prefix}{name}"), value.to_string()))
            .to_vec(),
            body: b"{}".to_vec(),
        };

        let read = AmqpEventRules.read(&carried);

        if prefix == "cloudevents_" {
            assert!(read.is_err(), "{prefix}");
        } else {
            assert_eq!(read.expect("a WireEvent").id, "1234-1234-1234", "{prefix}");
        }
    }
}

#[test]
fn a_published_event_arrives_as_the_same_event_in_either_mode() {
    let at = audit_at("modes");
    for mode in [Mode::Structured, Mode::Binary] {
        let hub = allowing();
        let (address, told) = far_end(0);
        let mut forwarder = Forwarder::new(subscribed(&hub, &at), mode, wire(&address));
        let event = published();
        let _ = hub.publish(event.clone());

        let once = Retry::new(1, Duration::ZERO);
        let pumped = forwarder.pump(TIMEOUT, &[&once as &dyn Guard]);

        assert_eq!((pumped.carried, pumped.pending), (1, 0), "{mode:?}");
        let (_, user, publish) = told.recv_timeout(TIMEOUT).expect("published");
        assert_eq!(user, "xmip-events", "the Party's login presented");
        assert_eq!(
            (publish.exchange.as_str(), publish.routing_key.as_str()),
            ("xmip.events", "billing")
        );
        assert!(publish.properties.persistent);
        let names: Vec<&str> = publish
            .properties
            .headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        match mode {
            Mode::Structured => assert!(names.is_empty(), "{names:?}"),
            Mode::Binary => assert!(names.contains(&"cloudEvents_id"), "{names:?}"),
        }
        assert_eq!(read_back(&publish), event, "{mode:?}");
    }
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn a_broker_that_refuses_is_retried_and_the_events_arrive_in_order() {
    let at = audit_at("again");
    let hub = allowing();
    let (address, told) = far_end(2);
    let subscription = subscribed(&hub, &at);
    let mut forwarder = Forwarder::new(subscription, Mode::Binary, wire(&address));
    let first = published();
    let second = published();
    let _ = hub.publish(first.clone());
    let _ = hub.publish(second.clone());

    let thrice = Retry::new(3, Duration::ZERO);
    let pumped = forwarder.pump(TIMEOUT, &[&thrice as &dyn Guard]);

    assert_eq!((pumped.carried, pumped.pending), (2, 0), "{pumped:?}");
    let arrived: Vec<Event> = (0..2)
        .map(|_| read_back(&told.recv_timeout(TIMEOUT).expect("published").2))
        .collect();
    assert_eq!(arrived, [first, second], "once each, in order");
    let _ = fs::remove_dir_all(&at);
}
