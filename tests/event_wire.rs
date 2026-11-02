//! The event capability over AMQP 0-9-1 (ADR-0065 clause 3): a
//! subscription forwarded through this transport's wire to its own far
//! end, read back as the Event that was published — in both modes of the
//! binding, presenting the Party's login, at least once through a broker
//! that refuses. How near real time it is (the owner, 2026-09-26) is held
//! in the module's timing program, `tests/measured`.

use std::fs;
use std::time::Duration;

use event::Event;
use event::binding::{Binding, Mode};
use event::forward::Forwarder;
use resilience::Guard;
use retry::Retry;

#[path = "event_wire/fixture.rs"]
mod fixture;

use fixture::{TIMEOUT, allowing, audit_at, far_end, published, read_back, subscribed, wire};

#[test]
fn a_published_event_arrives_as_the_same_event_in_either_mode() {
    let at = audit_at("modes");
    for mode in [Mode::Structured, Mode::Binary] {
        let hub = allowing();
        let (address, told) = far_end(0);
        let mut forwarder =
            Forwarder::new(subscribed(&hub, &at), Binding::Amqp, mode, wire(&address));
        let event = published();
        hub.publish(event.clone());

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
    let mut forwarder = Forwarder::new(subscription, Binding::Amqp, Mode::Binary, wire(&address));
    let first = published();
    let second = published();
    hub.publish(first.clone());
    hub.publish(second.clone());

    let thrice = Retry::new(3, Duration::ZERO);
    let pumped = forwarder.pump(TIMEOUT, &[&thrice as &dyn Guard]);

    assert_eq!((pumped.carried, pumped.pending), (2, 0), "{pumped:?}");
    let arrived: Vec<Event> = (0..2)
        .map(|_| read_back(&told.recv_timeout(TIMEOUT).expect("published").2))
        .collect();
    assert_eq!(arrived, [first, second], "once each, in order");
    let _ = fs::remove_dir_all(&at);
}
