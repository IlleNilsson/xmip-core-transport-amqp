//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand publishes on one connection take a millisecond each at most
//!   beyond their wakes;
//! - an Event forwarded over AMQP 0-9-1 reaches the far end within a
//!   millisecond apart from load, and this process's processor time across
//!   it is under a millisecond beyond a plain wake's (the shared far end is
//!   `tests/event_wire/fixture.rs`).
//!
//! Each round beside a plain wake, the fastest of five
//! (`xcore::latency::Rounds`).

use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use event::binding::Mode;
use resilience::Guard;
use retry::Retry;
use transport::Configured;
use transport::Transport;
use xcluster::delivery::forward::Forwarder;
use xcore::latency::{Rounds, spin};
use xcore::processor_time::ProcessorTime;
use xmip_core_transport_amqp::AmqpTransport;

#[allow(dead_code)]
#[path = "../event_wire/fixture.rs"]
mod fixture;

use fixture::{TIMEOUT, allowing, audit_at, far_end, published, subscribed, wire};

#[test]
fn a_thousand_publishes_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end = AmqpTransport::new("127.0.0.1:0", "orders", "order.placed", "orders.in")
        .timing_out_after(Duration::from_secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = AmqpTransport::new(address, "orders", "order.placed", "q")
        .timing_out_after(Duration::from_secs(5));
    let sender = thread::spawn(move || {
        let mut rounds = Rounds::crossing(PUBLISH_WAKES);
        for n in 0..SENDS {
            rounds.round(|| near.send("order.placed", n.to_string().as_bytes()))?;
        }
        Ok::<_, transport::TransportError>(rounds)
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let publish = session.next_publish().expect("publish").expect("one");
        assert_eq!(publish.body, n.to_string().as_bytes());
    }
    let rounds = sender.join().expect("thread").expect("sending");
    rounds.hold("a publish, confirmed");
}

/// What a publish wakes where a plain wake wakes one: the far end at the
/// publish, and this end at its `basic.ack` (`xcore::latency::Rounds`).
const PUBLISH_WAKES: u32 = 2;

/// How many Events the latency test forwards.
const ROUNDS: usize = 300;

/// What an Event's way to the far end wakes where a plain wake wakes one:
/// the forwarder at the publish, and the far end at its frame.
const EVENT_WAKES: u32 = 2;

#[test]
fn an_event_reaches_the_far_end_within_a_millisecond_apart_from_load() {
    let at = audit_at("latency");
    let hub = Arc::new(allowing());
    let (address, told) = far_end(0);
    let mut forwarder = Forwarder::new(subscribed(&hub, &at), Mode::Binary, wire(&address));
    // The connection is opened once, before the rounds: what is measured
    // is an Event's way, not a handshake.
    let _ = hub.publish(published());
    let once = Retry::new(1, Duration::ZERO);
    assert_eq!(forwarder.pump(TIMEOUT, &[&once as &dyn Guard]).carried, 1);
    told.recv_timeout(TIMEOUT).expect("the first, published");
    let forwarding = thread::spawn(move || {
        let once = Retry::new(1, Duration::ZERO);
        let mut carried = 0;
        while carried < ROUNDS {
            carried += forwarder
                .pump(Duration::from_secs(5), &[&once as &dyn Guard])
                .carried;
        }
    });
    // From the publish to the far end's arrival, and this process's
    // processor time across it — the publisher, the forwarder and the far
    // end — round by round beside a plain wake. Until 2026-11-15 the
    // processor time was an Event's share of the whole run, held flat.
    let mut rounds = Rounds::crossing(EVENT_WAKES);
    for _ in 0..ROUNDS {
        spin(Duration::from_micros(500));
        let event = published();
        let from = ProcessorTime::process();
        let sent = Instant::now();
        let _ = hub.publish(event);
        let (arrived, _, _) = told
            .recv_timeout(Duration::from_secs(5))
            .expect("published");
        rounds.took.push(arrived - sent);
        rounds.worked.push(from.elapsed());
        rounds.beside();
    }
    forwarding.join().expect("forwarded");
    rounds.hold("publish to the AMQP far end");
    let _ = fs::remove_dir_all(&at);
}
