//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand publishes on one connection take a millisecond each at most;
//! - an Event forwarded over AMQP 0-9-1 reaches the far end within a
//!   millisecond apart from load, and Xmip's own processor time an Event is
//!   under a millisecond (the shared far end is `tests/event_wire/fixture.rs`).

use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use event::binding::{Binding, Mode};
use event::forward::Forwarder;
use resilience::Guard;
use retry::Retry;
use transport::Transport;
use transport::latency::TcpControl;
use xcore::latency::{spin, spread};
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
        let began = Instant::now();
        for n in 0..SENDS {
            near.send("order.placed", n.to_string().as_bytes())?;
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a publish.
        assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
        Ok::<_, transport::TransportError>(())
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let publish = session.next_publish().expect("publish").expect("one");
        assert_eq!(publish.body, n.to_string().as_bytes());
    }
    sender.join().expect("thread").expect("sending");
}

/// How many Events the latency test forwards.
const ROUNDS: usize = 300;

/// The bound: about a millisecond, apart from load.
const BOUND: Duration = Duration::from_millis(1);

#[test]
fn an_event_reaches_the_far_end_within_a_millisecond_apart_from_load() {
    let at = audit_at("latency");
    let hub = Arc::new(allowing());
    let (address, told) = far_end(0);
    let mut forwarder = Forwarder::new(
        subscribed(&hub, &at),
        Binding::Amqp,
        Mode::Binary,
        wire(&address),
    );
    // The connection is opened once, before the rounds: what is measured
    // is an Event's way, not a handshake.
    hub.publish(published());
    let once = Retry::new(1, Duration::ZERO);
    assert_eq!(forwarder.pump(TIMEOUT, &[&once as &dyn Guard]).carried, 1);
    told.recv_timeout(TIMEOUT).expect("the first, published");
    // The forwarding thread's processor time over every Event: its waits
    // are not counted, only its work.
    let forwarding = thread::spawn(move || {
        let spent = ProcessorTime::thread();
        let once = Retry::new(1, Duration::ZERO);
        let mut carried = 0;
        while carried < ROUNDS {
            carried += forwarder
                .pump(Duration::from_secs(5), &[&once as &dyn Guard])
                .carried;
        }
        spent.elapsed()
    });
    let sent = Arc::new(Mutex::new(Vec::with_capacity(ROUNDS)));
    let receiving = {
        let sent = Arc::clone(&sent);
        thread::spawn(move || {
            (0..ROUNDS)
                .map(|index| {
                    let (arrived, _, _) = told
                        .recv_timeout(Duration::from_secs(5))
                        .expect("published");
                    arrived - sent.lock().expect("sent")[index]
                })
                .collect::<Vec<Duration>>()
        })
    };
    let mut control = TcpControl::start();
    let mut publishing = Duration::ZERO;

    for _ in 0..ROUNDS {
        spin(Duration::from_micros(500));
        let event = published();
        sent.lock().expect("sent").push(Instant::now());
        let from = ProcessorTime::thread();
        hub.publish(event);
        publishing += from.elapsed();
        spin(Duration::from_micros(500));
        control.poke();
    }

    let taken = spread(
        "publish to the AMQP far end",
        receiving.join().expect("received"),
    );
    let forwarding_time = forwarding.join().expect("forwarded");
    let machine = control.load();
    // The wake is a wake: the fastest of each five against the machine's.
    assert!(
        taken.fastest_of_five < BOUND + machine.fastest_of_five,
        "the fastest of five {:?}, the machine's own {:?}",
        taken.fastest_of_five,
        machine.fastest_of_five
    );
    // Xmip's own work: the publisher's and the forwarder's processor time,
    // an Event's share.
    let own = (publishing + forwarding_time) / u32::try_from(ROUNDS).expect("a count");
    println!("Xmip's own processor time: {own:?} an Event");
    assert!(own < BOUND, "Xmip's own processor time, {own:?} an Event");
    let _ = fs::remove_dir_all(&at);
}
