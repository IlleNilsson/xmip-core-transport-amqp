//! What the event-wire tests and the timing program share: the Party's
//! hub and login, its subscription and Event, the far end that admits it
//! and the wire that reaches it.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use event::Event;
use event::binding::Binding;
use event::filter::Filter;
use event::outcome::Outcome;
use node::Stage;
use xcluster::delivery::hub::{EventSubscription, Hub};
use xcluster::delivery::subscriber::fixture;
use xcore::{JourneyId, PartyId};
use xmip_core_transport_amqp::event_wire::{EventWire, Exchange, carried};
use xmip_core_transport_amqp::{Credentials, Publish, Session};

/// The subscriber, a remote Party.
pub const PARTY: PartyId = PartyId::new(22);

/// A hub whose policy allows the Party these tests subscribe as: being in
/// this process admits nobody (ADR-0065, amendment 2026-09-26).
pub fn allowing() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PARTY))])
}

pub const TIMEOUT: Duration = Duration::from_secs(2);

/// What the Party is configured to be presented as, and what the far end
/// admits.
pub fn login() -> Credentials {
    Credentials::new("xmip-events", "presented-for-party-22").on("/events")
}

/// A temporary audit directory of its own, never empty and never the
/// estate's.
pub fn audit_at(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-amqp-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

pub fn subscribed(hub: &Hub, at: &Path) -> EventSubscription {
    let audit = ProgramAudit::new("xmip-core-transport-amqp tests", Some(at));
    hub.subscribe(
        fixture::subscriber(&fixture::party(PARTY), audit),
        Filter::everything(),
        0,
    )
    .expect("allowed")
}

pub fn published() -> Event {
    Event::completed(
        Stage::Send,
        Outcome::Failure,
        format!(
            "{}/send/billing",
            configure::fixture::test_cluster().node_scope(0)
        ),
    )
    .in_journey(JourneyId::new(7))
    .on_artifact("billing")
    .about(PARTY)
    .saying("status", "503 Service Unavailable")
}

/// The far end: one session after another, admitting only the Party's
/// login, the first `nacking` confirmed publishes refused, each publish
/// kept handed over with the moment it arrived and who published it.
pub fn far_end(nacking: u32) -> (String, Receiver<(Instant, String, Publish)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let address = listener.local_addr().expect("address").to_string();
    let (tell, told) = mpsc::channel();
    thread::spawn(move || {
        let mut nacking = nacking;
        while let Ok(session) = Session::accept(&listener, &login(), Some(TIMEOUT)) {
            // A refused attempt drops its connection, so each session
            // refuses once while refusals are left.
            let mut session = session.nacking(u32::from(nacking > 0));
            nacking = nacking.saturating_sub(1);
            while let Ok(Some(publish)) = session.next_publish() {
                let now = Instant::now();
                if tell
                    .send((now, session.user().to_string(), publish))
                    .is_err()
                {
                    return;
                }
            }
        }
    });
    (address, told)
}

pub fn wire(address: &str) -> EventWire {
    let exchange = Exchange::new(address, "xmip.events", "billing").presenting(login());
    EventWire::new()
        .to(PARTY, exchange)
        .timing_out_after(TIMEOUT)
}

pub fn read_back(publish: &Publish) -> Event {
    let wire_event = Binding::Amqp
        .read(&carried(&publish.properties, &publish.body))
        .expect("a WireEvent");
    wire_event.event().expect("an Xmip Event")
}
