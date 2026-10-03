//! How a delivery a kept consumer received is told the runtime's verdict:
//! `basic.ack` on the consumer that received it once the receive cycle
//! accepted it, `basic.reject` without requeue once it refused it — the
//! broker drops it, or dead-letters it where the queue names a
//! dead-letter exchange — and `basic.reject` with requeue once the cycle
//! failed (AMQP 0-9-1, section 1.8.3.14, `basic.reject`).
//!
//! The consumer never acknowledges on its own (no `no-ack`), so a delivery
//! not yet answered stays the broker's: unacknowledged, and delivered again
//! when it is rejected with requeue or when the consumer's connection
//! closes. The `amqp` and `rabbitmq` technologies both acknowledge with it.

use transport::{Acknowledgement, Pool, Verdict};

use crate::client::Client;

/// The acknowledgement of the delivery under `delivery_tag`, received on
/// the consumer `consumers` keeps for `broker`: `basic.ack` on
/// [`Verdict::Accepted`], `basic.reject` without requeue on
/// [`Verdict::Refused`], so it is not delivered again, and with requeue on
/// [`Verdict::Failed`]; one frame written on that consumer's channel,
/// nothing waited for.
///
/// A delivery tag names a delivery on one channel only, so the answer goes
/// on the kept consumer or not at all: where the broker closed it meanwhile,
/// no other is opened, and the broker, which requeued every delivery the
/// closed one had not answered, delivers this one again.
#[must_use]
pub fn acknowledging(consumers: &Pool<Client>, broker: &str, delivery_tag: u64) -> Acknowledgement {
    let consumers = consumers.clone();
    let broker = broker.to_string();
    Acknowledgement::deferred(move |verdict| {
        consumers.kept(
            broker.as_str(),
            "the consumer that received the delivery is closed; \
             the broker delivers it again",
            |client| match verdict {
                Verdict::Accepted => client.ack(delivery_tag),
                Verdict::Refused(_) => client.reject(delivery_tag, false),
                Verdict::Failed => client.reject(delivery_tag, true),
            },
        )
    })
}
