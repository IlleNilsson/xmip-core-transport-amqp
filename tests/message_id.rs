//! A keyed send carries its deduplication key as the message's
//! message-id property, the same on every attempt of one Journey; an
//! unkeyed send carries none.

use std::thread;

use transport::Transport;
use xmip_core_transport_amqp::AmqpTransport;

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_publish_carries_the_journey_id_as_its_message_id_on_every_attempt() {
    let far_end = AmqpTransport::loopback();
    let (listener, address) = far_end.bind().expect("bound");
    let sender = thread::spawn(move || {
        let near = AmqpTransport::loopback();
        let target = format!("amqp://{address}/probe/probe");
        near.send_keyed(&target, b"order", KEY)?;
        near.send_keyed(&target, b"order", KEY)?;
        near.send(&target, b"order")
    });
    let mut session = far_end.accept_one(&listener).expect("accepted");
    let heard: Vec<Option<String>> = (0..3)
        .map(|_| {
            let publish = session.next_publish().expect("read").expect("published");
            assert_eq!(publish.body, b"order");
            publish.properties.message_id
        })
        .collect();
    sender.join().expect("sender").expect("sent");
    let key = Some(KEY.to_string());
    assert_eq!(heard, [key.clone(), key, None]);
}
