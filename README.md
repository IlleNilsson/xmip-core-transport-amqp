# xmip-core-transport-amqp

AMQP 0-9-1 transport: one publish or delivery is one Stream, the exchange and routing key beside it; a Location consumes a queue or publishes through a broker, or accepts clients directly. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

This is the one AMQP 0-9-1 in the estate: `wire` (the frame, and AMQP's own
value encodings — short string, long string, field table — as the `Amqp` and
`AmqpWrite` traits over
[xmip-core-library-codec](https://github.com/IlleNilsson/xmip-core-library-codec)'s
byte cursor and writer), `method`, `content` (the header and body frames), `client` and
`session` (the far end a test or the System Tests stand up in place of a
broker). [xmip-core-transport-rabbitmq](https://github.com/IlleNilsson/xmip-core-transport-rabbitmq)
speaks through them rather than carrying its own.

A Send Location publishes on a connection kept per broker
(`transport::Pool`), its channel in confirm mode, and a publish returns
once the broker confirms it; until 2026-09-27 every send connected,
published without a confirm and closed. What a Location presents is
`Credentials`: the transport capability's `Login` and the virtual host,
kept with the secret the Location names in `Presenting`, which the
`rabbitmq` technology keeps its own in too. The PLAIN login presents the
Location's `user` with the password the secret holds, read as each
connection opens, never per Message on a kept one (ADR-0019, amendment
2026-11-25); a Location that names no secret presents its `user` with an
empty password, one that names no `user` logs in as the broker's default
guest, and one that names a secret and no `user` is refused as the node
starts. Until 2026-11-25 a Location naming a secret was refused as the
node started, and every Location logged in as guest. The far end `Session`
admits only the login it is given, closing with 403 `ACCESS_REFUSED`.

A Receive Location declares and consumes its queue once, on its first receive, and the consumer stays attached between receives; each receive takes what came until the broker is quiet for the timeout (`Client::consuming`, `Client::next_delivery`, `transport::pool::delivered`). A consumer the broker closed is replaced. Until 2026-09-28 every receive connected, declared, consumed and closed.

A delivery is acknowledged after the runtime's whole receive cycle, never on its own (the consumer does not use `no-ack`): `acknowledging` writes `basic.ack` on the consumer that received it when the cycle accepted it, `basic.reject` without requeue when it refused it, so the broker drops it or dead-letters it where the queue names a dead-letter exchange and never delivers it again, and `basic.reject` with requeue when the cycle failed, so the broker delivers it again. A delivery tag names a delivery on one channel only, so where the broker closed that consumer meanwhile no other is opened: the broker requeued every delivery that consumer had not answered, and delivers it again. The answer is one frame written on the kept connection, nothing waited for. `Session` reports a reject as `Event::Rejected`. The `rabbitmq` technology acknowledges with the same function. Until 2026-10-02 a delivery was acknowledged as it was received.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls. Until 2026-09-28 this technology stripped its scheme by hand.

A Send Location publishes through the exchange and under the routing key its `exchange` and `routing_key` settings name, on the broker its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which it reads, through `transport::destination::named`, as those two. A target naming another still wins — `order.cancelled`, or `amqp://host:5672/orders/order.cancelled` (ADR-0064, amendment 2026-11-10).

## Properties, publisher confirms, and the event capability's wire events

A content header carries four of the basic class's properties
(`content::Properties`): content-type, the headers field table,
delivery-mode and, since 2026-10-04, message-id, written by a publish and read off a publish or a delivery
(`Publish::properties`, `Delivery::properties`). Until 2026-09-26 a header
said only `application/octet-stream` and nothing a header said was read. A
publish goes out as one write — method, header and body frames together —
where it was three, each waiting on the one before under Nagle's algorithm.

Publisher confirms (`confirm`): `Client::publish_confirmed` puts the channel
in confirm mode and returns once the broker's `basic.ack` names the publish;
a `basic.nack` is a retryable failure. A `Session` confirms every publish on
a channel in confirm mode, and nacks the first few where told to
(`Session::nacking`).

On them rides the event capability (ADR-0065 clause 3):
`event_wire::EventWire` implements
[xmip-core-event](https://github.com/IlleNilsson/xmip-core-event)'s `Wire`,
and the AMQP protocol binding's rules are here, `event_wire::AmqpEventRules`
(ADR-0065, amendment 2026-11-14): `cloudEvents_` names, matched exactly,
and `cloudEvents:`, the prefix before 1.0.2, read too. The wire answers
them (`Wire::rules`), the event crate writes either mode over them, once,
and the wire publishes what was written, persistent and
confirmed: the content type in the content-type property and the
`cloudEvents_` attributes in the **headers field table**. The wire event's
AMQP binding is written for AMQP 1.0, where the attributes are
application-properties; AMQP 0-9-1 has none, and its headers table is where
an application's own named values go, so it is their equivalent here, the
names and the prefix unchanged. The identity presented for a Party is the
`Credentials` configured for it (ADR-0019 clause 3): the transport
capability's `Login` and the virtual host. The connections to each Party
are the capability's `Pool`, kept between events; one that fails is
replaced and the event published again. `event_wire::carried` is the read side: a
delivery's properties and body, as `AmqpEventRules` reads a `WireEvent` from.
The module's timing program, `tests/measured`, holds publish to far-end
receipt round by round beside a plain wake, the fastest of five
(`xcore::latency::Rounds`): under a millisecond beyond its two wakes, and
this process's processor time across it under a millisecond beyond the
plain wake's.

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) carries the Journey's identifier as the message-id basic property (`content::Properties::message_id`, flag bit 7, after delivery-mode), the same on every attempt on one Journey. An AMQP 0-9-1 broker does not drop a repeated publish by it; a consumer, or a broker's deduplication plugin, recognizes the repeat by its message-id. An unkeyed `send` sets none. A delivery's message-id is read back, the four properties between delivery-mode and it skipped.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `queue`, `exchange`, `routing_key`, `virtual_host`, `user` and `timeout`, and `SCHEMES`, `amqp`; `user` and the name `virtual_host` stay in `presenting`, where `rabbitmq` declares them from too. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on are behind the default feature. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check an AMQP Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
