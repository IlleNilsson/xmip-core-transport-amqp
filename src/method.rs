//! The methods a queue needs: their class and method ids, and the
//! arguments each is written and read with. The content a publish or a
//! delivery carries after its method is `content.rs`.
//!
//! Connection start, start-ok, tune, tune-ok, open, open-ok, close and
//! close-ok; channel open, open-ok, close and close-ok; queue declare and
//! declare-ok; basic consume, consume-ok, publish, deliver, ack and reject. That is
//! the whole of what a Location that declares, publishes, consumes and
//! acknowledges says and hears. Publisher confirms are `confirm.rs`'s;
//! exchanges, bindings and transactions are not here.
//!
//! Bits pack eight to an octet in the order a method lists them; no method
//! here has more than five in a row, so every bit field is one octet.

use codec::cursor::Cursor;
use codec::writer::ByteWriter;
use transport::error::{Result, protocol_error};

use crate::wire::{Amqp, AmqpWrite, Frame, Kind};

/// A class id and a method id, as every method frame opens.
pub type Id = (u16, u16);

pub const CONNECTION_START: Id = (10, 10);
pub const CONNECTION_START_OK: Id = (10, 11);
pub const CONNECTION_TUNE: Id = (10, 30);
pub const CONNECTION_TUNE_OK: Id = (10, 31);
pub const CONNECTION_OPEN: Id = (10, 40);
pub const CONNECTION_OPEN_OK: Id = (10, 41);
pub const CONNECTION_CLOSE: Id = (10, 50);
pub const CONNECTION_CLOSE_OK: Id = (10, 51);
pub const CHANNEL_OPEN: Id = (20, 10);
pub const CHANNEL_OPEN_OK: Id = (20, 11);
pub const CHANNEL_CLOSE: Id = (20, 40);
pub const CHANNEL_CLOSE_OK: Id = (20, 41);
pub const QUEUE_DECLARE: Id = (50, 10);
pub const QUEUE_DECLARE_OK: Id = (50, 11);
pub const BASIC_CONSUME: Id = (60, 20);
pub const BASIC_CONSUME_OK: Id = (60, 21);
pub const BASIC_PUBLISH: Id = (60, 40);
pub const BASIC_DELIVER: Id = (60, 60);
pub const BASIC_ACK: Id = (60, 80);
pub const BASIC_REJECT: Id = (60, 90);

/// queue.declare's bits — passive, durable, exclusive, auto-delete,
/// no-wait — with durable set.
const DURABLE: u8 = 0b10;

/// One method: which, and its arguments as written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Method {
    pub id: Id,
    pub arguments: Vec<u8>,
}

impl Method {
    #[must_use]
    pub const fn new(id: Id, arguments: Vec<u8>) -> Self {
        Self { id, arguments }
    }

    /// A method with no arguments: the ok that answers a close.
    #[must_use]
    pub const fn bare(id: Id) -> Self {
        Self::new(id, Vec::new())
    }

    /// As a method frame on `channel`.
    #[must_use]
    pub fn frame(&self, channel: u16) -> Frame {
        let mut payload = Vec::with_capacity(self.arguments.len() + 4);
        payload
            .u16_be(self.id.0)
            .u16_be(self.id.1)
            .bytes(&self.arguments);
        Frame::new(Kind::Method, channel, payload)
    }

    /// The method `frame` carries.
    ///
    /// # Errors
    /// A frame that is not a method frame, or too short to name one.
    pub fn of(frame: &Frame) -> Result<Self> {
        if frame.kind != Kind::Method {
            return Err(protocol_error("a frame where a method was expected"));
        }
        let mut cursor = Cursor::new(&frame.payload);
        let class = cursor.u16_be()?;
        let method = cursor.u16_be()?;
        Ok(Self::new((class, method), cursor.remaining().to_vec()))
    }

    #[must_use]
    pub fn is(&self, id: Id) -> bool {
        self.id == id
    }

    /// A cursor over the arguments, read in the order the method lists
    /// them.
    #[must_use]
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(&self.arguments)
    }

    /// `class.method` as a diagnostic prints it.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}.{}", self.id.0, self.id.1)
    }
}

/// connection.start: version 0-9, the server properties, PLAIN, `en_US`.
#[must_use]
pub fn connection_start() -> Method {
    let mut out = Vec::new();
    out.byte(0)
        .byte(9)
        .string_table(&[("product", "xmip")])
        .long_string(b"PLAIN")
        .long_string(b"en_US");
    Method::new(CONNECTION_START, out)
}

/// connection.start-ok: the client properties, PLAIN, the response PLAIN
/// defines — NUL, user, NUL, password — and the locale.
#[must_use]
pub fn connection_start_ok(user: &str, password: &str) -> Method {
    let mut out = Vec::new();
    out.string_table(&[("product", "xmip")])
        .short_string("PLAIN")
        .long_string(format!("\0{user}\0{password}").as_bytes())
        .short_string("en_US");
    Method::new(CONNECTION_START_OK, out)
}

/// Who connection.start-ok presents: the user and password in the PLAIN
/// response.
///
/// # Errors
/// Arguments that end early, or a mechanism other than PLAIN.
pub fn login_of(method: &Method) -> Result<(String, String)> {
    let mut cursor = method.cursor();
    cursor.skip_table()?;
    let mechanism = cursor.short_string()?;
    if mechanism != "PLAIN" {
        return Err(protocol_error(format!(
            "a mechanism not spoken here: {mechanism}"
        )));
    }
    let text = String::from_utf8_lossy(cursor.long_string()?);
    let mut parts = text.split('\0').skip(1);
    let user = parts.next().unwrap_or_default().to_string();
    let password = parts.next().unwrap_or_default().to_string();
    Ok((user, password))
}

/// A tune — [`CONNECTION_TUNE`] as the broker proposes it, or
/// [`CONNECTION_TUNE_OK`] as the client settles — with no heartbeat.
#[must_use]
pub fn tune(id: Id, channel_max: u16, frame_max: u32) -> Method {
    let mut out = Vec::new();
    out.u16_be(channel_max).u32_be(frame_max).u16_be(0);
    Method::new(id, out)
}

/// The channel and frame limits a tune or tune-ok carries.
///
/// # Errors
/// Arguments that end early.
pub fn tune_of(method: &Method) -> Result<(u16, u32)> {
    let mut cursor = method.cursor();
    Ok((cursor.u16_be()?, cursor.u32_be()?))
}

/// connection.open on `virtual_host`.
#[must_use]
pub fn connection_open(virtual_host: &str) -> Method {
    let mut out = Vec::new();
    out.short_string(virtual_host).short_string("").byte(0);
    Method::new(CONNECTION_OPEN, out)
}

/// The virtual host a connection.open names.
///
/// # Errors
/// Arguments that end early.
pub fn virtual_host_of(method: &Method) -> Result<String> {
    method.cursor().short_string()
}

/// connection.open-ok, its known-hosts empty as the spec deprecates it.
#[must_use]
pub fn connection_open_ok() -> Method {
    let mut out = Vec::new();
    out.short_string("");
    Method::new(CONNECTION_OPEN_OK, out)
}

/// A close — [`CONNECTION_CLOSE`] or [`CHANNEL_CLOSE`] — with its reply
/// code and text, and no method to blame.
#[must_use]
pub fn close(id: Id, code: u16, text: &str) -> Method {
    let mut out = Vec::new();
    out.u16_be(code).short_string(text).u16_be(0).u16_be(0);
    Method::new(id, out)
}

/// The reply code and text a close carries.
///
/// # Errors
/// Arguments that end early.
pub fn close_of(method: &Method) -> Result<(u16, String)> {
    let mut cursor = method.cursor();
    Ok((cursor.u16_be()?, cursor.short_string()?))
}

/// channel.open, its out-of-band empty as the spec deprecates it.
#[must_use]
pub fn channel_open() -> Method {
    let mut out = Vec::new();
    out.short_string("");
    Method::new(CHANNEL_OPEN, out)
}

/// channel.open-ok, its channel-id empty likewise.
#[must_use]
pub fn channel_open_ok() -> Method {
    let mut out = Vec::new();
    out.long_string(b"");
    Method::new(CHANNEL_OPEN_OK, out)
}

/// queue.declare `queue`, durable, and nothing else.
#[must_use]
pub fn queue_declare(queue: &str) -> Method {
    let mut out = Vec::new();
    out.u16_be(0)
        .short_string(queue)
        .byte(DURABLE)
        .empty_table();
    Method::new(QUEUE_DECLARE, out)
}

/// The queue a declare or a consume names, after the reserved ticket.
///
/// # Errors
/// Arguments that end early.
pub fn queue_of(method: &Method) -> Result<String> {
    let mut cursor = method.cursor();
    cursor.u16_be()?;
    cursor.short_string()
}

/// queue.declare-ok: the queue, how many messages it holds, no consumers.
#[must_use]
pub fn queue_declare_ok(queue: &str, messages: u32) -> Method {
    let mut out = Vec::new();
    out.short_string(queue).u32_be(messages).u32_be(0);
    Method::new(QUEUE_DECLARE_OK, out)
}

/// The queue and message count a declare-ok reports.
///
/// # Errors
/// Arguments that end early.
pub fn declare_ok_of(method: &Method) -> Result<(String, u32)> {
    let mut cursor = method.cursor();
    Ok((cursor.short_string()?, cursor.u32_be()?))
}

/// basic.consume `queue` as `tag`, acknowledged by the consumer.
#[must_use]
pub fn basic_consume(queue: &str, tag: &str) -> Method {
    let mut out = Vec::new();
    out.u16_be(0)
        .short_string(queue)
        .short_string(tag)
        .byte(0)
        .empty_table();
    Method::new(BASIC_CONSUME, out)
}

/// The queue and consumer tag a consume names.
///
/// # Errors
/// Arguments that end early.
pub fn consume_of(method: &Method) -> Result<(String, String)> {
    let mut cursor = method.cursor();
    cursor.u16_be()?;
    Ok((cursor.short_string()?, cursor.short_string()?))
}

/// basic.consume-ok under `tag`.
#[must_use]
pub fn basic_consume_ok(tag: &str) -> Method {
    let mut out = Vec::new();
    out.short_string(tag);
    Method::new(BASIC_CONSUME_OK, out)
}

/// The consumer tag a consume-ok confirms.
///
/// # Errors
/// Arguments that end early.
pub fn tag_of(method: &Method) -> Result<String> {
    method.cursor().short_string()
}

/// basic.publish to `exchange` under `routing_key`, neither mandatory nor
/// immediate.
#[must_use]
pub fn basic_publish(exchange: &str, routing_key: &str) -> Method {
    let mut out = Vec::new();
    out.u16_be(0)
        .short_string(exchange)
        .short_string(routing_key)
        .byte(0);
    Method::new(BASIC_PUBLISH, out)
}

/// The exchange and routing key a publish names.
///
/// # Errors
/// Arguments that end early.
pub fn publish_of(method: &Method) -> Result<(String, String)> {
    let mut cursor = method.cursor();
    cursor.u16_be()?;
    Ok((cursor.short_string()?, cursor.short_string()?))
}

/// basic.deliver to consumer `tag`: `delivery_tag`, not redelivered, from
/// `exchange` under `routing_key`.
#[must_use]
pub fn basic_deliver(tag: &str, delivery_tag: u64, exchange: &str, routing_key: &str) -> Method {
    let mut out = Vec::new();
    out.short_string(tag)
        .u64_be(delivery_tag)
        .byte(0)
        .short_string(exchange)
        .short_string(routing_key);
    Method::new(BASIC_DELIVER, out)
}

/// The delivery tag, exchange and routing key a deliver carries.
///
/// # Errors
/// Arguments that end early.
pub fn deliver_of(method: &Method) -> Result<(u64, String, String)> {
    let mut cursor = method.cursor();
    cursor.short_string()?;
    let delivery_tag = cursor.u64_be()?;
    cursor.byte()?;
    Ok((delivery_tag, cursor.short_string()?, cursor.short_string()?))
}

/// basic.ack of `delivery_tag`, and that one only.
#[must_use]
pub fn basic_ack(delivery_tag: u64) -> Method {
    let mut out = Vec::new();
    out.u64_be(delivery_tag).byte(0);
    Method::new(BASIC_ACK, out)
}

/// basic.reject of `delivery_tag`, back onto its queue for redelivery
/// where `requeue` is set.
#[must_use]
pub fn basic_reject(delivery_tag: u64, requeue: bool) -> Method {
    let mut out = Vec::new();
    out.u64_be(delivery_tag).byte(u8::from(requeue));
    Method::new(BASIC_REJECT, out)
}

/// The delivery tag a reject names, and whether it is requeued.
///
/// # Errors
/// Arguments that end early.
pub fn reject_of(method: &Method) -> Result<(u64, bool)> {
    let mut cursor = method.cursor();
    let delivery_tag = cursor.u64_be()?;
    Ok((delivery_tag, cursor.byte()? & 1 == 1))
}

/// The delivery tag an ack names.
///
/// # Errors
/// Arguments that end early.
pub fn delivery_tag_of(method: &Method) -> Result<u64> {
    Ok(method.cursor().u64_be()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::read_frame;

    fn back(method: &Method) -> Method {
        let frame = method.frame(1);
        let bytes = frame.encode();
        let read = read_frame(&mut bytes.as_slice())
            .expect("read")
            .expect("one");
        assert_eq!(read.channel, 1);
        Method::of(&read).expect("method")
    }

    #[test]
    fn the_connection_methods_round_trip() {
        let start = back(&connection_start());
        assert!(start.is(CONNECTION_START));
        assert_eq!(start.label(), "10.10");
        let mut cursor = start.cursor();
        assert_eq!(
            (cursor.byte().expect("0"), cursor.byte().expect("9")),
            (0, 9)
        );
        let start_ok = back(&connection_start_ok("xmip", "secret"));
        assert_eq!(
            login_of(&start_ok).expect("login"),
            ("xmip".to_string(), "secret".to_string())
        );
        let tuned = back(&tune(CONNECTION_TUNE, 1, 4096));
        assert!(tuned.is(CONNECTION_TUNE));
        assert_eq!(tune_of(&tuned).expect("tune"), (1, 4096));
        assert_eq!(
            tune_of(&back(&tune(CONNECTION_TUNE_OK, 7, 8))).expect("tune-ok"),
            (7, 8)
        );
        let open = back(&connection_open("/vhost"));
        assert_eq!(virtual_host_of(&open).expect("vhost"), "/vhost");
        assert!(back(&connection_open_ok()).is(CONNECTION_OPEN_OK));
        let close = back(&close(CONNECTION_CLOSE, 403, "ACCESS_REFUSED"));
        assert_eq!(
            close_of(&close).expect("close"),
            (403, "ACCESS_REFUSED".to_string())
        );
        assert!(
            back(&Method::bare(CONNECTION_CLOSE_OK))
                .arguments
                .is_empty()
        );
        assert!(back(&channel_open()).is(CHANNEL_OPEN));
        assert!(back(&channel_open_ok()).is(CHANNEL_OPEN_OK));
    }

    #[test]
    fn the_queue_and_basic_methods_round_trip() {
        let declare = back(&queue_declare("orders"));
        assert_eq!(queue_of(&declare).expect("queue"), "orders");
        assert_eq!(
            declare.arguments[2 + 1 + 6],
            DURABLE,
            "durable and nothing else"
        );
        assert_eq!(
            declare_ok_of(&back(&queue_declare_ok("orders", 3))).expect("ok"),
            ("orders".to_string(), 3)
        );
        let consume = back(&basic_consume("orders", "tag-1"));
        assert_eq!(queue_of(&consume).expect("queue"), "orders");
        assert_eq!(
            consume_of(&consume).expect("consume"),
            ("orders".to_string(), "tag-1".to_string())
        );
        assert_eq!(
            tag_of(&back(&basic_consume_ok("tag-1"))).expect("tag"),
            "tag-1"
        );
        assert_eq!(
            publish_of(&back(&basic_publish("", "orders"))).expect("publish"),
            (String::new(), "orders".to_string())
        );
        let deliver = back(&basic_deliver("tag-1", 9, "", "orders"));
        assert_eq!(
            deliver_of(&deliver).expect("deliver"),
            (9, String::new(), "orders".to_string())
        );
        assert_eq!(delivery_tag_of(&back(&basic_ack(9))).expect("ack"), 9);
        let reject = back(&basic_reject(9, true));
        assert!(reject.is(BASIC_REJECT));
        assert_eq!(reject_of(&reject).expect("reject"), (9, true));
        assert_eq!(
            reject_of(&basic_reject(4, false)).expect("reject"),
            (4, false)
        );
        let body = Frame::new(Kind::Body, 1, vec![1]);
        assert!(Method::of(&body).is_err(), "not a method");
        let short = Frame::new(Kind::Method, 1, vec![0]);
        let error = Method::of(&short).expect_err("too short to name one");
        assert!(error.message.contains("runs past"), "{}", error.message);
        assert!(deliver_of(&basic_ack(9)).is_err(), "ends early");
    }
}
