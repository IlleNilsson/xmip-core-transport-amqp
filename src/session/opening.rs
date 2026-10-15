//! How a session opens — the protocol header, the login, the tune and the
//! virtual host — and how it ends when it refuses: `connection.close`,
//! then [`socket::finish`], because a client may have sent frames after
//! the one refused, and a socket dropped with them unread is reset on
//! Windows, the close thrown away with it.

use std::io::Read;
use std::net::TcpListener;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::socket;

use super::{Queues, Session};
use crate::client::Credentials;
use crate::method::{
    self, CONNECTION_CLOSE, CONNECTION_OPEN, CONNECTION_START_OK, CONNECTION_TUNE,
    CONNECTION_TUNE_OK, Id, Method,
};
use crate::wire::{Kind, MAX_FRAME, PROTOCOL_HEADER, len32, read_frame};

impl Session {
    /// Accept one client on `listener` and complete its handshake, closing
    /// with 403 where its login is not `expected` and 530 where its
    /// virtual host is not.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the client did not
    /// open with AMQP 0-9-1, or it was refused.
    pub fn accept(
        listener: &TcpListener,
        expected: &Credentials,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut session = Self {
            reader,
            writer,
            peer,
            user: String::new(),
            virtual_host: String::new(),
            queues: Queues::new(),
            consumer: None,
            delivery_tag: 0,
            confirming: None,
            nacking: 0,
        };
        let mut header = [0u8; 8];
        session
            .reader
            .read_exact(&mut header)
            .map_err(|e| classify("reading the protocol header", &e))?;
        if header != PROTOCOL_HEADER {
            session.write(PROTOCOL_HEADER)?;
            socket::finish(&session.writer)?;
            return Err(protocol_error("the client did not open with AMQP 0-9-1"));
        }
        session.say(0, &method::connection_start())?;
        let start_ok = session.expect(0, CONNECTION_START_OK, "connection.start-ok")?;
        let (user, password) = method::login_of(&start_ok)?;
        let login = &expected.login;
        if user != login.user
            || !codec::constant_time::equal(password.as_bytes(), login.password.as_bytes())
        {
            return session.refuse(403, "ACCESS_REFUSED - login was refused");
        }
        session.user = user;
        session.say(0, &method::tune(CONNECTION_TUNE, 1, len32(MAX_FRAME)))?;
        session.expect(0, CONNECTION_TUNE_OK, "connection.tune-ok")?;
        let open = session.expect(0, CONNECTION_OPEN, "connection.open")?;
        session.virtual_host = method::virtual_host_of(&open)?;
        if session.virtual_host != expected.virtual_host {
            return session.refuse(530, "NOT_ALLOWED - access to the virtual host was refused");
        }
        session.say(0, &method::connection_open_ok())?;
        Ok(session)
    }

    /// connection.close with `code`, and the refusal it is.
    fn refuse(mut self, code: u16, text: &str) -> Result<Self> {
        self.close(code, text)?;
        Err(protocol_error(format!(
            "the client was refused: {code} {text}"
        )))
    }

    /// connection.close with `code`, and the connection ended through
    /// [`socket::finish`]: the close-ok the client answers with, and
    /// whatever it sent before it read the close, are read and dropped.
    pub(super) fn close(&mut self, code: u16, text: &str) -> Result<()> {
        self.say(0, &method::close(CONNECTION_CLOSE, code, text))?;
        socket::finish(&self.writer)
    }

    /// The next method, which must be `id` on `channel`.
    fn expect(&mut self, channel: u16, id: Id, what: &str) -> Result<Method> {
        match read_frame(&mut self.reader)? {
            Some(frame) if frame.kind == Kind::Method && frame.channel == channel => {
                let method = Method::of(&frame)?;
                if method.is(id) {
                    return Ok(method);
                }
                Err(protocol_error(format!(
                    "method {} where {what} was expected",
                    method.label()
                )))
            }
            Some(frame) => Err(protocol_error(format!(
                "a {:?} frame on channel {} where {what} was expected",
                frame.kind, frame.channel
            ))),
            None => Err(protocol_error(format!("the client closed before {what}"))),
        }
    }
}
