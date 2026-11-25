//! Who an AMQP or a `RabbitMQ` Location logs in as, and the secret whose
//! password its PLAIN login presents (ADR-0019, amendment 2026-11-25): kept
//! once, here, for both technologies, and read as each connects.
//!
//! A Location's `user` is presented with the password of the secret it
//! names, read at each connection, never per Message on a kept one, so a
//! secret set or replaced reaches the next connection without a restart
//! (`transport::secured`). A Location that names no secret presents the
//! user it names with an empty password, as IMAP's does; one that names no
//! user logs in as the broker's default guest, as it always did, and one
//! that names a secret and no user is refused as the node starts, since the
//! secret would go out as guest's password.

use transport::error::Result;
use transport::{Login, Secured, secured};
use xcore::settings::{Applies, Kind, Presence, Read, Setting};

use crate::client::Credentials;

/// The `user` setting, as AMQP and `RabbitMQ` each declare it.
pub const USER: Setting = Setting {
    name: "user",
    kind: Kind::Text,
    presence: Presence::Optional,
    meaning: "The user the PLAIN login presents, its password the Location's secret; the \
              broker's default guest when left out.",
    applies: Applies::Both,
};

/// The `virtual_host` setting's name, which each technology declares in its
/// own words.
pub const VIRTUAL_HOST: &str = "virtual_host";

/// What a Location presents as it connects, and what a far end expects.
#[derive(Clone, Debug)]
pub struct Presenting {
    credentials: Credentials,
    /// Whether the user was named, so a secret has one to go with.
    named: bool,
    secret: Option<Secured>,
}

impl Default for Presenting {
    /// The broker's default guest on the default virtual host, as a
    /// Location that names no user logs in.
    fn default() -> Self {
        Self {
            credentials: Credentials::default(),
            named: false,
            secret: None,
        }
    }
}

impl Presenting {
    /// Logging in as `login`, on the same virtual host: a far end's
    /// expected login, or a test's.
    #[must_use]
    pub fn logging_in(mut self, login: Login) -> Self {
        self.credentials.login = login;
        self.named = true;
        self
    }

    /// What a Location's `settings` name: its [`USER`] with no password, or
    /// the broker's default guest, on its [`VIRTUAL_HOST`] or the default
    /// one.
    #[must_use]
    pub fn configured(settings: &Read) -> Self {
        let mut presenting = Self::default();
        if let Some(user) = settings.optional_text(USER.name) {
            presenting = presenting.logging_in(Login::new(user, ""));
        }
        if let Some(virtual_host) = settings.optional_text(VIRTUAL_HOST) {
            presenting.credentials = presenting.credentials.on(virtual_host);
        }
        presenting
    }

    /// The password of `secret` presented for the user, read at each
    /// connection.
    ///
    /// # Errors
    /// Where no user was named: the secret would go out as guest's.
    pub fn secured_by(mut self, secret: Secured) -> Result<Self> {
        if !self.named {
            return Err(secured::without_user(&secret, USER.name));
        }
        self.secret = Some(secret);
        Ok(self)
    }

    /// The credentials as given, without the secret: what a far end expects.
    #[must_use]
    pub const fn expected(&self) -> &Credentials {
        &self.credentials
    }

    /// The credentials `technology` presents now, the password the
    /// Location's secret holds read where it names one.
    ///
    /// # Errors
    /// Where the secret is not set, is no password, or could not be read.
    pub fn presented(&self, technology: &str) -> Result<Credentials> {
        Ok(Credentials {
            login: secured::logged_in(&self.credentials.login, self.secret.as_ref(), technology)?,
            virtual_host: self.credentials.virtual_host.clone(),
        })
    }
}
