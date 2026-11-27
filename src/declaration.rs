//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. `user` and
//! the name `virtual_host` are `presenting`'s, which `rabbitmq` declares from
//! too.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

use crate::presenting;

/// Every setting a Location takes beyond its address, the broker,
/// `host:5672`. The password is the secret the Location names, never a
/// setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "queue",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The queue a Receive Location declares and consumes.",
            applies: Applies::Receive,
        },
        Setting {
            name: "exchange",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The exchange a Send Location publishes to; the broker's default \
                      exchange when left out.",
            applies: Applies::Send,
        },
        Setting {
            name: "routing_key",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The routing key published under when a send target names none.",
            applies: Applies::Send,
        },
        Setting {
            name: presenting::VIRTUAL_HOST,
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The broker's virtual host connected to; the default virtual host \
                      when left out.",
            applies: Applies::Both,
        },
        presenting::USER,
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a peer that stops mid-frame is waited on, and how long a \
             receive waits on a quiet broker; unbounded when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["amqp"];
