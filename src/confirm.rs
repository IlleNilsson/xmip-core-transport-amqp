//! Publisher confirms: how a publisher learns that the broker took what it
//! published — the extension every AMQP 0-9-1 broker of note speaks, and
//! the only acknowledgement a publish has.
//!
//! A channel put in confirm mode by `confirm.select` numbers its publishes
//! from one, and the broker answers each with `basic.ack` — taken — or
//! `basic.nack` — not taken, publish again — naming that number, or every
//! number up to it where `multiple` is set. Without it a publish is written
//! and hoped for; with it a publish is at least once, which is what the
//! event capability's wire promises (ADR-0065 clause 3).

use codec::writer::ByteWriter;
use transport::error::Result;

use crate::method::{BASIC_ACK, Id, Method};

pub const CONFIRM_SELECT: Id = (85, 10);
pub const CONFIRM_SELECT_OK: Id = (85, 11);
const BASIC_NACK: Id = (60, 120);

/// confirm.select, waiting for its select-ok.
#[must_use]
pub fn select() -> Method {
    Method::new(CONFIRM_SELECT, vec![0])
}

/// basic.nack of `delivery_tag`, and that one only, not to be requeued.
#[must_use]
pub fn basic_nack(delivery_tag: u64) -> Method {
    let mut out = Vec::new();
    out.u64_be(delivery_tag).byte(0);
    Method::new(BASIC_NACK, out)
}

/// What a broker said of the publishes up to a number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Confirmed {
    /// Taken (`basic.ack`), or not (`basic.nack`).
    pub taken: bool,
    pub delivery_tag: u64,
    /// Every publish up to `delivery_tag`, not that one alone.
    pub multiple: bool,
}

impl Confirmed {
    /// What an ack or a nack says, or `None` for any other method.
    ///
    /// # Errors
    /// Arguments that end early.
    pub fn of(method: &Method) -> Result<Option<Self>> {
        let taken = if method.is(BASIC_ACK) {
            true
        } else if method.is(BASIC_NACK) {
            false
        } else {
            return Ok(None);
        };
        let mut cursor = method.cursor();
        let delivery_tag = cursor.u64_be()?;
        let multiple = cursor.byte()? & 1 == 1;
        Ok(Some(Self {
            taken,
            delivery_tag,
            multiple,
        }))
    }

    /// Whether this answers the publish numbered `number`.
    #[must_use]
    pub const fn answers(&self, number: u64) -> bool {
        self.delivery_tag == number || (self.multiple && self.delivery_tag > number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::basic_ack;

    #[test]
    fn an_ack_and_a_nack_say_which_publishes_they_answer() {
        let ack = Confirmed::of(&basic_ack(3)).expect("read").expect("an ack");
        assert!(ack.taken && ack.answers(3) && !ack.answers(2));
        let nack = Confirmed::of(&basic_nack(4))
            .expect("read")
            .expect("a nack");
        assert!(!nack.taken && nack.answers(4));
        let mut multiple = Vec::new();
        multiple.u64_be(9).byte(1);
        let all = Confirmed::of(&Method::new(BASIC_ACK, multiple))
            .expect("read")
            .expect("an ack");
        assert!(all.answers(5) && all.answers(9) && !all.answers(10));
        assert_eq!(Confirmed::of(&select()).expect("read"), None);
        assert!(
            Confirmed::of(&Method::bare(BASIC_NACK)).is_err(),
            "cut short"
        );
    }
}
