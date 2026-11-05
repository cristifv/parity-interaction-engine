//! The xsid (parity-contracts 0.13.0, CONVENTIONS "The xsid"): one id per customer call, the same
//! in every service, on every leg, in the CDR, the events and the logs. It **is** the
//! interaction's id: no second id is minted.
//!
//! Where a call enters the platform decides who mints it: opensips for inbound SIP (IE takes it
//! from TC's `Arrived`), parity-voice-api for a CPaaS call (its `call_id`), and **IE for the
//! contact center's own calls**: click-to-dial, a campaign's call, a consult (a new UUIDv7 each).
//! A callback attempt's id is made by routing-engine (`RunCallback`, routing.v1 "Callbacks": one
//! id per attempt across redeliveries, so IE's claim drops a repeat); IE takes it as the xsid.
//!
//! **A consult is a child interaction** (the accepted design, 2026-11-02): its own xsid, and
//! `parent_xsid` = the customer's call. When it completes (a warm transfer), the consulted agent's
//! leg joins the parent call, which keeps its xsid; the consult's xsid ends there. A transfer,
//! blind or warm, and a conference keep the xsid of the call they grew from. The CDR carries the
//! parent as `parent_interaction_id`.
//!
//! Pure: the new id comes from an [`Ids`] source (UUIDv7 in production).

use crate::machine::Ids;

/// Where a call came from (the log record's `attrs.origin`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Inbound SIP: the edge minted the xsid.
    Pstn,
    /// A CPaaS API call: parity-voice-api's `call_id`.
    Api,
    /// An agent's click-to-dial (step 13): IE mints.
    ClickToDial,
    /// A campaign's call (step 13a/b): IE mints.
    Campaign,
    /// A callback attempt (step 12): routing-engine's attempt id.
    Callback,
    /// A consult (step 11): IE mints; the parent is the customer's call.
    Consult,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pstn => "pstn",
            Self::Api => "api",
            Self::ClickToDial => "click_to_dial",
            Self::Campaign => "campaign",
            Self::Callback => "callback",
            Self::Consult => "consult",
        }
    }

    /// Whether IE makes this call's xsid (its own outbound calls).
    pub fn minted_by_ie(self) -> bool {
        matches!(self, Self::ClickToDial | Self::Campaign | Self::Consult)
    }
}

/// Why a call's ids were refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum XsidError {
    /// IE doesn't mint this origin's xsid (it comes with the call).
    #[error("IE doesn't mint the xsid of a {0} call")]
    NotMintedHere(&'static str),
    /// The minter (or routing-engine) gave something that isn't a lowercase UUIDv7.
    #[error("not a lowercase UUIDv7")]
    NotV7,
    /// A consult needs the call it consults for.
    #[error("a consult needs its parent call")]
    NoParent,
    /// A call can't be its own parent.
    #[error("parent_xsid equals xsid")]
    OwnParent,
}

/// A call's ids: the xsid (= its interaction id) and the call that caused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallIds {
    pub xsid: String,
    pub parent_xsid: Option<String>,
    pub origin: Origin,
}

/// A lowercase RFC 9562 UUIDv7 (version 7, variant 10xx).
pub fn is_v7(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
        })
        && b[14] == b'7'
        && matches!(b[19], b'8' | b'9' | b'a' | b'b')
}

impl CallIds {
    /// A call IE starts (click-to-dial, campaign, consult): a new xsid from `ids`, and the call
    /// that caused it (a consult's customer call: required).
    pub fn mint(
        origin: Origin,
        ids: &mut dyn Ids,
        parent: Option<&str>,
    ) -> Result<Self, XsidError> {
        if !origin.minted_by_ie() {
            return Err(XsidError::NotMintedHere(origin.as_str()));
        }
        if origin == Origin::Consult && parent.is_none() {
            return Err(XsidError::NoParent);
        }
        let xsid = ids.next_id();
        Self::checked(xsid, origin, parent)
    }

    /// A call whose xsid came with it: inbound (the edge's, through TC), an API call, a callback
    /// attempt (routing-engine's). Inbound and API ids are taken as they are (readers accept any
    /// RFC 9562 UUID; the edge's may predate v7, PL-081); a callback's must be a UUIDv7.
    pub fn given(xsid: &str, origin: Origin, parent: Option<&str>) -> Result<Self, XsidError> {
        if origin.minted_by_ie() {
            return Err(XsidError::NotMintedHere(origin.as_str()));
        }
        if origin == Origin::Callback && !is_v7(xsid) {
            return Err(XsidError::NotV7);
        }
        if parent == Some(xsid) {
            return Err(XsidError::OwnParent);
        }
        Ok(Self {
            xsid: xsid.to_owned(),
            parent_xsid: parent.map(str::to_owned),
            origin,
        })
    }

    fn checked(xsid: String, origin: Origin, parent: Option<&str>) -> Result<Self, XsidError> {
        if !is_v7(&xsid) {
            return Err(XsidError::NotV7);
        }
        if parent == Some(xsid.as_str()) {
            return Err(XsidError::OwnParent);
        }
        Ok(Self {
            xsid,
            parent_xsid: parent.map(str::to_owned),
            origin,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Seq(u64);
    impl Ids for Seq {
        fn next_id(&mut self) -> String {
            self.0 += 1;
            format!("0192f6a1-7c3e-7b21-9a4d-{:012x}", self.0)
        }
    }

    const CUSTOMER: &str = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";

    #[test]
    fn ie_mints_its_own_calls_and_a_consult_is_a_child_of_the_customers_call() {
        let mut ids = Seq(0);
        let c = CallIds::mint(Origin::Consult, &mut ids, Some(CUSTOMER)).unwrap();
        assert!(is_v7(&c.xsid));
        assert_ne!(c.xsid, CUSTOMER);
        assert_eq!(c.parent_xsid.as_deref(), Some(CUSTOMER));
        assert_eq!(
            CallIds::mint(Origin::Consult, &mut ids, None),
            Err(XsidError::NoParent)
        );
        let d = CallIds::mint(Origin::ClickToDial, &mut ids, None).unwrap();
        assert!(d.parent_xsid.is_none());
        assert!(CallIds::mint(Origin::Campaign, &mut ids, None).is_ok());
        assert_eq!(
            CallIds::mint(Origin::Pstn, &mut ids, None),
            Err(XsidError::NotMintedHere("pstn"))
        );
    }

    #[test]
    fn a_minter_that_isnt_v7_or_a_call_that_is_its_own_parent_is_refused() {
        struct V4;
        impl Ids for V4 {
            fn next_id(&mut self) -> String {
                "2465ecce-98d8-4143-8e19-4073541af2cb".into()
            }
        }
        assert_eq!(
            CallIds::mint(Origin::ClickToDial, &mut V4, None),
            Err(XsidError::NotV7)
        );
        assert_eq!(
            CallIds::given(CUSTOMER, Origin::Callback, Some(CUSTOMER)),
            Err(XsidError::OwnParent)
        );
        assert_eq!(
            CallIds::given(
                "2465ecce-98d8-4143-8e19-4073541af2cb",
                Origin::Callback,
                None
            ),
            Err(XsidError::NotV7)
        );
        // An inbound id from an edge that predates v7 is still the call's id (PL-081).
        assert!(CallIds::given("2465ecce-98d8-4143-8e19-4073541af2cb", Origin::Pstn, None).is_ok());
    }
}
