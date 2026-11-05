//! The static flow file (step 4): IVR flows in admin-console's flow profile of the voice verbs
//! (PLAN "The verb runner"; admin-console PLAN "IVR flows"), the numbers bound to them, and each
//! tenant's recording consent. Checked when loaded; a call never sees an invalid flow.
//!
//! PCI: a flow `gather` is for menu choices. Collecting card numbers, CVV, PINs or other PCI-like
//! data is out of scope for v1 flows, so a `gather` asking for more than 6 digits is refused.

use serde::Deserialize;
use std::collections::BTreeMap;

/// The most digits a flow `gather` may ask for (PCI-like collection is out of scope for v1).
pub const MAX_GATHER_DIGITS: u32 = 6;

/// One verb: an object with exactly one key, the verb's name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    /// A pre-rendered prompt for this text (question 7).
    Say(Say),
    /// A tenant prompt.
    Play(Play),
    /// Silence.
    Pause(Pause),
    /// Digits, routed to a step.
    Gather(Gather),
    /// Jump to a step.
    Goto(Goto),
    /// End the call.
    Hangup(Hangup),
    /// To a queue (step 5a: with routing on; else skipped and counted) or a number (skipped and
    /// counted until step 13).
    Dial(Dial),
}

/// `dial`: exactly one of `queue` and `number`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dial {
    /// The tenant's queue.
    #[serde(default)]
    pub queue: Option<String>,
    /// An E.164 number (step 13).
    #[serde(default)]
    pub number: Option<String>,
    /// 1–100, higher first; 0: the queue's default.
    #[serde(default)]
    pub priority: u32,
    /// Step 6: the tenant prompt the caller hears, looped, while waiting (none: silence).
    #[serde(default)]
    pub music: Option<String>,
    /// Step 6: announce the caller's position (`queue-position-1` .. `-9`, `-more`) when it
    /// changes, at most every 20 s, between the music.
    #[serde(default)]
    pub announce_position: bool,
}

/// `say`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Say {
    pub text: String,
    #[serde(default)]
    pub language: String,
    #[serde(default)]
    pub voice: String,
}

/// `play`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Play {
    /// A tenant prompt id (`[A-Za-z0-9_-]{1,64}`).
    pub prompt: String,
}

/// `pause`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pause {
    /// 1–60.
    pub seconds: u32,
}

fn one() -> u32 {
    1
}
fn five() -> u32 {
    5
}
fn three() -> u32 {
    3
}

/// `gather`, flow profile.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gather {
    /// Only `dtmf`.
    #[serde(default)]
    pub input: Option<String>,
    /// 1–6.
    #[serde(default = "one")]
    pub num_digits: u32,
    /// Ends the input early (`0-9*#`, or none).
    #[serde(default)]
    pub finish_on_key: String,
    /// The first digit's timeout, 1–60 s.
    #[serde(default = "five")]
    pub timeout_s: u32,
    /// Between digits, 1–30 s.
    #[serde(default = "three")]
    pub inter_digit_s: u32,
    /// Digits → a step.
    #[serde(default)]
    pub routes: BTreeMap<String, String>,
    /// The step for anything else, and for no input; none: the next verb.
    #[serde(default)]
    pub default: Option<String>,
}

/// `goto`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Goto {
    pub step: String,
}

/// `hangup`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hangup {}

/// One flow: named steps of verbs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    /// The tenant that owns it; only its numbers may be bound to it.
    pub tenant_id: String,
    /// The first step.
    pub start: String,
    /// The steps.
    pub steps: BTreeMap<String, Vec<Verb>>,
    /// After 3 no-input timeouts in a row or a loop limit (`say`, `play`, `pause`, `hangup` only);
    /// empty: hang up.
    #[serde(default)]
    pub no_input: Vec<Verb>,
    /// The schedule's name (closed paths come with step 10; ignored until then).
    #[serde(default)]
    pub schedule: Option<String>,
}

/// A number's binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub flow: String,
}

/// How a tenant asks for recording consent (PLAN "Privacy").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConsentMode {
    /// Never record; nothing is played.
    #[default]
    Never,
    /// Announce; the opt-out digit refuses, silence grants.
    AnnounceOptOut,
    /// Ask; only the opt-in digit grants.
    OptIn,
}

/// A tenant's consent rule.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Consent {
    pub mode: ConsentMode,
    /// The announcement (`say`, `play` only).
    #[serde(default)]
    pub announce: Vec<Verb>,
    /// The opt-out or opt-in digit.
    #[serde(default)]
    pub digit: String,
    /// How long after the announcement a digit still counts, 1–30 s.
    #[serde(default = "five")]
    pub window_s: u32,
    /// Record the call when consent allows it (a stub until parity-ms encrypts, MS-012).
    #[serde(default)]
    pub record: bool,
}

/// The whole file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowFile {
    /// 1.
    pub schema_version: u32,
    pub flows: BTreeMap<String, Flow>,
    /// E.164 → its flow.
    pub numbers: BTreeMap<String, Binding>,
    /// Tenant → its consent rule; a tenant without one never records.
    #[serde(default)]
    pub consent: BTreeMap<String, Consent>,
}

/// Why a flow file is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlowError {
    #[error("the flow file doesn't parse: {0}")]
    Parse(String),
    #[error("{at}: {what}")]
    Invalid { at: String, what: String },
}

fn bad<T>(at: impl Into<String>, what: impl Into<String>) -> Result<T, FlowError> {
    Err(FlowError::Invalid {
        at: at.into(),
        what: what.into(),
    })
}

fn is_dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn is_name(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A queue id (routing.v1 fixtures' pattern).
pub fn is_queue_id(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

fn is_key(s: &str) -> bool {
    s.len() == 1
        && s.bytes()
            .all(|b| b.is_ascii_digit() || b == b'*' || b == b'#')
}

/// E.164 as the tenant maps file has it.
pub fn is_e164(s: &str) -> bool {
    let Some(d) = s.strip_prefix('+') else {
        return false;
    };
    (7..=15).contains(&d.len()) && !d.starts_with('0') && d.bytes().all(|b| b.is_ascii_digit())
}

impl FlowFile {
    /// Parses and checks a file. `tenant_of` is the number map (the tenant maps file).
    pub fn load(text: &str, tenant_of: impl Fn(&str) -> Option<String>) -> Result<Self, FlowError> {
        let f: Self = serde_json::from_str(text).map_err(|e| FlowError::Parse(e.to_string()))?;
        f.validate(tenant_of)?;
        Ok(f)
    }

    fn validate(&self, tenant_of: impl Fn(&str) -> Option<String>) -> Result<(), FlowError> {
        if self.schema_version != 1 {
            return bad("schema_version", "must be 1");
        }
        for (name, flow) in &self.flows {
            let at = format!("flows.{name}");
            if !is_name(name) {
                return bad(&at, "a flow name is [A-Za-z0-9_-]{1,64}");
            }
            validate_flow(&at, flow)?;
        }
        for (number, b) in &self.numbers {
            // The number itself is personal data in other contexts; here it is the tenant's DID.
            let at = format!("numbers.{number}");
            if !is_e164(number) {
                return bad(&at, "not E.164");
            }
            let Some(flow) = self.flows.get(&b.flow) else {
                return bad(&at, format!("no flow {:?}", b.flow));
            };
            match tenant_of(number) {
                None => return bad(&at, "not in the number map"),
                Some(t) if t != flow.tenant_id => {
                    return bad(
                        &at,
                        format!(
                            "belongs to tenant {t:?}, but flow {:?} is {:?}'s",
                            b.flow, flow.tenant_id
                        ),
                    );
                }
                Some(_) => {}
            }
        }
        for (tenant, c) in &self.consent {
            let at = format!("consent.{tenant}");
            if !is_dns_label(tenant) {
                return bad(&at, "a tenant id is a DNS label");
            }
            if c.mode != ConsentMode::Never {
                if !is_key(&c.digit) {
                    return bad(&at, "digit must be one of 0-9 * #");
                }
                if !(1..=30).contains(&c.window_s) {
                    return bad(&at, "window_s must be 1-30");
                }
                if c.announce.is_empty() {
                    return bad(&at, "announce must play something");
                }
            }
            for (i, v) in c.announce.iter().enumerate() {
                let at = format!("{at}.announce[{i}]");
                if !matches!(v, Verb::Say(_) | Verb::Play(_)) {
                    return bad(&at, "only say and play");
                }
                validate_verb(&at, v, &BTreeMap::new())?;
            }
        }
        Ok(())
    }
}

fn validate_flow(at: &str, flow: &Flow) -> Result<(), FlowError> {
    if !is_dns_label(&flow.tenant_id) {
        return bad(at, "tenant_id is a DNS label");
    }
    if !flow.steps.contains_key(&flow.start) {
        return bad(at, format!("start {:?} is not a step", flow.start));
    }
    for (step, verbs) in &flow.steps {
        let at = format!("{at}.steps.{step}");
        if !is_name(step) {
            return bad(&at, "a step name is [A-Za-z0-9_-]{1,64}");
        }
        if verbs.is_empty() || verbs.len() > 50 {
            return bad(&at, "1-50 verbs");
        }
        for (i, v) in verbs.iter().enumerate() {
            validate_verb(&format!("{at}[{i}]"), v, &flow.steps)?;
        }
    }
    for (i, v) in flow.no_input.iter().enumerate() {
        let at = format!("{at}.no_input[{i}]");
        if !matches!(
            v,
            Verb::Say(_) | Verb::Play(_) | Verb::Pause(_) | Verb::Hangup(_)
        ) {
            return bad(&at, "only say, play, pause and hangup");
        }
        validate_verb(&at, v, &flow.steps)?;
    }
    Ok(())
}

fn validate_verb(at: &str, v: &Verb, steps: &BTreeMap<String, Vec<Verb>>) -> Result<(), FlowError> {
    let step = |s: &str| {
        if steps.contains_key(s) {
            Ok(())
        } else {
            bad(at, format!("no step {s:?}"))
        }
    };
    match v {
        Verb::Say(s) => {
            if s.text.is_empty() || s.text.chars().count() > 4000 {
                return bad(at, "say.text: 1-4000 characters");
            }
            if s.language.len() > 16 || s.voice.len() > 64 {
                return bad(at, "say.language or say.voice too long");
            }
        }
        Verb::Play(p) => {
            if !is_name(&p.prompt) {
                return bad(at, "play.prompt is [A-Za-z0-9_-]{1,64}");
            }
        }
        Verb::Pause(p) => {
            if !(1..=60).contains(&p.seconds) {
                return bad(at, "pause.seconds: 1-60");
            }
        }
        Verb::Gather(g) => {
            if g.input.as_deref().is_some_and(|i| i != "dtmf") {
                return bad(at, "gather.input: dtmf only");
            }
            if !(1..=MAX_GATHER_DIGITS).contains(&g.num_digits) {
                return bad(
                    at,
                    format!(
                        "gather.num_digits: 1-{MAX_GATHER_DIGITS} (PCI-like collection is out of scope for v1 flows)"
                    ),
                );
            }
            if !g.finish_on_key.is_empty() && !is_key(&g.finish_on_key) {
                return bad(at, "gather.finish_on_key: one of 0-9 * #");
            }
            if !(1..=60).contains(&g.timeout_s) || !(1..=30).contains(&g.inter_digit_s) {
                return bad(at, "gather.timeout_s 1-60, inter_digit_s 1-30");
            }
            for (k, target) in &g.routes {
                let ok = !k.is_empty()
                    && k.len() <= g.num_digits as usize
                    && k.bytes()
                        .all(|b| b.is_ascii_digit() || b == b'*' || b == b'#');
                if !ok {
                    return bad(at, "gather.routes: keys are 1..num_digits of 0-9 * #");
                }
                step(target)?;
            }
            if let Some(d) = &g.default {
                step(d)?;
            }
        }
        Verb::Goto(g) => step(&g.step)?,
        Verb::Dial(d) => {
            match (&d.queue, &d.number) {
                (Some(q), None) if is_queue_id(q) => {}
                (Some(_), None) => return bad(at, "dial.queue is [A-Za-z0-9_.-]{1,64}"),
                (None, Some(n)) if is_e164(n) => {}
                (None, Some(_)) => return bad(at, "dial.number is E.164"),
                _ => return bad(at, "dial: exactly one of queue and number"),
            }
            if d.priority > 100 {
                return bad(at, "dial.priority: 0-100");
            }
            if d.queue.is_none() && (d.music.is_some() || d.announce_position) {
                return bad(at, "dial.music and dial.announce_position are for a queue");
            }
            if d.music.as_deref().is_some_and(|m| !is_name(m)) {
                return bad(at, "dial.music is [A-Za-z0-9_-]{1,64}");
            }
        }
        Verb::Hangup(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) const FILE: &str = r#"{
      "schema_version": 1,
      "flows": {
        "acme-main": {
          "tenant_id": "acme",
          "start": "menu",
          "steps": {
            "menu": [
              {"say": {"text": "Welcome to Acme.", "language": "en-GB"}},
              {"say": {"text": "Press 1 for sales.", "language": "en-GB"}},
              {"gather": {"num_digits": 1, "timeout_s": 5, "routes": {"1": "sales"}, "default": "menu"}}
            ],
            "sales": [{"say": {"text": "Sales will be with you soon.", "language": "en-GB"}}, {"hangup": {}}]
          },
          "no_input": [{"say": {"text": "Goodbye.", "language": "en-GB"}}, {"hangup": {}}]
        }
      },
      "numbers": {"+15550100100": {"flow": "acme-main"}},
      "consent": {"acme": {"mode": "announce_opt_out", "digit": "9", "window_s": 3, "record": true,
                  "announce": [{"say": {"text": "Calls are recorded. Press 9 to opt out.", "language": "en-GB"}}]}}
    }"#;

    fn map(n: &str) -> Option<String> {
        match n {
            "+15550100100" => Some("acme".into()),
            "+15550101100" => Some("globex".into()),
            _ => None,
        }
    }

    #[test]
    fn the_demo_file_loads() {
        let f = FlowFile::load(FILE, map).unwrap();
        assert_eq!(f.flows["acme-main"].steps["menu"].len(), 3);
        assert_eq!(f.consent["acme"].mode, ConsentMode::AnnounceOptOut);
    }

    #[test]
    fn a_dial_to_a_queue_loads() {
        let text = FILE.replacen(
            r#"{"hangup": {}}]
          },"#,
            r#"{"dial": {"queue": "sales", "priority": 5}}]
          },"#,
            1,
        );
        let f = FlowFile::load(&text, map).unwrap();
        assert!(matches!(
            f.flows["acme-main"].steps["sales"][1],
            Verb::Dial(Dial { ref queue, priority: 5, .. }) if queue.as_deref() == Some("sales")
        ));
    }

    #[test]
    fn checks_refuse_what_a_call_must_never_meet() {
        for (from, to, why) in [
            (
                r#""+15550100100": {"flow""#,
                r#""+15550101100": {"flow""#,
                "another tenant's number",
            ),
            (
                r#""+15550100100": {"flow""#,
                r#""+15550109999": {"flow""#,
                "a number not in the map",
            ),
            (
                r#""num_digits": 1"#,
                r#""num_digits": 16"#,
                "PCI-like collection",
            ),
            (
                r#""default": "menu""#,
                r#""default": "nowhere""#,
                "a route to no step",
            ),
            (r#""start": "menu""#, r#""start": "main""#, "no start step"),
            (r#""digit": "9""#, r#""digit": "99""#, "a consent digit"),
            (
                r#"{"hangup": {}}]
          },"#,
                r#"{"hangup": {}}, {"tts": {}}]
          },"#,
                "an unknown verb",
            ),
            (
                r#""tenant_id": "acme""#,
                r#""tenant_id": "Acme""#,
                "a tenant id",
            ),
            (
                r#"{"hangup": {}}]
          },"#,
                r#"{"dial": {"queue": "sales", "number": "+15550100200"}}]
          },"#,
                "a dial to a queue and a number",
            ),
            (
                r#"{"hangup": {}}]
          },"#,
                r#"{"dial": {"queue": "sales queue"}}]
          },"#,
                "a queue id",
            ),
            (
                r#"{"hangup": {}}]
          },"#,
                r#"{"dial": {"queue": "sales", "priority": 101}}]
          },"#,
                "a priority",
            ),
        ] {
            let text = FILE.replacen(from, to, 1);
            assert_ne!(text, FILE, "{why}: replacement didn't apply");
            assert!(FlowFile::load(&text, map).is_err(), "{why}");
        }
    }
}
