//! JSON logs on stdout in parity-contracts' `log-record.v1` format (CONVENTIONS "Logging"), with
//! PII masking (ADR 0001 §7.1).
//!
//! One JSON object per line: the envelope (`ts`, lowercase `level`, `service`, `version`,
//! `instance`, `incarnation`, `seq`, `event`, `msg`, `component`, `code`), the correlation and
//! flow fields log-record.v1 names (`tenant_id`, `xsid`, `leg_id`, `op`, `outcome`, `error`, ...)
//! at the top level, and every other field under `attrs`. Fields come from the enclosing spans
//! (root first) and then the event, so a call's span gives each of its records `tenant_id` and
//! the `xsid`. Every field goes through [`crate::pii::mask_field`] by its own name before it is
//! placed. Writing never blocks: lines go through a bounded, lossy queue; drops are counted (a
//! `seq` gap shows where) and reported as `log.dropped` by the binary.
//!
//! - `event`: the record's catalogue name (`logging/events.parity-interaction-engine.v1.json`):
//!   every IE log call sets it (`info!(event = "routing.enqueued", ...)`). Without one, a record
//!   of IE's own code is `log.unclassified` (not in the catalogue: CI's contract-check fails it)
//!   and a library's is `log.external`.
//! - The xsid: an `interaction_id` field that is a UUIDv7 is written as `xsid` (the interaction's
//!   id is the xsid, CONVENTIONS "The xsid"); any other UUID stays `interaction_id`.
//!   `parent_interaction_id` (or `parent_xsid`) becomes `parent_xsid`.
//! - `error = %e` becomes `error.message`; `error_code`, `error_kind` and `error_stack` fill the
//!   rest of `error`.
//! - A value that doesn't fit its log-record.v1 field (a non-UUID `leg_id`, an out-of-range
//!   `attempt`) goes under `attrs` instead, so a line never breaks the schema.

use crate::pii::mask_field;
use serde_json::{Map, Value};
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields, MakeWriter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

/// log-record.v1's line limit (bytes, without the newline).
pub const MAX_LINE: usize = 16_000;
/// `msg`'s limit (characters).
const MAX_MSG: usize = 256;
/// `attrs` keys at most.
const MAX_ATTRS: usize = 32;

/// Who is logging, written on every line.
#[derive(Debug, Clone)]
pub struct Identity {
    pub service: String,
    pub version: String,
    pub zone: String,
    pub bin: String,
    pub instance: String,
}

/// Logging settings.
#[derive(Debug, Clone)]
pub struct LogConfig {
    pub identity: Identity,
    /// `EnvFilter` directives, e.g. `info`.
    pub filter: String,
    /// The non-blocking queue's capacity, in lines.
    pub buffer_lines: usize,
}

/// Keeps the writer thread alive; dropping it flushes what is queued.
pub struct LogGuard {
    _guard: WorkerGuard,
    dropped: ErrorCounter,
}

impl LogGuard {
    /// Lines dropped because the queue was full.
    pub fn dropped_lines(&self) -> usize {
        self.dropped.dropped_lines()
    }

    /// A handle to the drop counter, for the metrics callback.
    pub fn dropped_counter(&self) -> ErrorCounter {
        self.dropped.clone()
    }
}

/// The production layer over any writer: the log-record.v1 formatter and the masking field
/// formatter. [`init`] uses it with stdout; `ie_obs::testing` with an in-memory buffer or a file.
pub fn layer<S, W>(identity: &Identity, writer: W) -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .event_format(JsonFormat::new(identity))
        .fmt_fields(MaskingFields)
        .with_writer(writer)
}

/// Installs the global subscriber: the filter (`IE_LOG` overrides `cfg.filter`) and the JSON
/// layer over a non-blocking, lossy stdout. Also logs panics (`service.panic`, level `fatal`).
pub fn init(cfg: &LogConfig) -> anyhow::Result<LogGuard> {
    let filter = match std::env::var("IE_LOG") {
        Ok(f) if !f.is_empty() => f,
        _ => cfg.filter.clone(),
    };
    let filter =
        EnvFilter::try_new(&filter).map_err(|e| anyhow::anyhow!("log filter {filter:?}: {e}"))?;
    let (writer, guard) = NonBlockingBuilder::default()
        .lossy(true)
        .buffered_lines_limit(cfg.buffer_lines)
        .finish(io::stdout());
    let dropped = writer.error_counter();
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(layer(&cfg.identity, writer));
    tracing::subscriber::set_global_default(subscriber)?;
    install_panic_hook();
    Ok(LogGuard {
        _guard: guard,
        dropped,
    })
}

/// Panics as one `service.panic` record (level `fatal`): the payload in `error.message`, the
/// location in `error.stack` (no backtrace: it can hold values).
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map(|l| format!("at {}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_default();
        tracing::error!(
            event = "service.panic",
            error_kind = "panic",
            error = %message,
            error_stack = %location,
            "the process panicked"
        );
    }));
}

/// A process start id (`incarnation`): a UUIDv7, so that `seq` stays unambiguous when a
/// restarted process keeps its instance name.
fn incarnation() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Formats one event as a log-record.v1 line.
pub struct JsonFormat {
    service: String,
    version: String,
    instance: String,
    incarnation: String,
    zone: String,
    bin: String,
    seq: Arc<AtomicU64>,
}

impl JsonFormat {
    fn new(identity: &Identity) -> Self {
        Self {
            service: identity.service.clone(),
            version: identity.version.clone(),
            instance: identity.instance.clone(),
            incarnation: incarnation(),
            zone: identity.zone.clone(),
            bin: identity.bin.clone(),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// One record being built.
#[derive(Default)]
struct Record {
    top: Map<String, Value>,
    attrs: Map<String, Value>,
    error: Map<String, Value>,
    event: Option<String>,
    msg: Option<String>,
}

fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
        })
}

/// A lowercase RFC 9562 UUIDv7 (version 7, variant 10xx).
pub fn is_uuid7(s: &str) -> bool {
    is_uuid(s) && s.as_bytes()[14] == b'7' && matches!(s.as_bytes()[19], b'8' | b'9' | b'a' | b'b')
}

fn chars_ok(s: &str, max: usize, first: impl Fn(u8) -> bool, rest: impl Fn(u8) -> bool) -> bool {
    let b = s.as_bytes();
    !b.is_empty() && b.len() <= max && first(b[0]) && b[1..].iter().all(|c| rest(*c))
}

fn alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}

/// A dotted snake_case event name (`noun.verb`).
fn valid_event(s: &str) -> bool {
    s.len() <= 64
        && s.contains('.')
        && s.split('.').all(|p| {
            chars_ok(
                p,
                64,
                |c| c.is_ascii_lowercase(),
                |c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_',
            )
        })
}

fn text(v: &Value) -> Option<&str> {
    v.as_str()
}

impl Record {
    /// Places one (already masked) field.
    fn put(&mut self, key: &str, v: Value) {
        let s = text(&v);
        let ok = match key {
            "event" => {
                if let Some(e) = s.filter(|e| valid_event(e)) {
                    self.event = Some(e.to_owned());
                    return;
                }
                false
            }
            "message" => {
                self.msg = s.map(str::to_owned).or_else(|| Some(v.to_string()));
                return;
            }
            "error" => {
                let m = s.map_or_else(|| v.to_string(), str::to_owned);
                self.error
                    .insert("message".into(), Value::String(cut(&m, 1024)));
                return;
            }
            "error_code" => {
                if let Some(c) = s.filter(|c| {
                    chars_ok(
                        c,
                        64,
                        |x| x.is_ascii_lowercase(),
                        |x| x.is_ascii_lowercase() || x.is_ascii_digit() || x == b'_',
                    )
                }) {
                    self.error
                        .insert("code".into(), Value::String(c.to_owned()));
                    return;
                }
                false
            }
            "error_kind" => {
                if let Some(k) = s.filter(|k| {
                    chars_ok(
                        k,
                        128,
                        |x| x.is_ascii_alphabetic() || x == b'_',
                        |x| x.is_ascii_alphanumeric() || b"_.$:<>".contains(&x),
                    )
                }) {
                    self.error
                        .insert("kind".into(), Value::String(k.to_owned()));
                    return;
                }
                false
            }
            "error_stack" => {
                if let Some(st) = s {
                    self.error
                        .insert("stack".into(), Value::String(cut(st, 8192)));
                    return;
                }
                false
            }
            "interaction_id" | "xsid" => {
                if let Some(id) = s {
                    if is_uuid7(id) {
                        self.top.insert("xsid".into(), Value::String(id.to_owned()));
                        self.top.remove("interaction_id");
                        return;
                    }
                    if is_uuid(id) && !self.top.contains_key("xsid") {
                        self.top
                            .insert("interaction_id".into(), Value::String(id.to_owned()));
                        return;
                    }
                }
                false
            }
            "parent_interaction_id" | "parent_xsid" => {
                if let Some(id) = s.filter(|id| is_uuid7(id)) {
                    self.top
                        .insert("parent_xsid".into(), Value::String(id.to_owned()));
                    return;
                }
                false
            }
            "tenant_id" => s.is_some_and(|t| {
                chars_ok(
                    t,
                    63,
                    |c| c.is_ascii_lowercase() || c.is_ascii_digit(),
                    |c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-',
                ) && !t.ends_with('-')
            }),
            "call_id" | "leg_id" | "entry_id" | "reservation_id" | "event_id" => {
                s.is_some_and(is_uuid)
            }
            "session_id" | "agent_id" | "node" => s.is_some_and(|x| {
                chars_ok(x, 128, alnum, |c| {
                    c.is_ascii_alphanumeric() || b"._:-".contains(&c)
                })
            }),
            "request_id" | "command_id" => s.is_some_and(|x| {
                chars_ok(
                    x,
                    200,
                    |c| c.is_ascii_alphanumeric() || b"._:-".contains(&c),
                    |c| c.is_ascii_alphanumeric() || b"._:-".contains(&c),
                )
            }),
            "queue_id" => s.is_some_and(|x| {
                chars_ok(
                    x,
                    64,
                    |c| c.is_ascii_alphanumeric() || b"_.-".contains(&c),
                    |c| c.is_ascii_alphanumeric() || b"_.-".contains(&c),
                )
            }),
            "bin" | "zone" | "cell" => s.is_some_and(|x| {
                chars_ok(x, 64, alnum, |c| {
                    c.is_ascii_alphanumeric() || b"._-".contains(&c)
                })
            }),
            "leader_epoch" | "owner_epoch" => {
                v.as_u64().is_some_and(|n| n <= 9_007_199_254_740_991)
            }
            "op" => s.is_some_and(|x| {
                chars_ok(
                    x,
                    128,
                    |c| c.is_ascii_alphanumeric() || b"_./:{}* -".contains(&c),
                    |c| c.is_ascii_alphanumeric() || b"_./:{}* -".contains(&c),
                )
            }),
            "from_state" | "to_state" => s.is_some_and(|x| {
                chars_ok(
                    x,
                    64,
                    |c| c.is_ascii_alphabetic(),
                    |c| c.is_ascii_alphanumeric() || c == b'_',
                )
            }),
            "cause" => s.is_some_and(|x| {
                x.len() <= 200
                    && x.split_once(':').is_some_and(|(k, r)| {
                        [
                            "event", "rpc", "cmd", "timer", "sip", "http", "kafka", "ws", "signal",
                            "operator", "config", "startup", "internal",
                        ]
                        .contains(&k)
                            && !r.is_empty()
                            && r.bytes()
                                .all(|c| c.is_ascii_alphanumeric() || b"._:/@{}* -".contains(&c))
                    })
            }),
            "duration_ms" => v.as_f64().is_some_and(|n| n >= 0.0),
            "attempt" => v.as_u64().is_some_and(|n| (1..=1000).contains(&n)),
            "outcome" => {
                s.is_some_and(|x| ["ok", "refused", "error", "timeout", "retry"].contains(&x))
            }
            _ => {
                self.attr(key, v);
                return;
            }
        };
        if ok {
            self.top.insert(key.to_owned(), v);
        } else {
            // It doesn't fit its log-record.v1 field: kept as an attribute.
            self.attr(key, v);
        }
    }

    fn attr(&mut self, key: &str, v: Value) {
        let mut k = snake(key);
        if !k.starts_with(|c: char| c.is_ascii_lowercase()) {
            k.insert(0, 'f');
            k.truncate(64);
        }
        if self.attrs.len() < MAX_ATTRS || self.attrs.contains_key(&k) {
            self.attrs.insert(k, v);
        }
    }
}

/// An `attrs` key: snake_case (`callerNumber` -> `caller_number`, `DTMFDigit` -> `dtmf_digit`),
/// other characters as `_`, at most 64.
fn snake(key: &str) -> String {
    let c: Vec<char> = key.chars().collect();
    let mut out = String::new();
    for (i, ch) in c.iter().enumerate() {
        if ch.is_ascii_uppercase() {
            let prev = i.checked_sub(1).map(|j| c[j]);
            let next = c.get(i + 1);
            let boundary = prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
                || (prev.is_some_and(|p| p.is_ascii_uppercase())
                    && next.is_some_and(char::is_ascii_lowercase));
            if boundary && !out.ends_with('_') {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii_lowercase() || ch.is_ascii_digit() || *ch == '_' {
            out.push(*ch);
        } else {
            out.push('_');
        }
    }
    out.chars().take(64).collect()
}

/// `s` cut to at most `max` characters, on a character boundary.
fn cut(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => s[..i].to_owned(),
        None => s.to_owned(),
    }
}

fn level_name(l: Level) -> &'static str {
    match l {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

/// `component` from the target (`ie_core::machine`), made to fit its pattern.
fn component(target: &str) -> String {
    let mut c: String = target
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "_.:/$-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    if !c.starts_with(|x: char| x.is_ascii_alphabetic() || x == '_') {
        c.insert(0, '_');
        c.truncate(128);
    }
    c
}

/// IE's own code (a record without an `event` here is a bug the catalogue check finds).
fn own_target(target: &str) -> bool {
    target.starts_with("ie_") || target.starts_with("interaction_engine")
}

impl<S, N> FormatEvent<S, N> for JsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut rec = Record::default();
        // The process's place (correlation fields known for every record).
        for (k, v) in [("bin", &self.bin), ("zone", &self.zone)] {
            if !v.is_empty() {
                rec.put(k, Value::String(v.clone()));
            }
        }
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let ext = span.extensions();
                if let Some(Ok(Value::Object(fields))) = ext
                    .get::<FormattedFields<N>>()
                    .map(|f| serde_json::from_str::<Value>(&f.fields))
                {
                    for (k, v) in fields {
                        if k != "span_message" {
                            rec.put(&k, v);
                        }
                    }
                }
            }
        }
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        for (k, v) in std::mem::take(&mut visitor.fields) {
            rec.put(&k, v);
        }
        if let Some(m) = visitor.message.take() {
            rec.msg = Some(m);
        }
        let target = meta.target();
        let event_name = rec.event.take().unwrap_or_else(|| {
            if own_target(target) {
                "log.unclassified".to_owned()
            } else {
                "log.external".to_owned()
            }
        });
        let level = if event_name == "service.panic" {
            "fatal"
        } else {
            level_name(*meta.level())
        };
        let msg = rec
            .msg
            .take()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| event_name.clone());
        let mut obj = Map::new();
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        obj.insert("ts".into(), Value::String(ts));
        obj.insert("level".into(), Value::String(level.into()));
        obj.insert("service".into(), Value::String(self.service.clone()));
        obj.insert("version".into(), Value::String(self.version.clone()));
        obj.insert("instance".into(), Value::String(self.instance.clone()));
        obj.insert(
            "incarnation".into(),
            Value::String(self.incarnation.clone()),
        );
        obj.insert("seq".into(), Value::from(seq));
        obj.insert("event".into(), Value::String(event_name));
        obj.insert("msg".into(), Value::String(cut(&msg, MAX_MSG)));
        obj.insert("component".into(), Value::String(component(target)));
        if let Some(file) = meta
            .file()
            .filter(|f| !f.starts_with('/') && f.len() <= 256)
        {
            let mut code = Map::new();
            code.insert("file".into(), Value::String(file.to_owned()));
            if let Some(line) = meta.line().filter(|l| *l >= 1) {
                code.insert("line".into(), Value::from(line));
            }
            obj.insert("code".into(), Value::Object(code));
        }
        for (k, v) in std::mem::take(&mut rec.top) {
            obj.insert(k, v);
        }
        if !rec.error.is_empty() {
            if !matches!(level, "error" | "fatal") {
                // log-record.v1: a stack only at error or fatal.
                rec.error.remove("stack");
            }
            if !rec.error.is_empty() {
                obj.insert(
                    "error".into(),
                    Value::Object(std::mem::take(&mut rec.error)),
                );
            }
        }
        if !rec.attrs.is_empty() {
            obj.insert(
                "attrs".into(),
                Value::Object(std::mem::take(&mut rec.attrs)),
            );
        }
        let mut line = Value::Object(obj).to_string();
        if line.len() > MAX_LINE {
            line = shorten(line);
        }
        writeln!(writer, "{line}")
    }
}

/// A line over [`MAX_LINE`]: `attrs` strings cut to 256 characters, then `attrs` and
/// `error.message` dropped (`attrs.truncated`), never the line itself.
fn shorten(line: String) -> String {
    let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(&line) else {
        return line;
    };
    if let Some(Value::Object(a)) = obj.get_mut("attrs") {
        for v in a.values_mut() {
            if let Value::String(s) = v {
                *s = cut(s, 256);
            } else if v.is_array() || v.is_object() {
                *v = Value::String(cut(&v.to_string(), 256));
            }
        }
    }
    let s = Value::Object(obj.clone()).to_string();
    if s.len() <= MAX_LINE {
        return s;
    }
    let mut a = Map::new();
    a.insert("truncated".into(), Value::Bool(true));
    obj.insert("attrs".into(), Value::Object(a));
    if let Some(Value::Object(e)) = obj.get_mut("error") {
        e.remove("stack");
        if let Some(Value::String(m)) = e.get_mut("message") {
            *m = cut(m, 256);
        }
    }
    Value::Object(obj).to_string()
}

/// Formats a span's fields as a masked JSON object, kept in the span's extensions.
pub struct MaskingFields;

impl<'writer> FormatFields<'writer> for MaskingFields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        if let Some(m) = visitor.message {
            visitor
                .fields
                .insert("span_message".into(), Value::String(m));
        }
        write!(writer, "{}", Value::Object(visitor.fields))
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        let mut merged = match serde_json::from_str::<Value>(&current.fields) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        };
        merged.extend(visitor.fields);
        current.fields = Value::Object(merged).to_string();
        Ok(())
    }
}

/// Collects fields as masked JSON values; the message separately.
#[derive(Default)]
struct JsonVisitor {
    fields: Map<String, Value>,
    message: Option<String>,
}

impl JsonVisitor {
    fn put(&mut self, field: &Field, value: Value) {
        self.fields
            .insert(field.name().to_owned(), mask_field(field.name(), value));
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.put(field, Value::String(value.to_owned()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::from(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.put(field, Value::String(value.to_string()));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.put(field, Value::String(value.to_string()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::Bool(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let text = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(text);
        } else {
            self.put(field, Value::String(text));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid7_and_event_names() {
        assert!(is_uuid7("0192f6a1-7c3e-7b21-9a4d-5e6f70819203"));
        assert!(!is_uuid7("2465ecce-98d8-4143-8e19-4073541af2cb"), "a v4");
        assert!(
            !is_uuid7("0192F6A1-7c3e-7b21-9a4d-5e6f70819203"),
            "uppercase"
        );
        assert!(valid_event("routing.enqueued"));
        assert!(!valid_event("enqueued"));
        assert!(!valid_event("Routing.enqueued"));
    }

    #[test]
    fn fields_go_top_level_only_when_they_fit() {
        let mut r = Record::default();
        r.put("leg_id", Value::String("not-a-uuid".into()));
        r.put("attempt", Value::from(0));
        r.put("outcome", Value::String("ok".into()));
        r.put(
            "interaction_id",
            Value::String("2465ecce-98d8-4143-8e19-4073541af2cb".into()),
        );
        assert!(r.attrs.contains_key("leg_id") && r.attrs.contains_key("attempt"));
        assert_eq!(r.top["outcome"], "ok");
        assert!(r.top.contains_key("interaction_id") && !r.top.contains_key("xsid"));
        r.put(
            "interaction_id",
            Value::String("0192f6a1-7c3e-7b21-9a4d-5e6f70819203".into()),
        );
        assert!(r.top.contains_key("xsid") && !r.top.contains_key("interaction_id"));
        r.put("panic.location", Value::String("x".into()));
        assert!(r.attrs.contains_key("panic_location"));
        assert_eq!(snake("callerNumber"), "caller_number");
        assert_eq!(snake("DTMFDigit"), "dtmf_digit");
        assert_eq!(snake("Caller_ANI"), "caller_ani");
    }

    #[test]
    fn a_long_line_is_shortened_not_cut() {
        let mut obj = Map::new();
        obj.insert("event".into(), Value::String("a.b".into()));
        let mut a = Map::new();
        a.insert("big".into(), Value::String("x".repeat(20_000)));
        obj.insert("attrs".into(), Value::Object(a));
        let s = shorten(Value::Object(obj).to_string());
        assert!(s.len() <= MAX_LINE);
        assert!(serde_json::from_str::<Value>(&s).is_ok());
    }
}
