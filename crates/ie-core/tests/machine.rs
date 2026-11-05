//! The state machine: the main paths, then property tests of its invariants over random input
//! sequences (PLAN "Invariants", step 4's part).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ie_core::flow::FlowFile;
use ie_core::machine::{
    AgentLeg, AgentOutcome, Call, CallEvent, Cmd, CmdResult, ConsentBasis, Decision, DequeueReason,
    DtmfKind, DtmfResult, Effect, EndCause, EnqueueReason, Ids, Input, OverflowAction, Phase,
    QueueOutcome, QueueState, RecState, Reconcile, Route, RouteCmd, RouteResult, RouteSync,
    UNSENT_HOLD_MS,
};
use ie_core::{ConsentMode, say_prompt_id};
use proptest::prelude::*;
use std::sync::Arc;

const FILE: &str = r#"{
  "schema_version": 1,
  "flows": {
    "acme-main": {
      "tenant_id": "acme",
      "start": "menu",
      "steps": {
        "menu": [
          {"say": {"text": "Welcome to Acme.", "language": "en-GB"}},
          {"say": {"text": "Press 1 for sales.", "language": "en-GB"}},
          {"gather": {"num_digits": 1, "timeout_s": 5, "routes": {"1": "sales", "2": "queue", "3": "queue6"}, "default": "menu"}}
        ],
        "sales": [{"say": {"text": "Sales will be with you soon.", "language": "en-GB"}}, {"pause": {"seconds": 1}}, {"hangup": {}}],
        "queue": [{"dial": {"queue": "sales", "priority": 3}}, {"say": {"text": "Nobody is free.", "language": "en-GB"}}, {"hangup": {}}],
        "queue6": [{"dial": {"queue": "sales", "music": "hold-music", "announce_position": true}}, {"say": {"text": "Goodbye.", "language": "en-GB"}}, {"hangup": {}}],
        "spin": [{"goto": {"step": "spin"}}]
      },
      "no_input": [{"say": {"text": "Goodbye.", "language": "en-GB"}}, {"hangup": {}}]
    }
  },
  "numbers": {"+15550100100": {"flow": "acme-main"}},
  "consent": {"acme": {"mode": "announce_opt_out", "digit": "9", "window_s": 3, "record": true,
              "announce": [{"say": {"text": "Calls are recorded. Press 9 to opt out.", "language": "en-GB"}}]}}
}"#;

struct Counter(u64);
impl Ids for Counter {
    fn next_id(&mut self) -> String {
        self.0 += 1;
        format!("00000000-0000-7000-8000-{:012x}", self.0)
    }
}

fn route(mode: ConsentMode) -> Route {
    routed(mode, false)
}

fn routed(mode: ConsentMode, routing: bool) -> Route {
    routed_rec(mode, routing, false)
}

fn routed_rec(mode: ConsentMode, routing: bool, recording: bool) -> Route {
    let f = FlowFile::load(FILE, |n| (n == "+15550100100").then(|| "acme".to_owned())).unwrap();
    let mut consent = f.consent["acme"].clone();
    consent.mode = mode;
    Route {
        tenant_id: "acme".into(),
        flow_name: "acme-main".into(),
        flow: Arc::new(f.flows["acme-main"].clone()),
        consent,
        routing,
        recording,
    }
}

fn say(t: &str) -> String {
    say_prompt_id("en-GB", "", t)
}

/// A driver that answers every command with Done and records what went out.
struct Drive {
    call: Call,
    now: u64,
    out: Vec<Effect>,
}

impl Drive {
    fn new(mode: ConsentMode) -> Self {
        Self::with(route(mode))
    }
    fn with(r: Route) -> Self {
        let (call, fx) = Call::arrive(0, Some(r), "acme", Box::new(Counter(0)));
        assert_eq!(fx, vec![Effect::Claim]);
        let mut d = Self {
            call,
            now: 0,
            out: Vec::new(),
        };
        d.feed(Input::Claimed(5));
        d
    }
    /// Feeds an input, then answers each command it sends with Done (StopPlay and the rest).
    fn feed(&mut self, i: Input) {
        let mut fx = self.call.step(self.now, i);
        loop {
            let cmd = fx.iter().any(|e| matches!(e, Effect::Command(_)));
            self.out.append(&mut fx);
            if !cmd {
                break;
            }
            fx = self.call.step(self.now, Input::Done(CmdResult::Done));
        }
    }
    fn at(&mut self, ms: u64) {
        self.now = ms;
        self.feed(Input::Tick);
    }
    fn plays(&self) -> Vec<String> {
        self.out
            .iter()
            .filter_map(|e| match e {
                Effect::Command(Cmd::Play { prompt_id, .. }) => Some(prompt_id.clone()),
                _ => None,
            })
            .collect()
    }
    fn last_play_id(&self) -> String {
        self.out
            .iter()
            .rev()
            .find_map(|e| match e {
                Effect::Command(Cmd::Play { play_id, .. }) => Some(play_id.clone()),
                _ => None,
            })
            .unwrap()
    }
    fn collection(&self) -> String {
        self.out
            .iter()
            .rev()
            .find_map(|e| match e {
                Effect::Command(Cmd::DigitsOn { collection_id, .. }) => Some(collection_id.clone()),
                _ => None,
            })
            .unwrap()
    }
    fn finish_played(&mut self) {
        let p = self.last_play_id();
        self.feed(Input::PlayFinished {
            play_id: p,
            completed: true,
        });
    }
    fn press(&mut self, d: char) {
        let c = self.collection();
        self.feed(Input::Digit {
            collection_id: c,
            digit: d,
        });
    }
}

#[test]
fn welcome_press_1_reaches_sales_and_hangs_up() {
    let mut d = Drive::new(ConsentMode::Never);
    assert!(d.out.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::Never
    )));
    assert!(!d.out.contains(&Effect::StartRecording));
    assert_eq!(d.plays(), vec![say("Welcome to Acme.")]);
    d.finish_played();
    assert_eq!(d.plays().last(), Some(&say("Press 1 for sales.")));
    d.finish_played();
    assert_eq!(
        d.call.deadline(),
        Some(5_000),
        "the gather's first-digit timeout"
    );
    d.press('1');
    assert_eq!(d.plays().last(), Some(&say("Sales will be with you soon.")));
    assert!(d.out.contains(&Effect::Gathered {
        to: "sales".into(),
        digits: 1
    }));
    assert!(
        d.out
            .iter()
            .any(|e| matches!(e, Effect::Command(Cmd::DigitsOff { .. })))
    );
    d.finish_played();
    d.at(1_000);
    assert!(
        d.out
            .contains(&Effect::Command(Cmd::EndSession(EndCause::Normal)))
    );
    d.feed(Input::Ended {
        reason: "ended_by_owner".into(),
        sip_status: 0,
    });
    assert_eq!(d.call.phase(), Phase::Ended);
    let Some(Effect::Finish(s)) = d.out.last() else {
        panic!("no Finish")
    };
    assert_eq!(
        (s.last_step.as_str(), s.gathers, s.owner_epoch),
        ("sales", 1, 5)
    );
}

#[test]
fn a_digit_during_the_menu_prompt_barges_in() {
    let mut d = Drive::new(ConsentMode::Never);
    // The collection opens with the first prompt of the run that leads into the gather.
    assert!(
        d.out
            .iter()
            .any(|e| matches!(e, Effect::Command(Cmd::DigitsOn { .. })))
    );
    let welcome = d.last_play_id();
    d.press('1');
    assert!(
        d.out
            .contains(&Effect::Command(Cmd::StopPlay { play_id: welcome }))
    );
    assert_eq!(d.plays().last(), Some(&say("Sales will be with you soon.")));
}

#[test]
fn three_silent_menus_run_no_input_then_hang_up() {
    let mut d = Drive::new(ConsentMode::Never);
    for round in 0..3u64 {
        d.finish_played();
        d.finish_played();
        let t = d.call.deadline().unwrap();
        assert!(t >= round * 5_000);
        d.at(t);
    }
    assert!(d.out.contains(&Effect::Count("no_input_action")));
    assert_eq!(d.plays().last(), Some(&say("Goodbye.")));
    d.finish_played();
    assert!(
        d.out
            .contains(&Effect::Command(Cmd::EndSession(EndCause::Normal)))
    );
}

#[test]
fn consent_opt_out_refuses_and_silence_grants_recording() {
    let mut d = Drive::new(ConsentMode::AnnounceOptOut);
    assert_eq!(
        d.plays(),
        vec![say("Calls are recorded. Press 9 to opt out.")]
    );
    // No barge-in on the announcement: the digit waits for its end.
    d.press('9');
    assert!(
        !d.out
            .iter()
            .any(|e| matches!(e, Effect::ConsentDecided(..)))
    );
    assert!(
        !d.out
            .iter()
            .any(|e| matches!(e, Effect::Command(Cmd::StopPlay { .. })))
    );
    d.finish_played();
    assert!(d.out.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::OptedOut
    )));
    assert!(!d.out.contains(&Effect::StartRecording));
    assert_eq!(d.plays().last(), Some(&say("Welcome to Acme.")));

    let mut g = Drive::new(ConsentMode::AnnounceOptOut);
    g.finish_played();
    g.at(3_000);
    let pos = |e: &Effect| g.out.iter().position(|x| x == e);
    let decided = pos(&Effect::ConsentDecided(
        Decision::Granted,
        ConsentBasis::NotOptedOut,
    ))
    .unwrap();
    assert!(pos(&Effect::StartRecording).unwrap() > decided);
}

#[test]
fn consent_opt_in_needs_the_digit() {
    let mut d = Drive::new(ConsentMode::OptIn);
    d.finish_played();
    d.at(3_000);
    assert!(d.out.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::NoOptIn
    )));
    assert!(!d.out.contains(&Effect::StartRecording));
    let mut g = Drive::new(ConsentMode::OptIn);
    g.press('9');
    assert!(
        !g.out.contains(&Effect::StartRecording),
        "not before the announcement completed"
    );
    g.finish_played();
    assert!(g.out.contains(&Effect::ConsentDecided(
        Decision::Granted,
        ConsentBasis::OptedIn
    )));
    assert!(g.out.contains(&Effect::StartRecording));
}

/// The consent announcement's outcome is a legal fact (the user, 2026-11-03): recording only
/// after its `PlayFinished(COMPLETED)`; a `PlayFailed`, a `PlayFinished` other than completed,
/// or a late `PlayFinished` never switches recording on.
#[test]
fn consent_records_only_after_the_announcement_completed() {
    // PlayFailed (parity-ms unavailable) twice: tried once more, then no recording.
    let mut d = Drive::new(ConsentMode::AnnounceOptOut);
    let first = d.last_play_id();
    d.feed(Input::PlayFailed {
        play_id: first.clone(),
        prompt_unknown: false,
    });
    let second = d.last_play_id();
    assert_ne!(first, second, "tried once more, with a new play id");
    d.feed(Input::PlayFailed {
        play_id: second.clone(),
        prompt_unknown: false,
    });
    assert!(d.out.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::NotAnnounced
    )));
    // A late end of either play changes nothing.
    d.feed(Input::PlayFinished {
        play_id: second,
        completed: true,
    });
    d.feed(Input::PlayFinished {
        play_id: first,
        completed: true,
    });
    d.at(10_000);
    assert!(!d.out.contains(&Effect::StartRecording));
    assert_eq!(
        d.out
            .iter()
            .filter(|e| matches!(e, Effect::ConsentDecided(..)))
            .count(),
        1
    );

    // A missing prompt: skipped at once (no retry), no recording.
    let mut u = Drive::new(ConsentMode::AnnounceOptOut);
    let p = u.last_play_id();
    u.feed(Input::PlayFailed {
        play_id: p,
        prompt_unknown: true,
    });
    assert_eq!(
        u.plays().len(),
        2,
        "the flow's first prompt next, no retry: {:?}",
        u.plays()
    );
    assert!(u.out.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::NotAnnounced
    )));

    // Ended other than completed (stopped, replaced, failed): no recording, even in opt-in
    // with the digit pressed.
    for mode in [ConsentMode::AnnounceOptOut, ConsentMode::OptIn] {
        let mut s = Drive::new(mode);
        s.press('9');
        let p = s.last_play_id();
        s.feed(Input::PlayFinished {
            play_id: p.clone(),
            completed: false,
        });
        s.feed(Input::PlayFinished {
            play_id: p,
            completed: true,
        });
        s.at(10_000);
        assert!(
            s.out.contains(&Effect::ConsentDecided(
                Decision::Refused,
                ConsentBasis::NotAnnounced
            )),
            "{mode:?}"
        );
        assert!(!s.out.contains(&Effect::StartRecording), "{mode:?}");
    }

    // A PlayFinished of another play (an older one) doesn't count as the announcement's.
    let mut o = Drive::new(ConsentMode::AnnounceOptOut);
    o.feed(Input::PlayFinished {
        play_id: "00000000-0000-7000-8000-ffffffffffff".into(),
        completed: true,
    });
    o.at(10_000);
    assert!(
        !o.out
            .iter()
            .any(|e| matches!(e, Effect::ConsentDecided(..))),
        "still waiting for its own end"
    );
    assert!(!o.out.contains(&Effect::StartRecording));
}

/// IE-068 with the play saga's events: an IVR prompt whose play fails is tried once more with
/// a new play id, then skipped; a missing prompt is skipped at once; the call is never ended.
#[test]
fn ie_068_play_failed_events_retry_once_then_skip_and_never_end_the_call() {
    let mut d = Drive::new(ConsentMode::Never);
    assert_eq!(d.plays(), vec![say("Welcome to Acme.")]);
    let first = d.last_play_id();
    d.feed(Input::PlayFailed {
        play_id: first.clone(),
        prompt_unknown: false,
    });
    assert_eq!(
        d.plays(),
        vec![say("Welcome to Acme."), say("Welcome to Acme.")]
    );
    let second = d.last_play_id();
    assert_ne!(first, second);
    d.feed(Input::PlayFailed {
        play_id: second,
        prompt_unknown: false,
    });
    assert_eq!(
        d.plays().last(),
        Some(&say("Press 1 for sales.")),
        "skipped after one more try"
    );
    let p = d.last_play_id();
    d.feed(Input::PlayFailed {
        play_id: p,
        prompt_unknown: true,
    });
    assert!(
        !d.out
            .iter()
            .any(|e| matches!(e, Effect::Command(Cmd::EndSession(_)))),
        "never ended for a prompt"
    );
    assert_eq!(d.call.phase(), Phase::Ivr);
}

#[test]
fn arrivals_outside_the_map_or_with_another_tenant_are_refused_unclaimed() {
    let (c, fx) = Call::arrive(0, None, "acme", Box::new(Counter(0)));
    assert_eq!(fx, vec![Effect::Decline { sip_status: 404 }]);
    assert!(c.done() && !c.claimed());
    let (c, fx) = Call::arrive(
        0,
        Some(route(ConsentMode::Never)),
        "globex",
        Box::new(Counter(0)),
    );
    assert_eq!(
        fx,
        vec![Effect::TenantMismatch, Effect::Decline { sip_status: 403 }]
    );
    assert!(c.done() && !c.claimed());
}

#[test]
fn a_missing_prompt_is_skipped_and_counted() {
    let (mut c, _) = Call::arrive(
        0,
        Some(route(ConsentMode::Never)),
        "acme",
        Box::new(Counter(0)),
    );
    c.step(0, Input::Claimed(1));
    let fx = c.step(0, Input::Done(CmdResult::Done)); // Answer
    assert!(matches!(
        fx.last(),
        Some(Effect::Command(Cmd::DigitsOn { .. }))
    ));
    c.step(0, Input::Done(CmdResult::Done)); // DigitsOn -> Play(welcome)
    let fx = c.step(0, Input::Done(CmdResult::PromptUnknown));
    assert!(fx.contains(&Effect::Count("prompt_unknown")));
    assert!(
        matches!(fx.last(), Some(Effect::Command(Cmd::Play { prompt_id, .. })) if *prompt_id == say("Press 1 for sales."))
    );
}

#[test]
fn a_goto_loop_hits_the_guard() {
    let mut r = route(ConsentMode::Never);
    let mut flow = (*r.flow).clone();
    flow.start = "spin".into();
    r.flow = Arc::new(flow);
    let (mut c, _) = Call::arrive(0, Some(r), "acme", Box::new(Counter(0)));
    c.step(0, Input::Claimed(1));
    let fx = c.step(0, Input::Done(CmdResult::Done));
    assert!(fx.contains(&Effect::Count("goto_limit")));
    assert!(
        matches!(fx.last(), Some(Effect::Command(Cmd::Play { prompt_id, .. })) if *prompt_id == say("Goodbye."))
    );
}

#[test]
fn unmanaged_suspends_the_timers() {
    let mut d = Drive::new(ConsentMode::Never);
    d.finish_played();
    d.finish_played();
    assert!(d.call.deadline().is_some());
    d.feed(Input::Unmanaged);
    assert_eq!(d.call.deadline(), None);
    d.feed(Input::Managed);
    assert!(d.call.deadline().is_some());
}

// ---- the queue (step 5a) ------------------------------------------------------------------

const E1: &str = "0192f6a1-9000-7aaa-8bbb-000000000001";
const R1: &str = "0192f6a1-9100-7ccc-8ddd-000000000001";
const R2: &str = "0192f6a1-9100-7ccc-8ddd-000000000002";

impl Drive {
    /// A call with routing on (no consent prompt) that pressed 2: in the `dial` to `sales`.
    fn queued() -> Self {
        let mut d = Self::with(routed(ConsentMode::Never, true));
        d.finish_played();
        d.finish_played();
        d.press('2');
        d
    }
    fn routes(&self) -> Vec<RouteCmd> {
        self.out
            .iter()
            .filter_map(|e| match e {
                Effect::Route(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }
    fn last_route(&self) -> RouteCmd {
        self.routes().pop().unwrap()
    }
    fn assigned(&mut self, res: &str) {
        self.feed(Input::Assigned {
            reservation_id: res.into(),
            entry_id: E1.into(),
            agent_id: "agent-a1".into(),
            agent_contact: "sip:agent-a1@acme.agents.example".into(),
            deadline_ms: self.now + 30_000,
        });
    }
    /// The leg id of the latest agent leg placed.
    fn last_leg(&self) -> String {
        self.out
            .iter()
            .rev()
            .find_map(|e| match e {
                Effect::Command(Cmd::AddAgentLeg { leg_id, .. }) => Some(leg_id.clone()),
                _ => None,
            })
            .unwrap()
    }
    fn leg(&mut self, leg: AgentLeg) {
        let l = self.last_leg();
        self.feed(Input::AgentLeg { leg_id: l, leg });
    }
    fn removed(&self, leg: &str) -> bool {
        self.out
            .contains(&Effect::Command(Cmd::RemoveLeg { leg_id: leg.into() }))
    }
    fn legs_placed(&self) -> usize {
        self.out
            .iter()
            .filter(|e| matches!(e, Effect::Command(Cmd::AddAgentLeg { .. })))
            .count()
    }
    fn ended(&mut self) {
        self.feed(Input::Ended {
            reason: "remote_hangup".into(),
            sip_status: 0,
        });
    }
}

fn outcome(res: &str, outcome: AgentOutcome) -> RouteCmd {
    let detail = match outcome {
        AgentOutcome::Connected => "",
        AgentOutcome::AgentNoAnswer => "timeout",
        AgentOutcome::AgentLegFailed => "leg_failed",
        AgentOutcome::CallerGone => "ended",
        AgentOutcome::AlreadyAssigned => "already_assigned",
    };
    RouteCmd::ReportOutcome {
        reservation_id: res.into(),
        outcome,
        detail,
    }
}

#[test]
fn with_routing_off_a_dial_to_a_queue_is_skipped_and_counted() {
    let mut d = Drive::with(routed(ConsentMode::Never, false));
    d.finish_played();
    d.finish_played();
    d.press('2');
    assert!(d.routes().is_empty());
    assert!(d.out.contains(&Effect::Count("verb_unsupported")));
    assert_eq!(d.plays().last().unwrap(), &say("Nobody is free."));
}

#[test]
fn queued_offering_connected_then_the_caller_hangs_up_sends_call_ended() {
    let mut d = Drive::queued();
    let at = d.now;
    assert_eq!(
        d.routes(),
        vec![RouteCmd::Enqueue {
            enqueue_seq: 1,
            queue_id: "sales".into(),
            priority: 3,
            first_queued_ms: at,
            ordering_ms: at,
            reason: EnqueueReason::New,
        }]
    );
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    let leg = d.last_leg();
    assert!(d.out.contains(&Effect::Command(Cmd::AddAgentLeg {
        leg_id: leg.clone(),
        agent_contact: "sip:agent-a1@acme.agents.example".into(),
        record_locked: true,
    })));
    assert_eq!(d.call.queue_state(), Some(QueueState::Offering));
    d.leg(AgentLeg::Answered);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::Connected));
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert_eq!(d.call.queue_state(), Some(QueueState::Connected));
    d.now = 90_000;
    d.ended();
    assert!(d.out.iter().any(|e| matches!(e, Effect::Finish(_))));
    assert_eq!(
        d.last_route(),
        RouteCmd::CallEnded {
            agent_id: "agent-a1".into(),
            reservation_id: R1.into(),
            ended_ms: 90_000,
        }
    );
    assert_eq!(d.call.phase(), Phase::Closing);
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());
    assert_eq!(d.routes().len(), 3);
}

#[test]
fn a_caller_hanging_up_while_queued_sends_dequeue_abandoned_even_with_the_enqueue_in_flight() {
    // The entry is known.
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.ended();
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Abandoned
        }
    );
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());

    // The Enqueue is still in flight: the Dequeue follows its answer; the Finish doesn't wait.
    let mut d = Drive::queued();
    d.ended();
    assert!(d.out.iter().any(|e| matches!(e, Effect::Finish(_))));
    assert_eq!(d.routes().len(), 1);
    assert_eq!(d.call.phase(), Phase::Closing);
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Abandoned
        }
    );
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());

    // Refused while the caller left: nothing to dequeue.
    let mut d = Drive::queued();
    d.ended();
    d.feed(Input::Route(RouteResult::Unavailable));
    assert!(d.call.done());
    assert_eq!(d.routes().len(), 1);
}

#[test]
fn abandon_vs_assign_reports_caller_gone_once() {
    // Assigned arrives after the caller left (race-abandon-vs-assign, IE's side).
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.ended();
    d.assigned(R1);
    assert!(
        !d.out
            .iter()
            .any(|e| matches!(e, Effect::Command(Cmd::AddAgentLeg { .. })))
    );
    d.feed(Input::Route(RouteResult::Done));
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::CallerGone));
    d.assigned(R1); // redelivered
    d.feed(Input::Route(RouteResult::Recorded { connected: false }));
    assert!(d.call.done());
    assert_eq!(d.routes().len(), 3);

    // The caller leaves while the agent's leg is being placed: CALLER_GONE, then the Dequeue.
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    d.ended();
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::CallerGone));
    d.feed(Input::Route(RouteResult::Recorded { connected: false }));
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Abandoned
        }
    );
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());
}

#[test]
fn an_unanswered_agent_leg_goes_back_to_queued_without_a_new_enqueue() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    let t0 = d.now;
    d.assigned(R1);
    let leg1 = d.last_leg();
    assert_eq!(d.call.deadline(), Some(t0 + 10_000));
    d.at(t0 + 10_000);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::AgentNoAnswer));
    assert!(d.removed(&leg1), "the ringing leg is hung up");
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    d.feed(Input::Route(RouteResult::Done));
    // RE re-offers the same entry: a new reservation, the same Enqueue.
    d.assigned(R2);
    d.leg(AgentLeg::Failed);
    assert_eq!(d.last_route(), outcome(R2, AgentOutcome::AgentLegFailed));
    assert!(!d.removed(&d.last_leg()), "TC ended a failed leg itself");
    let enqueues = d
        .routes()
        .iter()
        .filter(|r| matches!(r, RouteCmd::Enqueue { .. }))
        .count();
    assert_eq!(enqueues, 1);
}

#[test]
fn a_revoke_or_a_late_connected_drops_the_leg_and_stays_queued() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    let leg1 = d.last_leg();
    d.feed(Input::Revoked {
        reservation_id: R1.into(),
    });
    assert!(d.removed(&leg1));
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    d.assigned(R2);
    let leg2 = d.last_leg();
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Recorded { connected: false }));
    assert!(d.removed(&leg2));
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    // A second agent while one is being placed: ALREADY_ASSIGNED.
    d.assigned(R1); // R1 was revoked: ignored
    let mut d2 = Drive::queued();
    d2.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d2.assigned(R1);
    d2.assigned(R1);
    d2.assigned(R2);
    assert_eq!(d2.last_route(), outcome(R2, AgentOutcome::AlreadyAssigned));
    assert_eq!(d2.legs_placed(), 1);
}

#[test]
fn a_refused_queue_and_overflows_go_on_with_the_flow() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Refused));
    assert!(d.out.contains(&Effect::Count("queue_refused")));
    assert_eq!(d.plays().last().unwrap(), &say("Nobody is free."));

    // A flow-step overflow: Dequeue(OVERFLOW) first, then the step.
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.feed(Input::Overflow {
        entry_id: E1.into(),
        action: OverflowAction::FlowStep("sales".into()),
    });
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Overflow
        }
    );
    assert_ne!(
        d.plays().last().unwrap(),
        &say("Sales will be with you soon.")
    );
    d.feed(Input::Route(RouteResult::Done));
    assert_eq!(
        d.plays().last().unwrap(),
        &say("Sales will be with you soon.")
    );
    // The caller leaving now sends nothing more to RE.
    let n = d.routes().len();
    d.ended();
    assert_eq!(d.routes().len(), n);

    // RE moved the entry to another queue: nothing for IE.
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.feed(Input::Overflow {
        entry_id: E1.into(),
        action: OverflowAction::Queue,
    });
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    assert_eq!(d.routes().len(), 1);
}

#[test]
fn a_routing_fence_drops_the_call() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Fenced));
    assert_eq!(d.call.phase(), Phase::Dropped);
}

// ---- property tests -----------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Ev {
    Done(CmdResult),
    Finished(bool),
    Digit(char, bool),
    Tick(u64),
    Ended,
    Resync(bool),
    Unmanaged,
    Managed,
    Claim(u8),
}

fn ev() -> impl Strategy<Value = Ev> {
    prop_oneof![
        6 => prop_oneof![
            Just(CmdResult::Done), Just(CmdResult::Done), Just(CmdResult::Done),
            Just(CmdResult::Failed), Just(CmdResult::Gone), Just(CmdResult::PromptUnknown), Just(CmdResult::Fenced)
        ].prop_map(Ev::Done),
        3 => any::<bool>().prop_map(Ev::Finished),
        3 => (prop::sample::select(vec!['1', '9', '2', '#']), any::<bool>()).prop_map(|(d, ok)| Ev::Digit(d, ok)),
        3 => (0u64..8_000).prop_map(Ev::Tick),
        1 => Just(Ev::Ended),
        1 => any::<bool>().prop_map(Ev::Resync),
        1 => Just(Ev::Unmanaged),
        1 => Just(Ev::Managed),
        1 => (0u8..3).prop_map(Ev::Claim),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Over any sequence: one command in flight at most; recording never before a granting
    /// decision; one decision at most; Finish at most once and only when claimed; nothing after
    /// the end; never two collections open at once.
    #[test]
    fn invariants_hold_over_random_inputs(
        mode in prop::sample::select(vec![ConsentMode::Never, ConsentMode::AnnounceOptOut, ConsentMode::OptIn]),
        events in prop::collection::vec(ev(), 1..120),
    ) {
        let (mut call, first) = Call::arrive(0, Some(route(mode)), "acme", Box::new(Counter(0)));
        let mut all = first;
        let mut now = 0u64;
        let mut in_flight = false;
        let mut last_play = String::new();
        let mut collection = String::new();
        let mut open: Option<String> = None;
        let mut granted = false;
        let mut decisions = 0;
        let mut finished = false;
        let mut ended = false;
        for e in events {
            let input = match e {
                Ev::Done(r) => Input::Done(r),
                Ev::Finished(ours) => Input::PlayFinished {
                    play_id: if ours { last_play.clone() } else { "00000000-0000-7000-8000-ffffffffffff".into() },
                    completed: true,
                },
                Ev::Digit(d, ours) => Input::Digit {
                    collection_id: if ours { collection.clone() } else { "x".into() },
                    digit: d,
                },
                Ev::Tick(dt) => { now += dt; Input::Tick }
                Ev::Ended => Input::Ended { reason: "all_legs_gone".into(), sip_status: 0 },
                Ev::Resync(e) => Input::Resync { ended: e },
                Ev::Unmanaged => Input::Unmanaged,
                Ev::Managed => Input::Managed,
                Ev::Claim(0) => Input::Claimed(7),
                Ev::Claim(1) => Input::ClaimTaken,
                Ev::Claim(_) => Input::ClaimUnavailable,
            };
            let is_done = matches!(input, Input::Done(_));
            let was_ended = ended;
            if is_done { in_flight = false; }
            let fx = call.step(now, input);
            for f in &fx {
                prop_assert!(!was_ended, "an effect after the end: {f:?}");
                match f {
                    Effect::Command(c) => {
                        prop_assert!(!in_flight, "two commands in flight: {c:?}");
                        in_flight = true;
                        match c {
                            Cmd::Play { play_id, .. } => last_play = play_id.clone(),
                            Cmd::DigitsOn { collection_id, .. } => {
                                prop_assert!(open.is_none(), "a second collection opened");
                                open = Some(collection_id.clone());
                                collection = collection_id.clone();
                            }
                            Cmd::DigitsOff { collection_id } => {
                                let was = open.take();
                                prop_assert_eq!(was.as_deref(), Some(collection_id.as_str()));
                            }
                            Cmd::EndSession(_) => open = None,
                            _ => {}
                        }
                    }
                    Effect::ConsentDecided(d, _) => {
                        decisions += 1;
                        prop_assert_eq!(decisions, 1, "two consent decisions");
                        granted = *d == Decision::Granted;
                    }
                    Effect::StartRecording => prop_assert!(granted, "recording before a granting decision"),
                    Effect::Finish(_) => {
                        prop_assert!(!finished, "two Finish effects (two CDRs)");
                        prop_assert!(call.claimed(), "Finish without a claim");
                        finished = true;
                    }
                    _ => {}
                }
            }
            all.extend(fx);
            if call.done() { ended = true; }
            if call.phase() == Phase::Ending { open = None; }
        }
        if call.recording() { prop_assert!(granted); }
        prop_assert!(all.iter().filter(|e| matches!(e, Effect::Finish(_))).count() <= 1);
    }
}

#[derive(Debug, Clone)]
enum Qe {
    /// A TC event or command result (as in the step-4 property test).
    Call(Ev),
    /// The answer to the routing request in flight: a choice among the results for its kind.
    Answer(u8),
    Assigned(u8, bool),
    Revoked(u8),
    Overflow(u8),
    Leg(u8, u8),
    Press2,
    Position(u8),
    Dtmf(u8, bool),
}

fn qe() -> impl Strategy<Value = Qe> {
    prop_oneof![
        6 => ev().prop_map(Qe::Call),
        6 => any::<u8>().prop_map(Qe::Answer),
        3 => (0u8..3, any::<bool>()).prop_map(|(r, ok)| Qe::Assigned(r, ok)),
        1 => (0u8..3).prop_map(Qe::Revoked),
        1 => (0u8..5).prop_map(Qe::Overflow),
        3 => ((0u8..3), (0u8..4)).prop_map(|(r, l)| Qe::Leg(r, l)),
        3 => Just(Qe::Press2),
        2 => (0u8..12).prop_map(Qe::Position),
        1 => ((0u8..3), any::<bool>()).prop_map(|(a, p)| Qe::Dtmf(a, p)),
    ]
}

fn res_id(i: u8) -> String {
    format!("0192f6a1-9100-7ccc-8ddd-00000000000{i}")
}

/// What the queue property test tracks across effects.
#[derive(Default)]
struct QueueSeen {
    in_flight: Option<RouteCmd>,
    session_ended: bool,
    dequeued: std::collections::BTreeSet<String>,
    /// Per reservation: the non-CONNECTED outcome sent, if any.
    other_outcome: std::collections::BTreeSet<String>,
    /// Reservations with a CONNECTED sent whose answer is awaited.
    connect_out: std::collections::BTreeSet<String>,
    /// Reservations whose last CONNECTED RE didn't answer (a retry may follow).
    connect_unanswered: std::collections::BTreeSet<String>,
    connect_sent: std::collections::BTreeSet<String>,
    connected: std::collections::BTreeSet<String>,
    call_ended: std::collections::BTreeSet<String>,
    /// IE-059: outcome RPCs RE didn't answer (each may go once more, unchanged).
    unanswered: Vec<RouteCmd>,
    legs: Vec<String>,
    finishes: u32,
    last_play: String,
    collection: String,
    in_cmd: bool,
}

impl QueueSeen {
    /// A repeat of an outcome RPC RE didn't answer (IE-059's retry): allowed once per answer.
    fn resend(&mut self, c: &RouteCmd) -> bool {
        match self.unanswered.iter().position(|u| u == c) {
            Some(i) => {
                self.unanswered.remove(i);
                true
            }
            None => false,
        }
    }

    fn check(&mut self, f: &Effect, was_done: bool) -> Result<(), TestCaseError> {
        prop_assert!(!was_done, "an effect after the end: {f:?}");
        match f {
            Effect::Route(c) => {
                prop_assert!(
                    self.in_flight.is_none(),
                    "two routing requests in flight: {c:?}"
                );
                self.in_flight = Some(c.clone());
                match c {
                    RouteCmd::Enqueue { .. } => {
                        prop_assert!(!self.session_ended, "Enqueue after the end")
                    }
                    RouteCmd::Dequeue { entry_id, .. } => {
                        prop_assert!(
                            self.dequeued.insert(entry_id.clone()) || self.resend(c),
                            "two Dequeues of {entry_id}"
                        );
                    }
                    RouteCmd::ReportOutcome {
                        reservation_id: r,
                        outcome: AgentOutcome::Connected,
                        ..
                    } => {
                        // IE-041: CONNECTED again only after RE didn't answer the last one.
                        prop_assert!(
                            !self.other_outcome.contains(r),
                            "CONNECTED after another outcome for {r}"
                        );
                        prop_assert!(
                            !self.connect_sent.contains(r) || self.connect_unanswered.remove(r),
                            "CONNECTED repeated though RE answered it: {r}"
                        );
                        self.connect_sent.insert(r.clone());
                        self.connect_out.insert(r.clone());
                    }
                    RouteCmd::ReportOutcome {
                        reservation_id: r, ..
                    } => {
                        prop_assert!(
                            !self.connect_sent.contains(r),
                            "another outcome after CONNECTED for {r}"
                        );
                        prop_assert!(
                            self.other_outcome.insert(r.clone()) || self.resend(c),
                            "two outcomes for {r}"
                        );
                    }
                    RouteCmd::CallEnded { reservation_id, .. } => {
                        prop_assert!(
                            self.connected.contains(reservation_id),
                            "CallEnded before RE recorded CONNECTED"
                        );
                        prop_assert!(
                            self.call_ended.insert(reservation_id.clone()) || self.resend(c),
                            "two CallEnded"
                        );
                    }
                }
            }
            Effect::Command(c) => {
                prop_assert!(!self.in_cmd, "two TC commands in flight: {c:?}");
                self.in_cmd = true;
                match c {
                    Cmd::AddAgentLeg { leg_id, .. } => {
                        prop_assert!(!self.session_ended, "an agent leg after the end");
                        prop_assert!(!self.legs.contains(leg_id), "a leg placed twice");
                        self.legs.push(leg_id.clone());
                    }
                    Cmd::Play { play_id, .. } => self.last_play = play_id.clone(),
                    Cmd::DigitsOn { collection_id, .. } => self.collection = collection_id.clone(),
                    _ => {}
                }
            }
            Effect::Finish(_) => self.finishes += 1,
            _ => {}
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The queue's invariants over any sequence: one routing request and one TC command in
    /// flight at most; no Enqueue once the session ended; one Dequeue per entry; per reservation
    /// one outcome kind (CONNECTED sent again only after RE didn't answer it, IE-041); each agent
    /// leg placed once and never after the end; CallEnded only for a reservation RE recorded
    /// CONNECTED, once; nothing after the end; every Finish once. With hold music, position
    /// announcements, recording and the agent's DTMF actions in play.
    #[test]
    fn queue_invariants_hold_over_random_inputs(
        music in any::<bool>(),
        events in prop::collection::vec(qe(), 1..150),
    ) {
        let (mut call, _) = Call::arrive(0, Some(routed_rec(ConsentMode::Never, true, true)), "acme", Box::new(Counter(0)));
        call.step(0, Input::Claimed(7));
        call.step(0, Input::Done(CmdResult::Done));
        let press = if music { '3' } else { '2' };
        let mut now = 0u64;
        let mut seen = QueueSeen::default();
        let mut entries = 0u32;
        let mut current_entry = String::new();
        for e in events {
            let input = match e {
                Qe::Call(Ev::Done(r)) => { seen.in_cmd = false; Input::Done(r) }
                Qe::Call(Ev::Finished(_)) => Input::PlayFinished { play_id: seen.last_play.clone(), completed: true },
                Qe::Call(Ev::Digit(d, _)) => Input::Digit { collection_id: seen.collection.clone(), digit: d },
                Qe::Call(Ev::Tick(dt)) => { now += dt; Input::Tick }
                Qe::Call(Ev::Ended) => Input::Ended { reason: "remote_hangup".into(), sip_status: 0 },
                Qe::Call(Ev::Resync(e)) => Input::Resync { ended: e },
                Qe::Call(Ev::Unmanaged) => Input::Unmanaged,
                Qe::Call(Ev::Managed) => Input::Managed,
                Qe::Call(Ev::Claim(_)) => { now += 15_000; Input::Tick }
                Qe::Press2 => Input::Digit { collection_id: seen.collection.clone(), digit: press },
                Qe::Answer(c) => {
                    let Some(req) = seen.in_flight.take() else { continue };
                    let r = match (&req, c % 6) {
                        (_, 0) => RouteResult::Fenced,
                        (_, 1) => RouteResult::Unavailable,
                        (RouteCmd::Enqueue { .. }, 2) => RouteResult::Refused,
                        (RouteCmd::Enqueue { .. }, _) => {
                            entries += 1;
                            current_entry = format!("0192f6a1-9000-7aaa-8bbb-00000000000{entries}");
                            RouteResult::Enqueued { entry_id: current_entry.clone(), position: 0 }
                        }
                        (RouteCmd::ReportOutcome { reservation_id, outcome: AgentOutcome::Connected, .. }, n) => {
                            let ok = n % 2 == 0;
                            if ok { seen.connected.insert(reservation_id.clone()); }
                            RouteResult::Recorded { connected: ok }
                        }
                        _ => RouteResult::Done,
                    };
                    if r == RouteResult::Unavailable
                        && !matches!(&req, RouteCmd::Enqueue { .. } | RouteCmd::ReportOutcome { outcome: AgentOutcome::Connected, .. })
                    {
                        seen.unanswered.push(req.clone());
                    }
                    if let RouteCmd::ReportOutcome { reservation_id, outcome: AgentOutcome::Connected, .. } = &req {
                        seen.connect_out.remove(reservation_id);
                        if r == RouteResult::Unavailable {
                            seen.connect_unanswered.insert(reservation_id.clone());
                        }
                    }
                    Input::Route(r)
                }
                Qe::Assigned(r, ok) => Input::Assigned {
                    reservation_id: res_id(r),
                    entry_id: if ok { current_entry.clone() } else { "0192f6a1-9000-7aaa-8bbb-0000000000ff".into() },
                    agent_id: "agent-a1".into(),
                    agent_contact: "sip:agent-a1@acme.agents.example".into(),
                    deadline_ms: now + 30_000,
                },
                Qe::Revoked(r) => Input::Revoked { reservation_id: res_id(r) },
                Qe::Overflow(a) => Input::Overflow {
                    entry_id: current_entry.clone(),
                    action: match a {
                        0 => OverflowAction::Queue,
                        1 => OverflowAction::CallbackOffer,
                        2 => OverflowAction::Voicemail,
                        3 => OverflowAction::Disconnect,
                        _ => OverflowAction::FlowStep("sales".into()),
                    },
                },
                Qe::Leg(r, l) => {
                    let Some(leg_id) = (!seen.legs.is_empty()).then(|| seen.legs[usize::from(r) % seen.legs.len()].clone()) else { continue };
                    Input::AgentLeg {
                        leg_id,
                        leg: [AgentLeg::Answered, AgentLeg::NoAnswer, AgentLeg::Failed, AgentLeg::Ended][usize::from(l)],
                    }
                }
                Qe::Position(p) => Input::Position { entry_id: current_entry.clone(), position: u32::from(p) },
                Qe::Dtmf(a, paused) => Input::DtmfAction {
                    action: [DtmfKind::RecordingPause, DtmfKind::RecordingResume, DtmfKind::MuteToggle][usize::from(a)],
                    result: DtmfResult::Applied,
                    recording: if paused { RecState::Paused } else { RecState::Active },
                    muted: paused,
                },
            };
            if matches!(input, Input::Ended { .. }) && call.claimed() && !call.done() {
                seen.session_ended = true;
            }
            let was_done = call.done();
            let fx = call.step(now, input);
            for f in &fx {
                seen.check(f, was_done)?;
            }
            // TC commands: answer Done so the flow moves (one in flight).
            let mut more = fx.iter().any(|f| matches!(f, Effect::Command(_)));
            while more {
                seen.in_cmd = false;
                let was_done = call.done();
                let fx2 = call.step(now, Input::Done(CmdResult::Done));
                more = fx2.iter().any(|f| matches!(f, Effect::Command(_)));
                for f in &fx2 {
                    seen.check(f, was_done)?;
                }
            }
        }
        prop_assert!(seen.finishes <= 1, "two Finish effects");
    }
}

/// The answer to the first command, then the session's end through the mailbox: the IE-049 path
/// (`session_ended`, then the client's Resync and Ended from the tombstone).
fn gone_on_answer() -> Call {
    let (mut call, _) = Call::arrive(
        0,
        Some(route(ConsentMode::Never)),
        "acme",
        Box::new(Counter(0)),
    );
    let fx = call.step(0, Input::Claimed(5));
    assert!(fx.contains(&Effect::Command(Cmd::Answer)));
    call.step(10, Input::Done(CmdResult::Gone));
    assert_eq!(call.phase(), Phase::Ending);
    call
}

#[test]
fn a_session_ended_answer_closes_from_the_tombstone_events() {
    let mut call = gone_on_answer();
    assert_eq!(call.deadline(), Some(10 + ie_core::machine::ENDING_MS));
    call.step(20, Input::Resync { ended: true });
    let fx = call.step(
        30,
        Input::Ended {
            reason: "ArrivalUnclaimed".into(),
            sip_status: 503,
        },
    );
    assert!(matches!(fx.as_slice(), [Effect::Finish(s)] if s.end_reason == "ArrivalUnclaimed"));
    assert!(call.done());
}

/// IE-050/IE-056: Ending asks TC at each deadline, the wait doubling up to 60 s; a session TC
/// still has gets EndSession again at each ask; unreachable (a partition) it keeps waiting, is
/// reported stuck once after 5 min, and never closes itself. Only TC's end (here the client's
/// `NodeDead`, on positive evidence) closes it.
#[test]
fn ending_never_closes_itself_it_re_ends_a_live_session_and_waits_out_a_partition() {
    use ie_core::machine::{ENDING_MAX_MS, ENDING_MS, ENDING_STUCK_MS};
    let mut call = gone_on_answer();
    let t0 = 10;
    call.step(t0, Input::Unmanaged);
    assert_eq!(
        call.deadline(),
        Some(t0 + ENDING_MS),
        "unmanaged, Ending still has a deadline"
    );
    assert!(call.step(t0 + ENDING_MS - 1, Input::Tick).is_empty());
    // First ask: TC still has it live. End it again.
    assert_eq!(
        call.step(t0 + ENDING_MS, Input::Tick),
        vec![Effect::Reconcile]
    );
    let fx = call.step(t0 + ENDING_MS, Input::Reconciled(Reconcile::Live));
    assert_eq!(fx, vec![Effect::Command(Cmd::EndSession(EndCause::System))]);
    call.step(t0 + ENDING_MS, Input::Done(CmdResult::Failed));
    // The waits double up to the cap; Live re-ends at each ask, Unreachable just waits.
    let mut at = t0 + ENDING_MS;
    let mut gaps = Vec::new();
    let mut stuck = 0;
    let mut ends = 1;
    for k in 0..12 {
        let next = call.deadline().unwrap();
        gaps.push(next - at);
        at = next;
        let fx = call.step(at, Input::Tick);
        stuck += fx
            .iter()
            .filter(|e| **e == Effect::Count("ending_stuck"))
            .count();
        assert!(fx.contains(&Effect::Reconcile), "{fx:?}");
        let r = if k % 2 == 0 {
            Reconcile::Unreachable
        } else {
            Reconcile::Live
        };
        let fx = call.step(at, Input::Reconciled(r));
        if r == Reconcile::Live {
            assert_eq!(fx, vec![Effect::Command(Cmd::EndSession(EndCause::System))]);
            call.step(at, Input::Done(CmdResult::Failed));
            ends += 1;
        } else {
            assert!(fx.is_empty());
        }
        assert!(!fx.iter().any(|e| matches!(e, Effect::Finish(_))));
        assert_eq!(call.phase(), Phase::Ending, "never closed without TC's end");
    }
    assert_eq!(
        &gaps[..4],
        &[2 * ENDING_MS, 4 * ENDING_MS, ENDING_MAX_MS, ENDING_MAX_MS]
    );
    assert!(gaps.iter().all(|g| *g <= ENDING_MAX_MS));
    assert!(at - t0 >= ENDING_STUCK_MS);
    assert_eq!(stuck, 1, "reported stuck once");
    assert_eq!(ends, 7);
    // The stream is back: ask at once.
    call.step(at + 1, Input::Managed);
    assert_eq!(call.deadline(), Some(at + 1));
    // The node is declared dead on positive evidence: the client ends the session.
    let fx = call.step(
        at + 2,
        Input::Ended {
            reason: "NodeDead".into(),
            sip_status: 0,
        },
    );
    assert!(matches!(fx.as_slice(), [Effect::Finish(s)] if s.end_reason == "NodeDead"));
    assert!(call.done());
}

#[test]
fn a_reconcile_that_finds_the_end_waits_for_it_and_a_fenced_one_stops() {
    use ie_core::machine::ENDING_MS;
    let mut call = gone_on_answer();
    assert_eq!(
        call.step(10 + ENDING_MS, Input::Tick),
        vec![Effect::Reconcile]
    );
    assert!(
        call.step(10 + ENDING_MS, Input::Reconciled(Reconcile::Ended))
            .is_empty()
    );
    let fx = call.step(
        10 + ENDING_MS + 1,
        Input::Ended {
            reason: "AllLegsGone".into(),
            sip_status: 0,
        },
    );
    assert!(matches!(fx.as_slice(), [Effect::Finish(_)]));
    let mut fenced = gone_on_answer();
    fenced.step(10 + ENDING_MS, Input::Tick);
    assert!(
        fenced
            .step(10 + ENDING_MS, Input::Reconciled(Reconcile::Fenced))
            .is_empty()
    );
    assert!(fenced.done());
}

// ---- step 6: hold music, announcements, the agent's leg, recording, DTMF, IE-041 ---------

impl Drive {
    /// Routing and recording on, consent granted (announce, not opted out), in `queue6`'s
    /// `dial` (hold music, position announcements).
    fn queued6() -> Self {
        let mut d = Self::with(routed_rec(ConsentMode::AnnounceOptOut, true, true));
        d.finish_played(); // the consent announcement
        let t = d.now + 3_000;
        d.at(t); // its window passes: granted
        d.finish_played();
        d.finish_played();
        d.press('3');
        d.feed(Input::Route(RouteResult::Enqueued {
            entry_id: E1.into(),
            position: 0,
        }));
        d
    }
    fn cmds(&self) -> Vec<Cmd> {
        self.out
            .iter()
            .filter_map(|e| match e {
                Effect::Command(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }
    fn music_id(&self) -> String {
        self.out
            .iter()
            .rev()
            .find_map(|e| match e {
                Effect::Command(Cmd::Play {
                    play_id,
                    looped: true,
                    ..
                }) => Some(play_id.clone()),
                _ => None,
            })
            .unwrap()
    }
}

#[test]
fn a_queued_caller_hears_music_and_positions_and_the_agent_leg_records() {
    let mut d = Drive::queued6();
    // Consent granted: the customer's leg records (a null sink until step 9).
    assert!(
        d.cmds().contains(&Cmd::StartRecording {
            agent_leg: None,
            recording_id: "00000000-0000-7000-8000-000000000002".into()
        }) || d.cmds().iter().any(|c| matches!(
            c,
            Cmd::StartRecording {
                agent_leg: None,
                ..
            }
        ))
    );
    assert!(
        !d.out.contains(&Effect::StartRecording),
        "no stub with recording on"
    );
    // Hold music, looped.
    assert!(d.cmds().iter().any(
        |c| matches!(c, Cmd::Play { prompt_id, looped: true, .. } if prompt_id == "hold-music")
    ));
    let music = d.music_id();
    // A position: the music stops, the position plays, then the music again.
    d.feed(Input::Position {
        entry_id: E1.into(),
        position: 3,
    });
    let n = d.cmds().len();
    assert!(d.cmds()[..n].contains(&Cmd::StopPlay {
        play_id: music.clone()
    }));
    assert_eq!(d.plays().last().unwrap(), "queue-position-3");
    assert!(
        d.out
            .contains(&Effect::Event(CallEvent::PositionAnnounced(3)))
    );
    d.finish_played();
    assert_eq!(d.plays().last().unwrap(), "hold-music");
    // A new position within 20 s waits for the gap.
    let t0 = d.now;
    d.feed(Input::Position {
        entry_id: E1.into(),
        position: 2,
    });
    assert_eq!(d.plays().last().unwrap(), "hold-music");
    assert_eq!(d.call.deadline(), Some(t0 + 20_000));
    d.at(t0 + 20_000);
    assert_eq!(d.plays().last().unwrap(), "queue-position-2");
    d.finish_played();
    // An agent accepts: the leg rings with the music on.
    d.assigned(R1);
    let leg = d.last_leg();
    assert!(d.cmds().contains(&Cmd::AddAgentLeg {
        leg_id: leg.clone(),
        agent_contact: "sip:agent-a1@acme.agents.example".into(),
        record_locked: false,
    }));
    let music = d.music_id();
    assert!(!d.cmds().contains(&Cmd::StopPlay {
        play_id: music.clone()
    }));
    // It answers: the music stops, the agent's leg records, then CONNECTED.
    d.leg(AgentLeg::Answered);
    let cmds = d.cmds();
    let stop = cmds
        .iter()
        .position(|c| {
            *c == Cmd::StopPlay {
                play_id: music.clone(),
            }
        })
        .unwrap();
    let rec = cmds
        .iter()
        .position(|c| matches!(c, Cmd::StartRecording { agent_leg: Some(l), .. } if *l == leg))
        .unwrap();
    assert!(stop < rec);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::Connected));
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert!(d.out.contains(&Effect::Event(CallEvent::AgentConnected)));
    // The CDR's open row: at answer, at the queue, at connect (with the agent).
    let progress: Vec<_> = d
        .out
        .iter()
        .filter_map(|e| match e {
            Effect::Progress(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(progress.len() >= 3);
    let last = progress.last().unwrap();
    assert_eq!(
        (
            last.agent_id.as_str(),
            last.reservation_id.as_str(),
            last.queue_id.as_str()
        ),
        ("agent-a1", R1, "sales")
    );
    assert_eq!(last.queue_outcome, QueueOutcome::Connected);
    assert_eq!(last.ended_ms, 0, "an open row");
    // No position is announced once connected.
    d.feed(Input::Position {
        entry_id: E1.into(),
        position: 1,
    });
    assert_ne!(d.plays().last().unwrap(), "queue-position-1");
}

#[test]
fn the_agents_dtmf_actions_are_events_without_digits() {
    let mut d = Drive::queued6();
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    let act = |a, r, rec, muted| Input::DtmfAction {
        action: a,
        result: r,
        recording: rec,
        muted,
    };
    d.feed(act(
        DtmfKind::RecordingPause,
        DtmfResult::Applied,
        RecState::Paused,
        false,
    ));
    d.feed(act(
        DtmfKind::RecordingPause,
        DtmfResult::Noop,
        RecState::Paused,
        false,
    ));
    d.feed(act(
        DtmfKind::RecordingResume,
        DtmfResult::Applied,
        RecState::Active,
        false,
    ));
    d.feed(act(
        DtmfKind::MuteToggle,
        DtmfResult::Applied,
        RecState::Active,
        true,
    ));
    d.feed(act(
        DtmfKind::MuteToggle,
        DtmfResult::Applied,
        RecState::Active,
        false,
    ));
    d.feed(act(
        DtmfKind::RecordingPause,
        DtmfResult::Locked,
        RecState::Active,
        false,
    ));
    let events: Vec<_> = d
        .out
        .iter()
        .filter_map(|e| match e {
            Effect::Event(ev)
                if !matches!(
                    ev,
                    CallEvent::RecordingStarted(_) | CallEvent::AgentConnected
                ) =>
            {
                Some(ev.name())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        events,
        vec!["recording_paused", "recording_resumed", "muted", "unmuted"]
    );
    assert!(d.out.contains(&Effect::Count("dtmf_action_noop")));
    assert!(d.out.contains(&Effect::Count("dtmf_action_locked")));
    d.ended();
    let fin = d
        .out
        .iter()
        .find_map(|e| match e {
            Effect::Finish(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((fin.recording_pauses, fin.mutes), (1, 1));
}

#[test]
fn recording_off_or_refused_starts_nothing_and_locks_the_facet() {
    // Recording off (no null sink): consent counts as allowed, nothing is started.
    let mut d = Drive::with(routed_rec(ConsentMode::AnnounceOptOut, true, false));
    d.finish_played();
    let t = d.now + 3_000;
    d.at(t);
    assert!(d.out.contains(&Effect::StartRecording));
    assert!(
        !d.cmds()
            .iter()
            .any(|c| matches!(c, Cmd::StartRecording { .. }))
    );
    // Consent refused (opted out): no recording, and the agent's DTMF can't touch the facet.
    let mut d = Drive::with(routed_rec(ConsentMode::AnnounceOptOut, true, true));
    d.press('9');
    d.finish_played();
    d.finish_played();
    d.press('2');
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    assert!(
        !d.cmds()
            .iter()
            .any(|c| matches!(c, Cmd::StartRecording { .. }))
    );
    assert!(d.cmds().iter().any(|c| matches!(
        c,
        Cmd::AddAgentLeg {
            record_locked: true,
            ..
        }
    )));
}

#[test]
fn ie_041_an_unanswered_connected_is_retried_then_given_up_without_double_booking() {
    let mut d = Drive::queued6();
    d.assigned(R1); // RE's deadline: now + 30 s
    let t0 = d.now;
    let leg = d.last_leg();
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Unavailable));
    assert!(d.out.contains(&Effect::Count("connect_retry")));
    assert_eq!(d.call.queue_state(), Some(QueueState::Offering));
    // Sent again after 500 ms, then 1 s, 2 s...
    assert_eq!(d.call.deadline(), Some(t0 + 500));
    d.at(t0 + 500);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::Connected));
    d.feed(Input::Route(RouteResult::Unavailable));
    assert_eq!(d.call.deadline(), Some(t0 + 1_500));
    d.at(t0 + 1_500);
    // RE answers: recorded; nothing more.
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert_eq!(d.call.queue_state(), Some(QueueState::Connected));
    let connects = d
        .routes()
        .iter()
        .filter(|r| **r == outcome(R1, AgentOutcome::Connected))
        .count();
    assert_eq!(connects, 3);
    assert!(!d.removed(&leg));

    // RE never answers: past its deadline + 12 s the agent's leg is hung up (RE has freed the
    // agent: keeping the conversation would double-book them) and the caller waits again.
    let mut d = Drive::queued6();
    d.assigned(R1);
    let t0 = d.now;
    let leg = d.last_leg();
    d.leg(AgentLeg::Answered);
    let mut t = t0;
    loop {
        d.feed(Input::Route(RouteResult::Unavailable));
        if d.removed(&leg) {
            break;
        }
        t = d.call.deadline().unwrap();
        assert!(t <= t0 + 42_000 + 4_000, "gives up by RE's deadline + 12 s");
        d.at(t);
    }
    assert!(t >= t0 + 30_000, "not before RE's deadline");
    assert!(d.out.contains(&Effect::Count("connect_given_up")));
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    assert_eq!(d.plays().last().unwrap(), "hold-music");
    assert!(
        !d.routes()
            .iter()
            .any(|r| matches!(r, RouteCmd::CallEnded { .. }))
    );
}

#[test]
fn ie_041_the_caller_leaving_during_an_unsettled_connected_waits_for_its_answer() {
    // RE recorded it after all: CallEnded (wrap-up), not a Dequeue.
    let mut d = Drive::queued6();
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Unavailable));
    d.ended();
    assert_eq!(d.call.phase(), Phase::Closing);
    let t = d.call.deadline().unwrap();
    d.at(t);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::Connected));
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert!(matches!(d.last_route(), RouteCmd::CallEnded { .. }));
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());

    // RE expired it: the entry is back in RE, so it is taken out.
    let mut d = Drive::queued6();
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    d.ended();
    d.feed(Input::Route(RouteResult::Recorded { connected: false }));
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Abandoned
        }
    );
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());
}

#[test]
fn the_agent_hanging_up_ends_the_dial_and_the_flow_goes_on() {
    let mut d = Drive::queued6();
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    d.now += 60_000;
    d.leg(AgentLeg::Ended);
    assert!(
        matches!(d.last_route(), RouteCmd::CallEnded { ref reservation_id, .. } if reservation_id == R1)
    );
    // queue6's next verb: "Goodbye.", then the hang-up.
    assert_eq!(d.plays().last().unwrap(), &say("Goodbye."));
    d.finish_played();
    assert!(d.cmds().contains(&Cmd::EndSession(EndCause::Normal)));
    d.feed(Input::Route(RouteResult::Done));
    d.ended();
    let fin = d
        .out
        .iter()
        .find_map(|e| match e {
            Effect::Finish(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(fin.queue_outcome, QueueOutcome::Connected);
    assert_eq!(fin.agent_id, "agent-a1");
    assert_eq!(fin.offers, 1);
    assert!(fin.connected_ms.is_some() && fin.queued_ms.is_some());
}

#[test]
fn an_abandoned_call_is_recorded_as_abandoned() {
    let mut d = Drive::queued6();
    d.ended();
    let fin = d
        .out
        .iter()
        .find_map(|e| match e {
            Effect::Finish(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(fin.queue_outcome, QueueOutcome::Abandoned);
    assert_eq!(fin.queue_id, "sales");
    assert!(fin.dequeued_ms.is_some());
    assert_eq!(
        d.last_route(),
        RouteCmd::Dequeue {
            entry_id: E1.into(),
            reason: DequeueReason::Abandoned
        }
    );
}

/// TC refusing a recording (no sink on the node) is reported and the call goes on: queued,
/// offered, bridged, connected.
#[test]
fn a_refused_recording_never_ends_or_stalls_the_call() {
    let mut d = Drive::with(routed_rec(ConsentMode::AnnounceOptOut, true, true));
    d.finish_played();
    let t = d.now + 3_000;
    // The consent window passes: StartRecording goes out (after the collection closes); TC
    // refuses it (invalid_state).
    d.now = t;
    let mut fx = d.call.step(t, Input::Tick);
    for _ in 0..5 {
        let rec = fx.iter().any(|e| {
            matches!(
                e,
                Effect::Command(Cmd::StartRecording {
                    agent_leg: None,
                    ..
                })
            )
        });
        d.out.append(&mut fx);
        if rec {
            break;
        }
        fx = d.call.step(t, Input::Done(CmdResult::Done));
    }
    assert!(d.cmds().iter().any(|c| matches!(
        c,
        Cmd::StartRecording {
            agent_leg: None,
            ..
        }
    )));
    d.feed(Input::Done(CmdResult::Failed));
    assert!(
        d.out
            .contains(&Effect::Event(CallEvent::RecordingFailed(false)))
    );
    assert_ne!(d.call.phase(), Phase::Ending);
    d.finish_played();
    d.finish_played();
    d.press('2');
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::Connected));
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert_eq!(d.call.queue_state(), Some(QueueState::Connected));
    assert_eq!(d.call.summary().recording_failures, 1);
}

/// RE's position 0 (a reserved entry has none) is never announced and sets no timer.
#[test]
fn position_zero_is_never_announced_and_sets_no_timer() {
    let mut d = Drive::queued6();
    let before = d.plays().len();
    d.feed(Input::Position {
        entry_id: E1.into(),
        position: 0,
    });
    assert_eq!(d.plays().len(), before);
    let dl = d.call.deadline().unwrap();
    assert!(dl > d.now + 1_000, "no timer due now: {dl} at {}", d.now);
}

// ---- IE-059: a routing-engine leader change ---------------------------------------------------

fn sync(entry: Option<&str>, res: &[(&str, bool)]) -> Input {
    Input::Reattached(Some(RouteSync {
        entry_id: entry.map(str::to_owned),
        reservations: res.iter().map(|(r, c)| ((*r).to_owned(), *c)).collect(),
    }))
}

#[test]
fn ie_059_a_queued_caller_keeps_their_place_across_a_leader_change() {
    let mut d = Drive::queued6();
    assert!(d.call.reattach_wanted());
    let cmds = d.cmds().len();
    let routes = d.routes().len();
    // The new leader holds the entry: nothing is sent, the music goes on.
    d.feed(sync(Some(E1), &[]));
    assert_eq!(d.cmds().len(), cmds, "no media change");
    assert_eq!(d.routes().len(), routes, "no new Enqueue");
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
    // A leader without the entry: enqueued again with the same seq and times.
    let first = d.routes()[0].clone();
    d.feed(sync(None, &[]));
    assert_eq!(
        d.last_route(),
        first,
        "the same enqueue_seq and ordering time"
    );
    assert!(d.out.contains(&Effect::Count("routing_reenqueued")));
    assert_eq!(d.cmds().len(), cmds, "the caller hears no change");
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: "0192f6a1-9000-7aaa-8bbb-000000000009".into(),
        position: 0,
    }));
    // Messages about the new entry reach the call.
    d.feed(Input::Position {
        entry_id: "0192f6a1-9000-7aaa-8bbb-000000000009".into(),
        position: 1,
    });
    assert_eq!(d.plays().last().unwrap(), "queue-position-1");
}

#[test]
fn ie_059_a_connected_call_is_untouched_and_an_unsent_outcome_is_sent_after_reattach() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    d.feed(Input::Route(RouteResult::Recorded { connected: true }));
    assert_eq!(d.call.queue_state(), Some(QueueState::Connected));
    let cmds = d.cmds().len();
    d.feed(sync(None, &[(R1, true)]));
    assert_eq!(d.cmds().len(), cmds, "the connected call is never touched");
    assert_eq!(d.call.queue_state(), Some(QueueState::Connected));
    // The caller hangs up during the failover: CallEnded finds RE unavailable for its 15 s.
    d.now = 60_000;
    d.ended();
    let call_ended = d.last_route();
    assert!(matches!(call_ended, RouteCmd::CallEnded { .. }));
    d.feed(Input::Route(RouteResult::Unavailable));
    assert_eq!(d.call.phase(), Phase::Closing, "held for the leader change");
    // The new leader answers the call's Reattach: CallEnded goes again.
    d.now = 70_000;
    d.feed(sync(None, &[(R1, true)]));
    assert_eq!(d.last_route(), call_ended);
    assert_eq!(d.routes().iter().filter(|r| **r == call_ended).count(), 2);
    d.feed(Input::Route(RouteResult::Done));
    assert!(d.call.done());
}

#[test]
fn mf_1b_a_call_mid_offer_that_re_forgot_is_enqueued_again_when_the_offer_ends() {
    let mut d = Drive::queued6();
    let first = d.routes()[0].clone();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.assigned(R1);
    let routes = d.routes().len();
    // RE restarted without its state while the agent's leg rings: no entry, no reservation.
    d.feed(sync(None, &[]));
    assert!(
        d.out
            .contains(&Effect::Count("routing_forgotten_mid_offer"))
    );
    assert_eq!(
        d.routes().len(),
        routes,
        "nothing yet: the reservation is still out"
    );
    // The agent's leg isn't answered: the outcome goes (RE: reservation_unknown), then the
    // caller, waiting again with no entry in RE, is enqueued again with the same seq and times.
    d.leg(AgentLeg::NoAnswer);
    assert_eq!(d.last_route(), outcome(R1, AgentOutcome::AgentNoAnswer));
    d.feed(Input::Route(RouteResult::Refused));
    assert_eq!(
        d.last_route(),
        first,
        "the same enqueue_seq and ordering time"
    );
    assert_eq!(d.call.queue_state(), Some(QueueState::Queued));
}

#[test]
fn ie_059_an_unsent_outcome_without_a_leader_change_is_given_up_after_the_hold() {
    let mut d = Drive::queued();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    d.now = 1_000;
    d.ended();
    assert!(matches!(d.last_route(), RouteCmd::Dequeue { .. }));
    let dequeue = d.last_route();
    d.feed(Input::Route(RouteResult::Unavailable));
    assert_eq!(d.call.phase(), Phase::Closing);
    // A retry timer (5 s, then 10 s, 20 s...): it only sends again; RE's answer settles it.
    assert_eq!(d.call.deadline(), Some(1_000 + 5_000));
    d.at(6_000);
    assert_eq!(d.last_route(), dequeue);
    assert!(d.out.contains(&Effect::Count("routing_outcome_retried")));
    d.feed(Input::Route(RouteResult::Unavailable));
    assert_eq!(d.call.deadline(), Some(6_000 + 10_000));
    // Never answered: given up at the hold (L18: RE dropped the interaction by then).
    for t in [16_000, 36_000, 56_000] {
        d.at(t);
        if d.call.done() {
            break;
        }
        d.feed(Input::Route(RouteResult::Unavailable));
    }
    d.at(1_000 + UNSENT_HOLD_MS);
    assert!(d.out.contains(&Effect::Count("routing_outcome_lost")));
    assert!(d.call.done());
}

// ---- IE-068 / IE-069: prompts that fail, plays on attach ---------------------------------------

/// Steps `c` with `i`, then answers every command but the last with `Done`; the effects.
fn step_all(c: &mut Call, i: Input) -> Vec<Effect> {
    c.step(0, i)
}

fn play_cmds(fx: &[Effect]) -> Vec<String> {
    fx.iter()
        .filter_map(|e| match e {
            Effect::Command(Cmd::Play { prompt_id, .. }) => Some(prompt_id.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn ie_068_a_failed_ivr_prompt_is_tried_once_more_then_skipped_never_dropping_the_call() {
    let (mut c, _) = Call::arrive(
        0,
        Some(route(ConsentMode::Never)),
        "acme",
        Box::new(Counter(0)),
    );
    let mut fx = step_all(&mut c, Input::Claimed(5)); // Answer
    fx.extend(step_all(&mut c, Input::Done(CmdResult::Done))); // -> DigitsOn (barge-in)
    fx.extend(step_all(&mut c, Input::Done(CmdResult::Done))); // -> Play "Welcome"
    assert_eq!(play_cmds(&fx), vec![say("Welcome to Acme.")]);
    // UNAVAILABLE (parity-ms's prompt wait or TC's timeout): the same prompt once more.
    let fx = step_all(&mut c, Input::Done(CmdResult::Failed));
    assert!(fx.contains(&Effect::Count("prompt_retried")));
    assert_eq!(play_cmds(&fx), vec![say("Welcome to Acme.")]);
    // Failed again: skipped; the flow goes on with the next prompt. The call is never ended.
    let fx = step_all(&mut c, Input::Done(CmdResult::Failed));
    assert!(fx.contains(&Effect::Count("prompt_failed_skipped")));
    assert_eq!(play_cmds(&fx), vec![say("Press 1 for sales.")]);
    assert!(
        !fx.iter()
            .any(|e| matches!(e, Effect::Command(Cmd::EndSession(_))))
    );
    assert_eq!(c.phase(), Phase::Ivr);
}

#[test]
fn ie_068_a_consent_announcement_that_fails_means_no_recording_and_the_call_goes_on() {
    let (mut c, _) = Call::arrive(
        0,
        Some(routed_rec(ConsentMode::AnnounceOptOut, false, true)),
        "acme",
        Box::new(Counter(0)),
    );
    let mut fx = c.step(0, Input::Claimed(5));
    fx.extend(c.step(0, Input::Done(CmdResult::Done))); // Answer -> consent: DigitsOn, Play
    // DigitsOn done, then the announcement's Play fails twice.
    fx.extend(c.step(0, Input::Done(CmdResult::Done)));
    fx.extend(c.step(0, Input::Done(CmdResult::Failed)));
    fx.extend(c.step(0, Input::Done(CmdResult::Failed)));
    assert!(fx.contains(&Effect::ConsentDecided(
        Decision::Refused,
        ConsentBasis::NotAnnounced
    )));
    assert!(!c.recording(), "an unannounced caller is never recorded");
    assert!(
        !fx.iter()
            .any(|e| matches!(e, Effect::Command(Cmd::StartRecording { .. })))
    );
    assert_eq!(c.phase(), Phase::Ivr, "the flow goes on");
}

#[test]
fn ie_069_the_agent_answering_stops_every_play_that_may_still_run() {
    let mut d = Drive::queued6();
    let music = d.music_id();
    d.feed(Input::Route(RouteResult::Enqueued {
        entry_id: E1.into(),
        position: 0,
    }));
    // A position: StopPlay(music), then the announcement's Play, which TC answers UNAVAILABLE
    // (its timeout) although parity-ms may have started it (MS-042's residual).
    let mut fx = d.call.step(
        d.now,
        Input::Position {
            entry_id: E1.into(),
            position: 2,
        },
    );
    fx.extend(d.call.step(d.now, Input::Done(CmdResult::Done)));
    let announce = fx
        .iter()
        .find_map(|e| match e {
            Effect::Command(Cmd::Play {
                play_id,
                looped: false,
                ..
            }) => Some(play_id.clone()),
            _ => None,
        })
        .unwrap();
    fx.extend(d.call.step(d.now, Input::Done(CmdResult::Failed)));
    d.out.append(&mut fx);
    // The music comes back (a new play); Drive answers it.
    d.feed(Input::Done(CmdResult::Done));
    let music2 = d.music_id();
    assert_ne!(music2, music);
    d.assigned(R1);
    d.leg(AgentLeg::Answered);
    let cmds = d.cmds();
    assert!(
        cmds.contains(&Cmd::StopPlay { play_id: music2 }),
        "the current music"
    );
    assert!(
        cmds.contains(&Cmd::StopPlay { play_id: announce }),
        "and the failed announcement that may still run"
    );
    assert!(d.out.contains(&Effect::Count("plays_stopped_on_attach")));
}
