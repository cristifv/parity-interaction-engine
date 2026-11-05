#!/usr/bin/env python3
"""IE step 6 e2e: the checks over one run's evidence directory (run.sh calls it at teardown).

  analyse.py <evidence dir>     writes summary.md and summary.json there; exit 0 when every check
                                passes, 1 otherwise.

Reads: marks.jsonl, interaction-engine.log (log-record.v1: `event`, `xsid`, `attrs`),
ie-log-check.txt, parity-gateway.log, cdr.jsonl, cdr-progress.jsonl, page-a1.jsonl, page-a2.jsonl,
media.json, a1-heard*.json, a1-reload.json, ie-after-*.txt, call-{a,b,c}.iid, inputs.txt.
Standard library only.
"""
import json
import os
import re
import sys


def lines(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()
    except FileNotFoundError:
        return []


def jsonl(path):
    out = []
    for line in lines(path):
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def text(path):
    return "\n".join(lines(path)).strip()


def main(d):
    j = lambda n: os.path.join(d, n)  # noqa: E731
    marks = jsonl(j("marks.jsonl"))
    mt = {m["what"]: m["t"] for m in marks}
    ie = jsonl(j("interaction-engine.log"))
    ie_raw = text(j("interaction-engine.log"))
    cdr = jsonl(j("cdr.jsonl"))
    progress_raw = text(j("cdr-progress.jsonl"))
    a1 = jsonl(j("page-a1.jsonl"))
    a2 = jsonl(j("page-a2.jsonl"))
    media = load(j("media.json"), {}) or {}
    iid_a, iid_b, iid_c = text(j("call-a.iid")), text(j("call-b.iid")), text(j("call-c.iid"))
    recording = "null_sink" in text(j("inputs.txt"))
    checks = []

    def check(ac, what, ok, detail=""):
        checks.append({"ac": ac, "check": what, "pass": bool(ok), "detail": detail})

    def of(iid, event=None):
        # log-record.v1: the xsid (a UUIDv7 interaction id; else interaction_id), the event name.
        return [e for e in ie if iid and iid in (e.get("xsid"), e.get("interaction_id"))
                and (event is None or e.get("event") == event)]

    def attr(e, k):
        return (e.get("attrs") or {}).get(k)

    def frames(page, kind, after=None):
        return [e for e in page if e.get("kind") == "frame" and e.get("type") == kind
                and (after is None or e.get("t", 0) / 1000 >= after)]

    # ---- AC1: PLAN step 5 through the real routing engine ---------------------------------------
    enq = of(iid_a, "routing.enqueued")
    check(1, "call A queued in sales through parity-cc-routing (one Enqueue)", len(enq) == 1,
          f"{len(enq)} enqueued (queue {enq[0].get('queue_id') if enq else None})")
    a2_offers = frames(a2, "offer")
    a1_offers = frames(a1, "offer") + [e for e in frames(a1, "snapshot")]
    check(1, "the agent ready longest (a2, ready 3 s before a1) got the offer first",
          "C a2 offered (ignores it)" in mt and any(o.get("interaction_id") == iid_a for o in a2_offers),
          f"a2 offers: {len(a2_offers)}")
    # The offers' own times (the page's frames): a2's offer arrives as a2 turns ready, before the
    # driver asks for it, so the marks would understate the gap.
    o2 = [e.get("t", 0) / 1000 for e in a2_offers if e.get("interaction_id") == iid_a]
    o1 = [e.get("t", 0) / 1000 for e in frames(a1, "offer") if e.get("interaction_id") == iid_a]
    t_a2 = min(o2) if o2 else mt.get("C a2 offered (ignores it)")
    t_a1 = min((t for t in o1 if t_a2 and t > t_a2), default=None) or mt.get("C a1 offered after the 15 s timeout")
    gap = round(t_a1 - t_a2, 1) if t_a1 and t_a2 else None
    a2_missed = [e for e in a2 if e.get("kind") == "frame" and e.get("type") == "agent_state" and e.get("state") == "not_ready"
                 and t_a2 and e.get("t", 0) / 1000 > t_a2]
    check(1, "the ignored offer went to a1 after the 15 s timeout, the entry's enqueue time kept (no new Enqueue)",
          gap is not None and 13 <= gap <= 25 and len(enq) == 1,
          f"a1 offered {gap} s after a2; a2 not ready afterwards: {bool(a2_missed)}")
    deq_b = [e for e in of(iid_b, "routing.dequeued") if "Abandoned" in str(attr(e, "reason"))]
    check(1, "call B hung up while queued: Dequeue(ABANDONED)", bool(deq_b),
          f"{len(deq_b)} Dequeue(ABANDONED) answered")

    # ---- AC2: gateway 5b ------------------------------------------------------------------------
    for name, page in (("a1", a1), ("a2", a2)):
        reg = [e for e in page if e.get("kind") == "sip_registration" and e.get("state") == "Registered"]
        check(2, f"{name}: logged in through the gateway, SIP registered over WS", bool(reg) and bool(frames(page, "snapshot")))
    reload = load(j("a1-reload.json"), {}) or {}
    t_reload = mt.get("D a1 reloaded")
    later_offer = [e for e in frames(a1, "offer", t_reload) if e.get("interaction_id") == iid_a] if t_reload else []
    check(2, "after the reconnect a1's page shows the offer and accepts it",
          bool(reload.get("offer_shown")) or bool(later_offer),
          "from the re-attach snapshot" if reload.get("offer_shown") else
          f"re-offered on the new session ({len(later_offer)} offer frame): RE revoked it at the detach")
    reserved = [e for e in frames(a1, "reserved") if e.get("interaction_id") == iid_a]
    auto = [e for e in a1 if e.get("kind") == "auto_answer" and e.get("interaction_id") == iid_a]
    ans = [e for e in a1 if e.get("kind") == "answered" and e.get("interaction_id") == iid_a]
    check(2, "a1 accepted, got `reserved`, auto-answered IE's agent leg", bool(reserved and auto and ans),
          f"reserved {len(reserved)}, auto_answer {len(auto)}, answered {len(ans)}")

    # ---- AC3: DEMO steps 1-6 ----------------------------------------------------------------------
    heard = media.get("heard", [])
    whats = [h["what"] for h in heard]
    check(3, "DEMO 2: the caller heard the consent notice, 'Welcome to Acme', 'Press 1 for sales'",
          whats[:3] == ["This call may be recorded. Press 9 to opt out.", "Welcome to Acme.", "Press 1 for sales."],
          " -> ".join(whats[:6]))
    i_music = next((i for i, w in enumerate(whats) if w == "hold music"), None)
    i_pos = next((i for i, w in enumerate(whats) if w.startswith("position")), None)
    i_agent = next((i for i, w in enumerate(whats) if w == "agent a1"), None)
    check(3, "DEMO 3: queued, the caller heard hold music and its position, then the agent",
          None not in (i_music, i_pos, i_agent) and i_music < i_agent and i_pos < i_agent,
          f"hold music at #{i_music}, position at #{i_pos}, agent a1 at #{i_agent}")
    a1_heard = (load(j("a1-heard.json"), {}) or {}).get("windows", [])
    w440 = [w for w in a1_heard if w.get("ratio_440", 0) > 10 and w.get("rms", 0) > 0.001]
    check(3, "DEMO 3: both sides hear each other (the caller hears a1's tone; a1 hears the caller's 440 Hz)",
          i_agent is not None and len(w440) >= 5, f"a1: 440 Hz in {len(w440)} of {len(a1_heard)} 1 s windows")
    k = media.get("kill", {})
    after_kill = [w for w in a1_heard if mt.get("F ie killed") and w.get("end_ms", 0) / 1000 > mt["F ie killed"] + 1
                  and w.get("ratio_440", 0) > 10]
    check(3, "DEMO 5: kill -9 of IE mid-call keeps the bridged audio (both ways)",
          k.get("packets_10s_after", 0) >= 400 and k.get("agent_tone_share_after", 0) >= 0.7 and len(after_kill) >= 3,
          f"to the caller: {k.get('packets_5s_before')} pkts in the 5 s before, {k.get('packets_10s_after')} in the 10 s after, "
          f"max gap {k.get('max_gap_ms_around')} ms, a1's tone {k.get('agent_tone_share_after')}; a1 heard 440 Hz in "
          f"{len(after_kill)} windows after the kill")
    rows_a = [r for r in cdr if r.get("interaction_id") == iid_a]
    rows_b = [r for r in cdr if r.get("interaction_id") == iid_b]
    ra = rows_a[0] if rows_a else {}
    check(3, "DEMO 5/6: the hangup after the kill still wrote call A's CDR (one row, from TC's end)",
          len(rows_a) == 1 and ra.get("closed_by") == "restart" and ra.get("ended_at") and ra.get("agent_id") == "agent-a1"
          and ra.get("queue_outcome") == "connected",
          json.dumps({k2: ra.get(k2) for k2 in ("closed_by", "queue_id", "queue_outcome", "offers", "agent_id", "wait_ms",
                                                 "recorded_legs", "recording_failures", "recording_pauses", "mutes",
                                                 "end_reason", "duration_ms")}))
    rb = rows_b[0] if rows_b else {}
    check(3, "one CDR per call; call B's says abandoned", len(rows_b) == 1 and rb.get("queue_outcome") == "abandoned"
          and len(cdr) == len({r.get("interaction_id") for r in cdr}),
          f"{len(cdr)} rows; B: {rb.get('queue_outcome')}, wait {rb.get('wait_ms')} ms")
    t_hang = mt.get(next((m for m in mt if m.startswith("G caller A hung up")), ""), None)
    wrap = frames(a1, "wrap_up", t_hang) if t_hang else []
    ready_again = [e for e in a1 if e.get("kind") == "frame" and e.get("type") == "agent_state" and e.get("state") == "ready"
                   and mt.get("G a1 wrap-up done") and e.get("t", 0) / 1000 >= mt["G a1 wrap-up done"]]
    check(3, "DEMO 6: after the hangup a1 went to wrap-up (CallEnded from the restarted IE) and is ready again",
          bool(wrap) and bool(ready_again), f"wrap_up {len(wrap)}, ready again {len(ready_again)}")
    moments = ("recording.paused", "recording.resumed", "agent.muted", "agent.unmuted", "recording.started",
               "recording.failed", "agent.connected", "queue.position_announced")
    events = [e.get("event") for e in of(iid_a) if e.get("event") in moments]
    dtmf = of(iid_a, "dtmf.action_reported")
    if recording:
        check(3, "the agent's DTMF pause shows as recording_paused (then resumed, muted, unmuted)",
              events.count("recording.paused") == 1 and events.count("recording.resumed") == 1
              and "agent.muted" in events and "agent.unmuted" in events,
              f"events: {events}; {len(dtmf)} dtmf actions")
    else:
        check(3, "the agent's DTMF codes reached parity-ms (recordings off: pause is NOOP until the null sink)",
              len(dtmf) == 4 and "agent.muted" in events and "agent.unmuted" in events, f"events: {events}")
    rec = [e for e in of(iid_a) if e.get("event") in ("recording.started", "recording.failed")]
    check(3, "recording per leg: the customer's at consent, the agent's at answer (null sink: nothing written)",
          (not recording) or [attr(e, "agent_leg") for e in rec if e.get("event") == "recording.started"] == [False, True],
          f"{[(e.get('event'), attr(e, 'agent_leg')) for e in rec]}")
    # The codes as values (a masked number's '***123' is not one).
    code_re = re.compile(r"(?<![*\w])\*[123](?!\d)")
    codes = sorted({m for src in (ie_raw, progress_raw, text(j("cdr.jsonl"))) for m in code_re.findall(src)})

    def flat(d, pre=""):
        for k, v in (d or {}).items():
            if isinstance(v, dict):
                yield from flat(v, pre + k + ".")
            else:
                yield pre + k, v
    digit_fields = [e for e in ie if any(re.search(r"digit|dtmf_seq", key) and val not in ("[redacted]", "", None)
                                         and not isinstance(val, (int, bool)) for key, val in flat(e))]
    check(3, "no digit anywhere in IE: logs, CDR, snapshots (the codes and the caller's 1)",
          not codes and not digit_fields, f"codes found: {codes}; digit fields: {len(digit_fields)}")

    # ---- AC4: call-path fixes in the run ------------------------------------------------------------
    # ---- AC4: IE-059, routing-engine restarted mid-call (a new leader_epoch) ---------------------
    after_re = text(j("ie-after-re-restart.txt"))
    if after_re:
        changed = [e for e in ie if e.get("event") == "routing.leader_changed"]
        reatt = of(iid_c, "routing.reattached")
        reenq = [e for e in of(iid_c, "flow.counted") if attr(e, "counter") == "routing_reenqueued"]
        rebased = of(iid_c, "routing.delivery_rebased")
        t_re = mt.get(next((m for m in mt if m.startswith("I re restarted")), ""), None)

        def at(e):  # a record's time, epoch seconds (its ts is RFC 3339 UTC, milliseconds)
            import datetime
            return datetime.datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=datetime.timezone.utc).timestamp()
        assigned_c = of(iid_c, "routing.assigned")
        before = [e for e in assigned_c if t_re and at(e) < mt.get("I re killed", t_re)]
        after = [e for e in assigned_c if t_re and at(e) > t_re]
        connected_after = [e for e in of(iid_c, "agent.connected") if t_re and at(e) > t_re]
        c_offer = [e for e in frames(a2, "offer", t_re) if e.get("interaction_id") == iid_c] if t_re else []
        rows_c = [r for r in cdr if r.get("interaction_id") == iid_c]
        rc = rows_c[0] if rows_c else {}
        check(4, "IE-059: RE restarted alone mid-call (leader_epoch rose, a fresh delivery numbering): IE stays up "
                 "(no exit, no restart); the queued caller C, which had a routing message before, re-attaches "
                 "(re-based, MF-1a), keeps its place and connects to a2 after the change",
              after_re.startswith("running 0 0") and len(changed) >= 1 and bool(reatt) and bool(before)
              and bool(after) and bool(connected_after) and bool(c_offer) and len(rows_c) == 1
              and rc.get("queue_outcome") == "connected",
              f"IE after RE's restart: {after_re}; leader changes {len(changed)} "
              f"({[attr(e, 'from_leader_epoch') for e in changed]} -> {[e.get('leader_epoch') for e in changed]}); "
              f"C: Assigned before {len(before)}, re-attached {len(reatt)} (entry {[attr(e, 'entry') for e in reatt]}), "
              f"re-based {len(rebased)} ({[attr(e, 'last_delivery_seq') for e in rebased]}), re-enqueued {len(reenq)}, "
              f"offered to a2 after {len(c_offer)}, Assigned after {len(after)}, connected after {len(connected_after)}; "
              f"C CDR rows {len(rows_c)} ({rc.get('queue_outcome')}, agent {rc.get('agent_id')})")
    log_check = text(j("ie-log-check.txt"))
    check(4, "IE's log is log-record.v1 with its catalogue (contract-check --logs: schema, xsid, masking)",
          log_check.endswith(" 0 violations"), log_check.splitlines()[-1] if log_check else "not run")
    unknown = [e for e in ie if e.get("level") in ("error", "fatal")]
    check(4, "no error in IE's log", not unknown, "; ".join(sorted({e.get("event", "") for e in unknown}))[:400])

    ok = all(c["pass"] for c in checks)
    with open(j("summary.json"), "w", encoding="utf-8") as f:
        json.dump({"pass": ok, "checks": checks, "marks": marks}, f, indent=1)
    with open(j("summary.md"), "w", encoding="utf-8") as f:
        f.write(f"# IE step 6 / DEMO level 1: {os.path.basename(d)}: {'PASS' if ok else 'FAIL'}\n\n")
        f.write(f"Call A {iid_a}, call B {iid_b}, call C {iid_c}; recording {'null sink' if recording else 'off'}.\n\n")
        f.write("| AC | Check | Result | Detail |\n|---|---|---|---|\n")
        for c in checks:
            f.write(f"| {c['ac']} | {c['check']} | {'PASS' if c['pass'] else 'FAIL'} | {c['detail'].replace('|', '/')} |\n")
        f.write("\n## Timeline\n\n")
        t0 = marks[0]["t"] if marks else 0
        for m in marks:
            f.write(f"- +{m['t'] - t0:6.1f} s {m['what']}\n")
        f.write("\n## What the caller heard (from parity-ms, in order)\n\n")
        for h in heard:
            f.write(f"- +{h['at_s']} s {h['what']} ({h['s']} s)\n")
    print(json.dumps({"pass": ok, "failed": [c["check"] for c in checks if not c["pass"]]}, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
