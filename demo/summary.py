#!/usr/bin/env python3
"""DEMO level 0 checks (IE step 3): reads a run directory written by demo/run.sh and prints a
Markdown table, each check with its result and where the evidence is.

    summary.py <run dir>
"""
import json
import os
import re
import sys

REPLICA = "ie-demo-1"


def lines(path):
    out = []
    if not os.path.exists(path):
        return out
    for raw in open(path, encoding="utf-8", errors="replace"):
        try:
            out.append(json.loads(raw))
        except ValueError:
            pass
    return out


def of(log, iid):
    return [l for l in log if l.get("interaction_id") == iid]


def has(log, message, **fields):
    return any(l.get("message") == message and all(str(l.get(k)) == str(v) for k, v in fields.items()) for l in log)


def read(path):
    try:
        return open(path, encoding="utf-8").read().strip()
    except OSError:
        return ""


def main(run):
    marks = json.load(open(os.path.join(run, "marks.json")))["calls"]
    media = json.load(open(os.path.join(run, "media.json")))["calls"]
    ie = lines(os.path.join(run, "ie.log"))
    restart = lines(os.path.join(run, "ie-restart.log"))
    rows = []

    def check(name, ok, evidence):
        rows.append((name, "PASS" if ok else "FAIL", evidence))

    # call 0: the tenant check
    c0 = of(ie, marks["call0"]["interaction_id"])
    sip0 = [s["line"] for s in media["call0"]["sip"]]
    check("call 0: X-Tenant-ID ≠ number map → refused before any claim",
          read(os.path.join(run, "call0", "exit")) == "0"
          and has(c0, "tenant_mismatch: X-Tenant-ID disagrees with the number map; refusing the call")
          and has(c0, "arrival declined", sip_status=403)
          and not has(c0, "claimed ie/owner in etcd")
          and any(" 403 " in s for s in sip0),
          "ie.log (tenant_mismatch, arrival declined 403, no claim); call0/sipp-messages.log; media.json call0.sip")

    # call 1: the IVR (step 4): consent, menu, the digit, sales, IE hangs up, one CDR
    c1 = of(ie, marks["call1"]["interaction_id"])
    m1 = media["call1"]
    sip1 = m1["sip"]
    check("call 1: answered through TC (200 OK to the carrier), IE's AnswerLeg done",
          read(os.path.join(run, "call1", "exit")) == "0" and has(c1, "command done", command="AnswerLeg")
          and any(s["dir"] == "fs->caller" and s["line"].startswith("SIP/2.0 200") for s in sip1),
          "call1/sipp-messages.log; ie.log")
    check("call 1: consent announced (500 Hz) and decided before the flow: granted, not opted out; recording only allowed after it (stub, MS-012)",
          m1["prompt_tone_sequence"][:1] == [500]
          and has(c1, "consent decided", decision="granted", basis="not_opted_out")
          and has(c1, "consent allows recording: not started until parity-ms encrypts (MS-012)"),
          f"media.json call1.prompt_tone_sequence {m1['prompt_tone_sequence']}; ie.log")
    check("call 1: \"Welcome to Acme\" (660 Hz) then \"Press 1 for sales\" (770 Hz) heard, played by parity-ms",
          m1["prompt_tone_sequence"][1:3] == [660, 770] and m1["caller_440_reaches_ms"],
          f"media.json call1: sequence {m1['prompt_tone_sequence']}, caller's 440 Hz reaches parity-ms {m1['caller_440_reaches_ms']}")
    check("call 1: gather received the digit (the caller's in-band DTMF 1, detected by parity-ms) and routed to sales (880 Hz heard)",
          m1["dtmf1_windows_from_caller"] > 0 and has(c1, "gather complete", to_step="sales", presses=1)
          and m1["prompt_tone_sequence"] == [500, 660, 770, 880],
          f"media.json call1: DTMF 1 in {m1['dtmf1_windows_from_caller']} 100 ms windows of the caller's stream; "
          f"sequence {m1['prompt_tone_sequence']}; ie.log 'gather complete' to sales, 1 press (no digit logged)")
    check("call 1: after the sales prompt IE hangs up: BYE to the caller; SessionEnded recorded and acked; owner key released",
          has(c1, "command done", command="EndSession")
          and any(s["dir"] == "fs->caller" and s["line"].startswith("BYE") for s in sip1)
          and has(c1, "session ended") and has(c1, "owner key released", released=True),
          "ie.log; media.json call1.sip")
    cdr_rows = [l.split(" | ") for l in read(os.path.join(run, "cdr.txt")).splitlines() if l.strip()]
    rows1 = [r for r in cdr_rows if r[0] == marks["call1"]["interaction_id"]]
    rows0 = [r for r in cdr_rows if r[0] == marks["call0"]["interaction_id"]]
    check("CDR: exactly one row for call 1 (acme, acme-main, last step sales, consent granted, 1 gather); none for the refused call 0",
          len(rows1) == 1 and rows1[0][1:8] == ["acme", "acme-main", "sales", "granted", "not_opted_out", "1", "0"]
          and rows1[0][9] == "ie" and not rows0 and has(ie, "CDR written", interaction_id=marks["call1"]["interaction_id"]),
          f"cdr.txt: {rows1}")
    digits_logged = [l for l in ie if any(k in l for k in ("digit", "digits", "dtmf"))
                     and not all(str(l.get(k)) == "[redacted]" for k in ("digit", "digits", "dtmf") if k in l)]
    check("No digit in IE's logs (gathered digits are PII: only the route and a press count)",
          not digits_logged, f"ie.log: {len(digits_logged)} lines with a digit field")

    # call 2: kill -9 mid-call
    m2 = media["call2"]
    k = m2.get("kill", {})
    after = read(os.path.join(run, "call2", "tc-session-after-kill.json"))
    try:
        s_after = json.loads(after)
    except ValueError:
        s_after = {}
    customer = [l for l in s_after.get("legs", []) if l.get("role") == "Customer"]
    check("call 2: kill -9 of IE mid-prompt; parity-ms frames keep reaching the caller",
          k.get("packets_3s_after", 0) >= 140 and k.get("prompt_660_coverage_kill_to_kill+20s", 0) >= 0.9
          and k.get("longest_gap_after_kill_ms", 1e9) < 100,
          f"media.json call2.kill: {json.dumps(k)}")
    check("call 2: the call stays up (no BYE from freeswitch); the caller hangs up at the end",
          read(os.path.join(run, "call2", "exit")) == "0"
          and not any(s["dir"] == "fs->caller" and s["line"].startswith("BYE") for s in m2["sip"])
          and any(s["dir"] == "caller->fs" and s["line"].startswith("BYE") for s in m2["sip"]),
          "call2/sipp-messages.log; media.json call2.sip")
    check("call 2: TC still holds the session after the kill: ACTIVE, customer leg ANSWERED, owner = the dead replica",
          s_after.get("state") == "Active" and s_after.get("owner_replica_id") == REPLICA
          and bool(customer) and customer[0].get("state") == "Answered",
          f"call2/tc-session-after-kill.json: {after}")
    ended = read(os.path.join(run, "call2", "tc-session-after-hangup.json"))
    check("call 2: after the caller's BYE, TC keeps the ENDED tombstone (unacked: no owner)",
          '"state":"Ended"' in ended, f"call2/tc-session-after-hangup.json: {ended}")
    check("restart: the new IE process is told of the session it doesn't hold (an orphan, adopted in step 8)",
          any(l.get("message", "").startswith("events for a session this replica doesn't hold")
              and l.get("interaction_id") == marks["call2"]["interaction_id"] for l in restart),
          "ie-restart.log")

    # no SDP in IE
    nosdp = read(os.path.join(run, "no-sdp.txt")).splitlines()
    hits = [l for l in nosdp if not l.startswith("##")]
    # Comments, and ie-obs's masking list (a log field named `sdp` would be redacted), are not SDP handling.
    code = [l for l in hits if not re.match(r"^(?:[^:]*:)?\d+:\s*//", l) and "ie-obs/src/pii.rs" not in l]
    check("IE never handled an SDP: no SDP type, field or code in IE or in telephony.proto",
          not code, f"no-sdp.txt: {len(hits)} lines: comments saying there is none, and ie-obs's log-masking "
          f"list; code lines: {len(code)}")

    print(f"# DEMO levels 0 and 1 (part 1) through TC: {os.path.basename(os.path.abspath(run))}\n")
    print("| Check | Result | Evidence |\n|---|---|---|")
    for n, r, e in rows:
        print(f"| {n} | {r} | {e} |")
    failed = sum(1 for _, r, _ in rows if r != "PASS")
    print(f"\n{len(rows) - failed}/{len(rows)} checks pass.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main(sys.argv[1])
