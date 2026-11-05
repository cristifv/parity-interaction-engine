#!/usr/bin/env python3
"""DEMO level 0 evidence (IE step 3) from the run's loopback capture.

    analyse.py <run dir>     -> <run dir>/media.json, and a summary on stdout

Reads <run>/lo.pcap and <run>/marks.json (what run.sh wrote: each call's SIPp media port, the
kill time). For each call: the RTP flow parity-ms -> caller (PCMU decoded, the dominant tone per
200 ms with the spike's Goertzel helpers in ../spike/audio.py), the caller -> parity-ms flow, and
the SIP requests seen. For the killed call: packets per second before and after the kill, and the
660 Hz prompt's coverage after it.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spike"))
from audio import goertzel, read_pcap, ulaw_decode  # noqa: E402

# The caller sends 440 Hz; the demo's prompts are 500 (consent), 660 (welcome, the hold prompt),
# 770 (menu), 880 (sales), 990 (goodbye).
FREQS = (440, 500, 660, 770, 880, 990)
FLOOR = 200.0
SIP = {5060, 5080}


def rtp(payload):
    if len(payload) < 12 or payload[0] >> 6 != 2:
        return None
    return payload[1] & 0x7F, payload[12:]


def windows(pkts):
    """[(t, pcmu bytes)] -> [(from, to, tone or None)] per 10 packets (200 ms)."""
    out = []
    for i in range(0, len(pkts), 10):
        chunk = pkts[i:i + 10]
        samples = [ulaw_decode(b) for _, p in chunk for b in p]
        e = {f: goertzel(samples, f) for f in FREQS}
        best = max(e, key=e.get)
        out.append((chunk[0][0], chunk[-1][0] + 0.02, best if e[best] > FLOOR else None))
    return out


def coverage(wins, tone, start, end):
    if end <= start:
        return 0.0
    hit = sum(max(0.0, min(b, end) - max(a, start)) for a, b, t in wins if t == tone)
    return round(min(hit / (end - start), 1.0), 3)


def main(run):
    marks = json.load(open(os.path.join(run, "marks.json")))
    flows = {}  # (src port, dst port) -> [(ts, pcmu payload)]
    events = {}  # (src port, dst port) -> RFC 4733 packets (PT 101)
    sip = []
    for ts, src, sp, dst, dp, pl in read_pcap(os.path.join(run, "lo.pcap")):
        if sp in SIP and dp in SIP:
            line = pl.split(b"\r\n", 1)[0].decode("latin-1")
            sip.append((ts, sp, dp, line))
            continue
        r = rtp(pl)
        if r and r[0] == 0:
            flows.setdefault((sp, dp), []).append((ts, r[1]))
        elif r and r[0] == 101:
            events[(sp, dp)] = events.get((sp, dp), 0) + 1
    report = {"calls": {}}
    for name, call in marks["calls"].items():
        port = call["media_port"]
        down = max((v for (s, d), v in flows.items() if d == port), key=len, default=[])
        up = max((v for (s, d), v in flows.items() if s == port), key=len, default=[])
        wins = windows(down)
        seq = []
        for _, _, t in wins:
            if t and t != 440 and (not seq or seq[-1] != t):
                seq.append(t)
        # The caller's in-band DTMF "1" (697 + 1209 Hz together) in its stream to parity-ms.
        dtmf1 = 0
        for i in range(0, len(up), 5):
            samples = [ulaw_decode(b) for _, p in up[i:i + 5] for b in p]
            if goertzel(samples, 697) > FLOOR and goertzel(samples, 1209) > FLOOR:
                dtmf1 += 1
        c = {
            "prompt_tone_sequence": seq,
            "dtmf1_windows_from_caller": dtmf1,
            "rfc4733_packets_from_caller": sum(n for (sp, dp), n in events.items() if sp == port),
            "ms_to_caller_packets": len(down),
            "caller_to_ms_packets": len(up),
            "ms_to_caller_span_s": round(down[-1][0] - down[0][0], 2) if down else 0,
            "tones_to_caller": sorted({t for _, _, t in wins if t}),
            "prompt_660_coverage_first_4s": coverage(wins, 660, down[0][0], down[0][0] + 4) if down else 0,
            "caller_440_reaches_ms": bool(up) and coverage(windows(up), 440, up[0][0], up[-1][0]) > 0.9,
        }
        kill = call.get("kill_epoch_s")
        if kill and down:
            t = [p[0] for p in down]
            c["kill"] = {
                "packets_3s_before": sum(1 for x in t if kill - 3 <= x < kill),
                "packets_3s_after": sum(1 for x in t if kill < x <= kill + 3),
                "packets_after_kill": sum(1 for x in t if x > kill),
                "last_packet_s_after_kill": round(t[-1] - kill, 2),
                "prompt_660_coverage_kill_to_kill+20s": coverage(wins, 660, kill, kill + 20),
                "longest_gap_after_kill_ms": round(1000 * max(
                    (b - a for a, b in zip(t, t[1:]) if a > kill), default=0), 1),
            }
        # SIP request and status lines, by the call's time window (both calls use SIPp's port 5060).
        a, b = call["start_epoch_s"], call["end_epoch_s"]
        c["sip"] = [
            {"t_s": round(ts - a, 2), "dir": "caller->fs" if dp == 5080 else "fs->caller", "line": line}
            for ts, sp, dp, line in sip if a <= ts <= b
        ]
        report["calls"][name] = c
    json.dump(report, open(os.path.join(run, "media.json"), "w"), indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
