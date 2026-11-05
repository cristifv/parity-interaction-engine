#!/usr/bin/env python3
"""DEMO level 1's audio (IE step 6): the prompts as tones, what SIPp and the browser send, and the
analysis of the loopback capture. Standard library only; G.711 u-law for the call path.

  media.py gen <work-dir>
      <work>/prompts/acme/<id>.wav  the flow's prompts as tones (parity-ms reads
                                    <prompt-dir>/<tenant>/<prompt_id>.wav):
                                    consent 500 Hz, welcome 660, "press 1" 770, "please hold" 880,
                                    goodbye 990 (IE's `say` ids), hold-music 1100 (3 s, looped),
                                    queue-position-1..9 1250..1650 (50 Hz apart), -more 1700
      <work>/media/caller.ulaw      what the caller sends: 440 Hz
      <work>/media/dtmf-1.ulaw      the caller's in-band DTMF 1 (697 + 1209 Hz)
      <work>/in/agent-<a>.wav       each browser's fake microphone (PCM 48 kHz): a1 1900 Hz, a2 2100 Hz
  media.py analyse <pcap> <caller-rtp-port> <kill-epoch-s or 0>
      What the caller heard from parity-ms, in order (runs of one tone), and around the IE kill:
      packets per second and the agent's tone before and after. JSON on stdout.
"""
import hashlib
import json
import math
import os
import struct
import sys

BIAS, CLIP = 0x84, 32635
SAYS = [("This call may be recorded. Press 9 to opt out.", 500, 2.0),
        ("Welcome to Acme.", 660, 1.5),
        ("Press 1 for sales.", 770, 1.5),
        ("Thank you. Please hold for an agent.", 880, 1.5),
        ("Goodbye.", 990, 1.0)]
HOLD_HZ = 1100
POSITION_HZ = {n: 1200 + 50 * n for n in range(1, 10)}
MORE_HZ = 1700
AGENT_HZ = {"a1": 1900, "a2": 2100}
CALLER_HZ = 440
TONES = [500, 660, 770, 880, 990, HOLD_HZ, *POSITION_HZ.values(), MORE_HZ, *AGENT_HZ.values()]


def say_prompt_id(text, language="en-GB", voice=""):
    """IE's ie-core prompt::say_prompt_id."""
    return "say-" + hashlib.sha256(f"{language}\n{voice}\n{text}".encode()).hexdigest()[:24]


def ulaw_encode(sample):
    sign = 0x80 if sample < 0 else 0
    sample = min(abs(sample), CLIP) + BIAS
    exponent, mask = 7, 0x4000
    while exponent > 0 and not (sample & mask):
        exponent -= 1
        mask >>= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return ~(sign | (exponent << 4) | mantissa) & 0xFF


def ulaw_decode(byte):
    byte = ~byte & 0xFF
    sign, exponent, mantissa = byte & 0x80, (byte >> 4) & 0x07, byte & 0x0F
    sample = (((mantissa << 3) + BIAS) << exponent) - BIAS
    return -sample if sign else sample


def tone(freq, seconds, rate=8000, amp=8000):
    return [int(amp * math.sin(2 * math.pi * freq * n / rate)) for n in range(int(seconds * rate))]


def write_ulaw_wav(path, samples, rate=8000):
    data = bytes(ulaw_encode(s) for s in samples)
    fmt = struct.pack("<HHIIHH", 7, 1, rate, rate, 1, 8) + struct.pack("<H", 0)
    body = (b"WAVE" + b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"fact" + struct.pack("<I", 4)
            + struct.pack("<I", len(samples)) + b"data" + struct.pack("<I", len(data)) + data)
    with open(path, "wb") as f:
        f.write(b"RIFF" + struct.pack("<I", len(body)) + body)


def write_pcm_wav(path, freq, seconds=30, rate=48000, amp=6000):
    data = b"".join(struct.pack("<h", int(amp * math.sin(2 * math.pi * freq * n / rate)))
                    for n in range(int(seconds * rate)))
    fmt = struct.pack("<HHIIHH", 1, 1, rate, rate * 2, 2, 16)
    body = b"WAVE" + b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", len(data)) + data
    with open(path, "wb") as f:
        f.write(b"RIFF" + struct.pack("<I", len(body)) + body)


def gen(work):
    prompts = os.path.join(work, "prompts", "acme")
    for d in (prompts, os.path.join(work, "media"), os.path.join(work, "in")):
        os.makedirs(d, exist_ok=True)
    for text, hz, secs in SAYS:
        write_ulaw_wav(os.path.join(prompts, say_prompt_id(text) + ".wav"), tone(hz, secs))
    write_ulaw_wav(os.path.join(prompts, "hold-music.wav"), tone(HOLD_HZ, 3.0))
    for n, hz in POSITION_HZ.items():
        write_ulaw_wav(os.path.join(prompts, f"queue-position-{n}.wav"), tone(hz, 1.0))
    write_ulaw_wav(os.path.join(prompts, "queue-position-more.wav"), tone(MORE_HZ, 1.0))
    with open(os.path.join(work, "media", "caller.ulaw"), "wb") as f:
        f.write(bytes(ulaw_encode(s) for s in tone(CALLER_HZ, 4.0)))
    r = 8000
    dtmf = ([0] * 800 + [int(5000 * (math.sin(2 * math.pi * 697 * n / r) + math.sin(2 * math.pi * 1209 * n / r)))
                         for n in range(2000)] + [0] * 2000)
    with open(os.path.join(work, "media", "dtmf-1.ulaw"), "wb") as f:
        f.write(bytes(ulaw_encode(s) for s in dtmf))
    for a, hz in AGENT_HZ.items():
        write_pcm_wav(os.path.join(work, "in", f"agent-{a}.wav"), hz)


def goertzel(samples, freq, rate=8000):
    k = 2 * math.cos(2 * math.pi * freq / rate)
    s1 = s2 = 0.0
    for x in samples:
        s1, s2 = x + k * s1 - s2, s1
    return math.sqrt(max(s1 * s1 + s2 * s2 - k * s1 * s2, 0)) / max(len(samples), 1)


def dominant(samples, freqs):
    if not samples:
        return None
    e = {f: goertzel(samples, f) for f in freqs}
    best = max(e, key=e.get)
    return best if e[best] > 150 else None


def udp_packets(path):
    """(ts, src_port, dst_port, payload) for IPv4/UDP (DLT_EN10MB, DLT_NULL, RAW or SLL)."""
    with open(path, "rb") as f:
        head = f.read(24)
        if len(head) < 24:
            return
        magic = struct.unpack("<I", head[:4])[0]
        endian = "<" if magic in (0xA1B2C3D4, 0xA1B23C4D) else ">"
        nano = magic in (0xA1B23C4D, 0x4D3CB2A1)
        linktype = struct.unpack(endian + "I", head[20:24])[0]
        while True:
            rec = f.read(16)
            if len(rec) < 16:
                return
            sec, frac, incl, _ = struct.unpack(endian + "IIII", rec)
            frame = f.read(incl)
            off = {1: 14, 0: 4, 101: 0, 113: 16}.get(linktype, 14)
            if linktype == 1 and frame[12:14] != b"\x08\x00":
                continue
            ip = frame[off:]
            if len(ip) < 28 or ip[0] >> 4 != 4 or ip[9] != 17:
                continue
            ihl = (ip[0] & 0x0F) * 4
            sport, dport = struct.unpack(">HH", ip[ihl:ihl + 4])
            yield sec + frac / (1e9 if nano else 1e6), sport, dport, ip[ihl + 8:]


def to_caller(pcap, port):
    out = []
    for ts, sport, dport, payload in udp_packets(pcap):
        if dport == port and 16384 <= sport <= 32767 and len(payload) >= 12 and payload[0] >> 6 == 2:
            out.append((ts, payload[1] & 0x7F, payload[12:]))
    return out


def windows(pkts, size=5):
    pcmu = [p for p in pkts if p[1] == 0]
    for i in range(0, len(pcmu) - size + 1, size):
        yield pcmu[i][0], [ulaw_decode(b) for _, _, body in pcmu[i:i + size] for b in body]


def name(hz):
    for text, h, _ in SAYS:
        if h == hz:
            return text
    if hz == HOLD_HZ:
        return "hold music"
    if hz == MORE_HZ:
        return "position: more"
    for n, h in POSITION_HZ.items():
        if h == hz:
            return f"position {n}"
    for a, h in AGENT_HZ.items():
        if h == hz:
            return f"agent {a}"
    return str(hz)


def analyse(pcap, port, kill_s):
    pkts = to_caller(pcap, port)
    if not pkts:
        return {"rtp_to_caller": 0}
    t0 = pkts[0][0]
    runs = []
    for ts, samples in windows(pkts):
        t = dominant(samples, TONES)
        if runs and runs[-1][0] == t:
            runs[-1][2] += 1
        else:
            runs.append([t, ts, 1])
    heard = [{"hz": t, "what": name(t), "at_s": round(ts - t0, 1), "s": round(n / 10, 1)}
             for t, ts, n in runs if t is not None and n >= 3]
    res = {"rtp_to_caller": len(pkts), "seconds": round(pkts[-1][0] - t0, 1),
           "payload_types": sorted({p[1] for p in pkts}), "heard": heard}
    if kill_s:
        before = [p for p in pkts if kill_s - 5 <= p[0] < kill_s]
        after = [p for p in pkts if kill_s <= p[0] < kill_s + 10]
        gaps = [b[0] - a[0] for a, b in zip(pkts, pkts[1:]) if kill_s - 1 <= a[0] <= kill_s + 10]

        def agent_share(ps):
            ws = [dominant(s, TONES) for _, s in windows(ps)]
            return round(sum(1 for w in ws if w == AGENT_HZ["a1"]) / max(len(ws), 1), 2)

        res["kill"] = {"packets_5s_before": len(before), "packets_10s_after": len(after),
                       "max_gap_ms_around": round(max(gaps) * 1000, 1) if gaps else None,
                       "agent_tone_share_before": agent_share(before),
                       "agent_tone_share_after": agent_share(after)}
    return res


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "gen":
        gen(sys.argv[2])
    elif len(sys.argv) == 5 and sys.argv[1] == "analyse":
        json.dump(analyse(sys.argv[2], int(sys.argv[3]), float(sys.argv[4])), sys.stdout, indent=1)
        sys.stdout.write("\n")
    else:
        sys.exit(__doc__)
