# DEMO level 1, end to end (IE step 6; ie-integration-1)

One automated run of DEMO.md steps 1-6 on parity-deploy's compose stack: SIPp is the carrier, two
headless browsers (Playwright) are the agents on the gateway's own agent page, and the real IE,
routing engine, TC, parity-ms, freeswitch, Postgres, etcd and gateway are under test. About 5
minutes (278 s on 2026-11-02). Evidence: `docs/evidence/step6/run-<time>/summary.md` first.

```
tests/e2e/l1/build-images.sh ie gateway re-build tc ms   # or `re` / `re=<image>` to tag an RE image
L1_TC_IMAGE=localhost/parity-telephony-controller:ie6 L1_MS_IMAGE=localhost/parity-ms:ie6 \
L1_TC_EXTRA=--parity.telephony-controller.node.recording-sink=null L1_MS_EXTRA=--recording-sink=null \
L1_RE_IMAGE=localhost/parity-cc-routing:ie6 tests/e2e/l1/run.sh [evidence dir]
```

`run.sh` takes and releases the shared e2e lock (`.e2e.lock.d`; exit 75 when it is busy), runs its
own compose project (`ie6-l1`), tears down to 0 containers and runs parity-deploy's
`check-no-secrets.sh` on the evidence. Per-run dev secrets (agent SIP passwords, tokens) live in
`secrets/local-dev/ie6-l1/` (700/600) and are deleted at teardown.

## The pieces (reusable one by one)

| File | What it is | Inputs and outputs |
|---|---|---|
| `run.sh` | The scenario, phases A-I (its header lists them and what each proves) | Images by tag (`L1_*_IMAGE`, recorded by id in `inputs.txt`); `L1_RECORDING` (`null_sink`/`off`), `L1_TC_EXTRA`, `L1_MS_EXTRA`, `L1_SKIP_RE_RESTART=1`, `L1_WORK_ROOT` |
| `compose.l1.yml` | The override of parity-deploy's `compose/compose.yml`, only at the README's sanctioned points: images, TC and parity-ms commands, `.run` mounts re-pointed (freeswitch: only `json_cdr/`, PL-083), the freeswitch directory overlay, the gateway's per-run agent-password file and IE's `ie.toml` (PL-084). `docker compose -f compose.yml -f compose.l1.yml config` must render | `L1_WORK`, `L1_SECRETS` |
| `sipp/caller.xml.in` | One carrier call: the headers opensips would stamp (`-key iid/tenant/bin`), PCMU only, in-band DTMF "1" after the IVR, a 440 Hz tone, BYE after `@STAY_MS@`; ACK and BYE carry `[routes]` (PL-085) | `run.sh caller <name> <stay ms> <iid>`; **always pass `-cid_str` with a run stamp** (SIPp in a container repeats Call-IDs: IE-072) |
| `agents.mjs` | Two browser agents on the gateway's page, driven over HTTP on loopback (its header lists the API: login, ready, offer, accept, reload, answered, capture, heard, dtmf, wrap, log) | Tokens come in the login body only; writes `page-<a>.jsonl`, `driver.jsonl` |
| `tokens.mjs` | Dev JWTs for the agents and the gateway's JWKS from the local-dev key | Tokens 600 in the secrets dir |
| `media.py` | `gen`: the flow's prompts as distinct tones, the caller's and the agents' audio; `analyse`: what the caller heard (in order) and the RTP around the IE kill, from the loopback pcap | `media.json` |
| `analyse.py` | Every check (AC1-AC4) over one evidence dir; writes `summary.md`/`summary.json`, exit 0 when all pass | Reads IE's log as log-record.v1 (`event`, `xsid`, `attrs`), `ie-log-check.txt` (contract-check `--logs` over IE's whole log), CDR rows, the pages' frames, marks |
| `flows.json`, `ie.toml.in` | The demo flow (consent, menu, `dial` with music and positions) and IE's config (P2: the `parity_ie` role, `migrate = false`, recording mode filled in) | |
| `build-images.sh` | The run's own image tags (`:ie6`) from the trees: IE (host release build on IE's base image), gateway, RE (`re-build`: its `bazel-bin`), TC (its jar), parity-ms (its Dockerfile) | Prints each image id |

## What a run proves (run-20261102-200148: PASS, 278 s, RE phase 8 with HA off)

AC1 routing through parity-cc-routing (one Enqueue, longest-ready first, the ignored offer moves
after 15 s with its enqueue time, an abandoned caller's `Dequeue(ABANDONED)`); AC2 the gateway (SIP
over WS, the offer from the re-attach snapshot, the auto-answered agent leg); AC3 DEMO 2-6 (prompts
in order, hold music and the position, both directions of audio, `kill -9` of IE mid-call keeps the
audio and the restarted IE writes the one CDR, wrap-up, DTMF pause/resume/mute/unmute as events, the
recording per leg on the null sink, no digit anywhere); AC4 IE-059 (RE killed and restarted alone
mid-call: IE stays up, the queued caller re-attaches and keeps its place), IE's log is
log-record.v1 with its catalogue, no error in IE's log.

## Gotchas

- The interaction ids are UUIDv7 (`uuid.uuid7()`, Python 3.14) so that IE logs them as `xsid`.
- A routing engine without `re.state` (phase 1) forgets its agents on a restart: the gateway closes
  the desks with 4009 (GW-037), so phase I logs a2 in again before it turns ready.
- freeswitch's `/var/log/freeswitch` is a tmpfs (its expanded configuration holds the ESL password):
  `freeswitch.log` is copied out with `compose exec cat`, lines naming a password dropped.
- Host load (another agent's build or trainer) stretches the offer timings; rerun once before
  concluding on a timing failure.
