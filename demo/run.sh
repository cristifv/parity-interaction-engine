#!/usr/bin/env bash
# DEMO level 0 and level 1 part 1 through TC (IE PLAN steps 3 and 4; PROGRESS.md): real calls,
# carrier (SIPp) -> freeswitch + TC -> IE, the IVR's prompts played by parity-ms, CDRs in Postgres.
# The number map is parity-contracts' shared test tenants; the flows are demo/flows.json. Three calls:
#   call 0  X-Tenant-ID globex on acme's number: IE declines, TC answers 403;
#   call 1  acme-main: the consent announcement (500 Hz), "Welcome to Acme" (660), "Press 1 for
#           sales" (770); the caller presses 1 (RFC 4733); the sales prompt (880); IE hangs up;
#           one CDR row (consent granted, last step sales);
#   call 2  acme-hold: the 30 s prompt; kill -9 of IE 6 s in; the caller keeps hearing it; TC's
#           GetSession shows the session live with its owner gone; the caller hangs up at 36 s;
#           a restarted IE is told about the session it doesn't hold (an orphan).
# Then demo/analyse.py reads the loopback capture. Evidence: <out>/ (summary.md first).
#
# TC's real-node pieces are reused, not copied into this repository: freeswitch's image digest
# and etcd's come from TC's test sources; TC's freeswitch/conf and its compose/freeswitch-test
# overlay are mounted read-only from a snapshot taken at start (TC may be edited meanwhile);
# TC's jar is copied at start. Nothing in TC or parity-ms is changed.
#
#   PARITY_MS_BIN=<media-server built from parity-ms's requirement-41 source> demo/run.sh [out dir]
#   TC_JAR (default: TC's target jar), TC_DIR (default: ../parity-telephony-controller)
#   IE_BIN (default: target/release/parity-interaction-engine), TMPDIR for the work directory
# The Postgres password is a local-dev secret: ../secrets/local-dev/ie-cdr-postgres.password (made
# if missing; dir 700, file 600; never printed). Postgres listens on a random loopback port.
# Needs: docker (images: TC's freeswitch and etcd digests, ie-spike-tools:fc43, postgres), Java 21,
# python3, curl. Host network, loopback only. Waits while a TC build runs (they share ports).
set -euo pipefail
IE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TC=${TC_DIR:-$IE/../parity-telephony-controller}
MS_BIN=${PARITY_MS_BIN:?set PARITY_MS_BIN to a media-server built from parity-ms requirement 41}
TC_JAR=${TC_JAR:-$TC/target/parity-telephony-controller-0.1.0.jar}
IE_BIN=${IE_BIN:-$IE/target/release/parity-interaction-engine}
OUT=${1:-$IE/docs/evidence/step3/run-$(date +%Y%m%d-%H%M%S)}
TOOLS_IMAGE=ie-spike-tools:fc43
PG_IMAGE=postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24  # 17-alpine
SECRETS=${IE_DEMO_SECRETS:-$IE/../secrets/local-dev}
MAPS=${IE_DEMO_MAPS:-$IE/../parity-contracts/tenants/test-tenants.v1.json}
LABEL=parity.test=parity-interaction-engine-demo
ME="$(id -u):$(id -g)"
# Ports (loopback): SIPp 5060 and freeswitch's carrier 5080, ESL 8021 (TC's configuration);
# etcd 23790; parity-ms control 22231, media 24000-24999; TC 19700 (gRPC) and 19701; IE 19600.
ETCD=http://127.0.0.1:23790
MS_PORT=22231
TC_GRPC=19700
TC_MGMT=19701
IE_MGMT=19600
TENANT=acme
BIN=bin-a
REPLICA=ie-demo-1
die() { echo "demo: $*" >&2; exit 3; }
now() { date +%s.%N; }
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$OUT/run.log"; }

FS_IMAGE=$(grep -oh 'docker.io/safarov/freeswitch:[^"]*' "$TC/src/test/java/parity/telephonycontroller/realnode/Freeswitch.java" | head -1)
ETCD_IMAGE=$(grep -oh 'gcr.io/etcd-development/etcd@sha256:[0-9a-f]*' "$TC/src/test/java/parity/telephonycontroller/scripts/ServerTarget.java" | head -1)
[[ -n "$FS_IMAGE" && -n "$ETCD_IMAGE" ]] || die "can't read the freeswitch and etcd images from TC's test sources"
for f in "$MS_BIN" "$TC_JAR" "$IE_BIN" "$IE/target/debug/examples/tc_session"; do
  [[ -x "$f" || -f "$f" ]] || die "missing $f (cargo build --release -p interaction-engine; cargo build -p ie-telephony-client --example tc_session)"
done

# A TC build runs freeswitch on the same host ports: wait for it (the user's coordination rule).
for i in $(seq 120); do
  # Java processes only: a plain pattern also matches any shell whose command line names it.
  if ! pgrep -f '^[^ ]*java .*(plexus-classworlds|surefirebooter|failsafe)' >/dev/null \
      && ! docker ps --format '{{.Names}}' | grep -qE 'tc-test-freeswitch|gw-e2e'; then
    break
  fi
  [[ $i == 120 ]] && die "a TC build is still running after 20 minutes"
  echo "demo: a TC build or the gateway's e2e run is using the node's ports; waiting"; sleep 10
done
for p in 5060 5080 8021 19600 19700 19701 22231 23790; do
  ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || die "port $p is in use"
done

mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)  # absolute: docker mounts need it
WORK=$(mktemp -d "${TMPDIR:-/tmp}/ie-demo.XXXXXX")
PIDS=()
cleanup() {
  set +e
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
  docker rm -f ie-demo-freeswitch ie-demo-etcd ie-demo-postgres ie-demo-pcap ie-demo-sipp0 ie-demo-sipp1 ie-demo-sipp2 >/dev/null 2>&1
  [[ -f "$WORK/fs-out/log/freeswitch.log" ]] && cp "$WORK/fs-out/log/freeswitch.log" "$OUT/freeswitch.log"
  rm -rf "$WORK"
}
trap cleanup EXIT

log "inputs"
cp -a "$TC/freeswitch/conf" "$WORK/fs-conf"
cp -a "$TC/compose/freeswitch-test" "$WORK/fs-test"
cp "$TC_JAR" "$WORK/tc.jar"
{
  echo "freeswitch image: $FS_IMAGE"; echo "etcd image: $ETCD_IMAGE"; echo "tools image: $TOOLS_IMAGE $(docker image inspect -f '{{.Id}}' $TOOLS_IMAGE)"
  sha256sum "$MS_BIN" "$WORK/tc.jar" "$IE_BIN"
  (cd "$WORK" && find fs-conf fs-test -type f | sort | xargs sha256sum)
} >"$OUT/inputs.txt"

log "etcd"
docker run -d --name ie-demo-etcd --label "$LABEL" --network host --security-opt label=disable --cap-drop ALL \
  --user "$ME" --tmpfs /tmp:mode=1777 "$ETCD_IMAGE" etcd --name demo --data-dir /tmp/etcd \
  --listen-client-urls "$ETCD" --advertise-client-urls "$ETCD" \
  --listen-peer-urls http://127.0.0.1:23800 --initial-advertise-peer-urls http://127.0.0.1:23800 \
  --initial-cluster demo=http://127.0.0.1:23800 >/dev/null
for _ in $(seq 100); do curl -sf "$ETCD/health" >/dev/null && break; sleep 0.2; done
curl -sf "$ETCD/health" >/dev/null || die "etcd never became healthy"

log "freeswitch (TC's configuration and test overlay, read-only)"
mkdir -p "$WORK/fs-out/json_cdr" "$WORK/fs-out/log" && chmod -R 777 "$WORK/fs-out"
docker run -d --name ie-demo-freeswitch --label "$LABEL" --network host --security-opt label=disable --cap-drop ALL \
  --user "$ME" --tmpfs /tmp:mode=1777 \
  -v "$WORK/fs-conf:/etc/freeswitch:ro" -v "$WORK/fs-test/vars.xml:/etc/freeswitch/vars.xml:ro" \
  -v "$WORK/fs-test/directory:/etc/freeswitch/directory:ro" -v "$WORK/fs-out:/tc-out" \
  --entrypoint /usr/bin/freeswitch "$FS_IMAGE" -nonat -nf -nc -np -conf /etc/freeswitch -log /tc-out/log \
  -db /tmp/db -run /tmp/run -temp /tmp -cache /tmp/cache -storage /tmp/storage -recordings /tmp/rec -sounds /tmp/sounds >/dev/null

log "Postgres (the CDR; a random loopback port)"
mkdir -p "$SECRETS" && chmod 700 "$SECRETS"
PG_PASSWORD_FILE="$SECRETS/ie-cdr-postgres.password"
if [[ ! -s "$PG_PASSWORD_FILE" ]]; then
  (umask 077; python3 -I -c 'import secrets; print(secrets.token_urlsafe(24), end="")' >"$PG_PASSWORD_FILE")
fi
chmod 600 "$PG_PASSWORD_FILE"
docker run -d --name ie-demo-postgres --label "$LABEL" --security-opt label=disable \
  -e POSTGRES_USER=parity_ie -e POSTGRES_DB=parity_interaction_engine -e POSTGRES_PASSWORD_FILE=/run/secrets/pg \
  -v "$PG_PASSWORD_FILE:/run/secrets/pg:ro" -p 127.0.0.1::5432 "$PG_IMAGE" >/dev/null
PG_PORT=$(docker port ie-demo-postgres 5432/tcp | head -1 | sed 's/.*://')
for _ in $(seq 120); do docker exec ie-demo-postgres pg_isready -q -U parity_ie -d parity_interaction_engine && break; sleep 0.5; done
docker exec ie-demo-postgres pg_isready -q -U parity_ie -d parity_interaction_engine || die "Postgres never became ready"
sleep 1  # the image restarts the server once after initdb
for _ in $(seq 60); do docker exec ie-demo-postgres pg_isready -q -U parity_ie -d parity_interaction_engine && break; sleep 0.5; done

log "parity-ms and the prompts (say texts rendered as tones under their say-<sha256> ids)"
mkdir -p "$WORK/ms/rec" "$WORK/ms/prompts/$TENANT" "$WORK/ms/health"
python3 -I -c '
import hashlib, sys; sys.path.insert(0, sys.argv[1])
from audio import tone, write_ulaw_wav
d = sys.argv[2]
def say(text, hz, s, lang="en-GB", voice=""):
    pid = "say-" + hashlib.sha256((lang + "\n" + voice + "\n" + text).encode()).hexdigest()[:24]
    write_ulaw_wav(d + "/" + pid + ".wav", tone(hz, s))
say("This call may be recorded. Press 9 to opt out.", 500, 2.0)
say("Welcome to Acme.", 660, 2.0)
say("Press 1 for sales.", 770, 2.0)
say("Thank you. Sales will be with you soon.", 880, 2.0)
say("Goodbye.", 990, 1.0)
write_ulaw_wav(d + "/long-welcome.wav", tone(660, 30.0))
' "$IE/spike" "$WORK/ms/prompts/$TENANT"
ls "$WORK/ms/prompts/$TENANT" >"$OUT/prompts.txt"
# The caller's in-band DTMF "1": 100 ms silence, 250 ms of 697 + 1209 Hz, 250 ms silence (mu-law).
mkdir -p "$WORK/caller" && chmod 755 "$WORK/caller"
python3 -I -c '
import math, sys; sys.path.insert(0, sys.argv[1])
from audio import write_ulaw_raw
r = 8000
s = [0] * 800 + [int(5000 * (math.sin(2 * math.pi * 697 * n / r) + math.sin(2 * math.pi * 1209 * n / r))) for n in range(2000)] + [0] * 2000
write_ulaw_raw(sys.argv[2], s)
' "$IE/spike" "$WORK/caller/dtmf-1.ulaw"
"$MS_BIN" --node-id media-1 --control-listen "127.0.0.1:$MS_PORT" --media-ip 127.0.0.1 --advertise-ip 127.0.0.1 \
  --port-min 24000 --port-max 24999 --sched-rtp other:0 --sched-watchdog other:0 --rtp-threads 2 \
  --idle-timeout-secs 600 --recording-dir "$WORK/ms/rec" --prompt-dir "$WORK/ms/prompts" \
  --health-dir "$WORK/ms/health" --log-json >"$OUT/parity-ms.log" 2>&1 &
PIDS+=($!)

log "TC (real node, the test profile: x-parity-test-peer until mTLS)"
mkdir -p "$WORK/tc-lock" "$WORK/tc-journal"
printf 'tc-test-esl' >"$WORK/esl-password" && chmod 600 "$WORK/esl-password"  # the test overlay's password
java -jar "$WORK/tc.jar" --spring.profiles.active=test \
  --server.port=$TC_MGMT --parity.telephony-controller.grpc.port=$TC_GRPC \
  --parity.telephony-controller.grpc.test-peer-header=true \
  --parity.telephony-controller.node-id=media-1 --parity.telephony-controller.bins=$BIN \
  --parity.telephony-controller.lock-dir="$WORK/tc-lock" --parity.telephony-controller.journal.dir="$WORK/tc-journal" \
  --parity.telephony-controller.lifecycle.healthy-file="$WORK/tc.healthy" \
  --parity.telephony-controller.lifecycle.drained-file="$WORK/tc.drained" \
  --parity.telephony-controller.etcd.endpoint=$ETCD \
  --parity.telephony-controller.node.esl.password-file="$WORK/esl-password" \
  --parity.telephony-controller.node.cdr-dir="$WORK/fs-out/json_cdr" \
  --parity.telephony-controller.node.parity-ms.port=$MS_PORT \
  --parity.telephony-controller.readiness.endpoints.parity-ms=127.0.0.1:$MS_PORT \
  >"$OUT/tc.log" 2>&1 &
TC_PID=$!
PIDS+=($TC_PID)
for _ in $(seq 180); do curl -sf "http://127.0.0.1:$TC_MGMT/readyz" >/dev/null && break; sleep 0.5; done
curl -sf "http://127.0.0.1:$TC_MGMT/readyz" >/dev/null || die "TC never became ready (see $OUT/tc.log)"

log "IE"
cat >"$OUT/ie.toml" <<EOF
# DEMO level 0 (written by demo/run.sh)
[service]
zone = "local"
bin = "$BIN"
replica_id = "$REPLICA"

[management]
listen = "127.0.0.1:$IE_MGMT"

[lifecycle]
deregistration_delay_ms = 500
drain_timeout_ms = 3000

[telephony]
profile = "demo"
test_peer_header = true
[[telephony.nodes]]
id = "media-1"
grpc = "http://127.0.0.1:$TC_GRPC"
management = "127.0.0.1:$TC_MGMT"

[etcd]
endpoint = "$ETCD"
lease_ttl_s = 10

[tenants]
maps_file = "$MAPS"
allow_test_data = true

[flows]
file = "$IE/demo/flows.json"

[cdr]
host = "127.0.0.1"
port = $PG_PORT
user = "parity_ie"
dbname = "parity_interaction_engine"
password_file = "$PG_PASSWORD_FILE"
migrate = true
EOF
start_ie() { # $1: log file
  IE_STATE_DIR="$WORK" IE_CONFIG="$OUT/ie.toml" "$IE_BIN" >"$1" 2>&1 &
  IE_PID=$!
  PIDS+=($IE_PID)
  for _ in $(seq 100); do curl -sf "http://127.0.0.1:$IE_MGMT/readyz" >/dev/null && return 0; sleep 0.2; done
  die "IE never became ready (see $1)"
}
start_ie "$OUT/ie.log"

log "capture (lo, SIP and RTP)"
# Attached, so the pcap goes straight to the host file (a log driver would mangle the binary).
docker run --rm --name ie-demo-pcap --label "$LABEL" --network host --security-opt label=disable --user 0:0 \
  --cap-drop ALL --cap-add NET_RAW --cap-add NET_ADMIN "$TOOLS_IMAGE" \
  tcpdump -Z root -i lo -U -s 0 -w - 'udp and (port 5060 or port 5080 or portrange 6000-6005 or portrange 24000-24999)' \
  >"$OUT/lo.pcap" 2>"$OUT/tcpdump.stderr" &
PCAP=$!
sleep 2

uuid() { python3 -I -c 'import uuid; print(uuid.uuid4())'; }
sipp() { # $1 name, $2 scenario, $3 media port, $4 tenant, $5 number, $6 interaction, then extra args
  local name=$1 sf=$2 mp=$3 tenant=$4 number=$5 iid=$6; shift 6
  mkdir -p "$OUT/$name" && chmod 777 "$OUT/$name"
  # SIPp's binary carries file capabilities: exec fails unless the container may hold them (as the spike ran it).
  docker run --rm --name "ie-demo-$name" --label "$LABEL" --network host --security-opt label=disable --cap-drop ALL \
    --cap-add NET_RAW --cap-add NET_ADMIN --user "$ME" -v "$IE/demo/sipp:/scen:ro" -v "$IE/spike/prompts:/prompts:ro" -v "$WORK/caller:/caller:ro" -v "$OUT/$name:/out" "$TOOLS_IMAGE" \
    sipp 127.0.0.1:5080 -sf "/scen/$sf" -s "$number" -m 1 -l 1 -i 127.0.0.1 -p 5060 -mi 127.0.0.1 -mp "$mp" \
    -key iid "$iid" -key tenant "$tenant" -key bin "$BIN" -key run "$name$(date +%s)" \
    -cid_str "ie-demo-$name-$(date +%s%N)-%u@%s" -nostdin -timeout 60s \
    -trace_msg -message_file /out/sipp-messages.log -trace_err -error_file /out/sipp-errors.log "$@" \
    >"$OUT/$name/sipp.out" 2>&1
}
declare -A START END IID KILL
call() { # name scenario media-port tenant number [sipp args]: foreground, records times
  local name=$1; IID[$name]=$(uuid); START[$name]=$(now)
  set +e; sipp "$1" "$2" "$3" "$4" "$5" "${IID[$name]}" "${@:6}"; echo $? >"$OUT/$name/exit"; set -e
  END[$name]=$(now)
  log "$name: SIPp exit $(cat "$OUT/$name/exit")"
}

log "call 0: tenant mismatch (X-Tenant-ID globex on acme's number)"
call call0 caller-refused.xml 6004 globex +15550100100
log "call 1: consent, menu, press 1 (RFC 4733) at +10.5 s, sales, IE hangs up"
call call1 caller-ivr.xml 6000 $TENANT +15550100100 -d 10500

log "call 2: answered, 30 s prompt, kill -9 IE mid-call"
IID[call2]=$(uuid); START[call2]=$(now)
( set +e; sipp call2 caller-hangs-up.xml 6002 $TENANT +15550100200 "${IID[call2]}" -d 36000; echo $? >"$OUT/call2/exit" ) &
SIPP2=$!
for _ in $(seq 150); do grep "${IID[call2]}" "$OUT/ie.log" 2>/dev/null | grep -q '"prompt_id":"long-welcome"' && break; sleep 0.1; done
sleep 6
KILL[call2]=$(now)
kill -9 "$IE_PID"
log "call 2: kill -9 IE (pid $IE_PID) at ${KILL[call2]}"
sleep 4
"$IE/target/debug/examples/tc_session" "http://127.0.0.1:$TC_GRPC" $BIN $REPLICA $TENANT "${IID[call2]}" \
  >"$OUT/call2/tc-session-after-kill.json" 2>&1 || true
curl -s "http://127.0.0.1:$TC_MGMT/metrics" | grep -E '^telephony_controller_(buffered_bytes|events_dropped)' >"$OUT/call2/tc-metrics-after-kill.txt" || true
log "call 2: TC's view 4 s after the kill: $(cat "$OUT/call2/tc-session-after-kill.json")"
wait "$SIPP2" || true
END[call2]=$(now)
log "call 2: SIPp exit $(cat "$OUT/call2/exit")"
sleep 1
"$IE/target/debug/examples/tc_session" "http://127.0.0.1:$TC_GRPC" $BIN $REPLICA $TENANT "${IID[call2]}" \
  >"$OUT/call2/tc-session-after-hangup.json" 2>&1 || true
log "call 2: TC's view after the caller hung up: $(cat "$OUT/call2/tc-session-after-hangup.json")"

log "IE restarted with the same replica id"
start_ie "$OUT/ie-restart.log"
sleep 3
kill -TERM "$IE_PID"
wait "$IE_PID" && rc=0 || rc=$?
log "IE (restarted) drained on SIGTERM: exit $rc"

log "the CDR table (no numbers selected)"
sleep 2  # the writer is off the call path
docker exec ie-demo-postgres psql -U parity_ie -d parity_interaction_engine -At -F ' | ' -c \
  "SELECT interaction_id, tenant_id, flow, last_step, consent, consent_basis, gathers, no_inputs, end_reason, ended_by, sip_status, answered_at IS NOT NULL, duration_ms FROM cdr ORDER BY started_at" \
  >"$OUT/cdr.txt" 2>&1 || true
log "CDR rows: $(wc -l <"$OUT/cdr.txt")"

log "stop"
docker stop ie-demo-pcap >/dev/null
wait "$PCAP" || true
kill -TERM "$TC_PID" 2>/dev/null || true
for _ in $(seq 60); do kill -0 "$TC_PID" 2>/dev/null || break; sleep 0.25; done

python3 -I -c '
import json, sys
calls = {}
for i in range(0, len(sys.argv) - 2, 5):
    n, s, e, iid, k = sys.argv[2 + i:7 + i]
    calls[n] = {"start_epoch_s": float(s), "end_epoch_s": float(e), "interaction_id": iid,
                "media_port": {"call0": 6004, "call1": 6000, "call2": 6002}[n]}
    if k != "-":
        calls[n]["kill_epoch_s"] = float(k)
json.dump({"calls": calls}, open(sys.argv[1], "w"), indent=2)
' "$OUT/marks.json" \
  call0 "${START[call0]}" "${END[call0]}" "${IID[call0]}" - \
  call1 "${START[call1]}" "${END[call1]}" "${IID[call1]}" - \
  call2 "${START[call2]}" "${END[call2]}" "${IID[call2]}" "${KILL[call2]}"
chmod 755 "$OUT"/call*  # opened for SIPp's container user during the run
python3 -I "$IE/demo/analyse.py" "$OUT" >/dev/null
# IE never handles an SDP: no SDP type or field anywhere in IE's sources or in telephony.proto.
{ echo "## grep -rni sdp over IE's sources (crates/*/src, examples)"; grep -rni 'sdp' "$IE"/crates/*/src "$IE"/crates/*/examples || true
  echo "## grep -ni sdp in telephony.proto"; grep -ni 'sdp' "$IE/../parity-contracts/proto/parity/telephony/v1/telephony.proto" || true
} >"$OUT/no-sdp.txt"
python3 -I "$IE/demo/summary.py" "$OUT" | tee "$OUT/summary.md"
