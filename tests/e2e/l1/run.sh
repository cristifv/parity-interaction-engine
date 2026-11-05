#!/usr/bin/env bash
# IE step 6 / gateway 5b: DEMO.md level 1, steps 1-6, as one automated run on parity-deploy's
# compose (an override, its own project), with the real routing engine, TC, parity-ms, freeswitch,
# the gateway and two browser agents (Playwright) and SIPp as the caller.
#   tests/e2e/l1/run.sh [evidence dir]      (default docs/evidence/step6/run-<time>)
# The phases (PROGRESS.md "Step 5b and step 6" says what each proves):
#   A  agents a1, a2 log in (SIP registered); both not ready                  DEMO 1
#   B  call A: IVR "welcome / press 1", consent, queued: hold music + its position  DEMO 2, 3
#   C  a2 ready, then a1: a2 (ready longest) is offered first, ignores it; after 15 s the
#      offer goes to a1 with the entry's enqueue time kept (one Enqueue)       DEMO 4, PLAN 5
#   D  a1's page reloads (a reconnect) and accepts the offer it shows; the browser auto-answers
#      the agent leg IE places through TC; both sides hear each other         DEMO 3, GW 5b
#   E  a1's DTMF codes: *1 pause, *2 resume (recording_paused/_resumed), *3 *3 mute, unmute
#   F  kill -9 of IE mid-call: the bridged audio goes on; IE restarts         DEMO 5
#   G  the caller hangs up: the restarted IE writes the CDR from TC's end, frees a1 (wrap-up),
#      a1 finishes wrap-up and is ready again                                 DEMO 5, 6
#   H  call B: queued, hangs up while waiting: Dequeue(ABANDONED), CDR abandoned   PLAN 5
#   I  IE-059: call C queued gets a routing message first (a2 accepts it, its softphone never
#      rings: AGENT_NO_ANSWER, C waits again), then RE killed and started alone (a new
#      leader_epoch, a fresh delivery numbering): IE stays up (no exit, no restart), C re-attaches
#      (re-based, MF-1a; re-enqueued with its seq and times when RE lost it) and keeps its place:
#      a2, logged in again, is offered C, accepts, and the agent leg connects
# Needs: the shared e2e lock (taken here, released on exit), docker, node 22, python3, curl; images
# L1_IE_IMAGE, L1_GW_IMAGE, L1_RE_IMAGE, L1_TC_IMAGE, L1_MS_IMAGE (tags; recorded by id),
# localhost/parity-tools:dev (SIPp, tcpdump) and Playwright's image by digest. Generated dev secrets
# live in secrets/local-dev/ie6-l1/ (700/600) for the run only; nothing is printed.
#   L1_RECORDING=null_sink|off (default null_sink: TC's null sink, contracts 0.10.3)
#   L1_TC_EXTRA / L1_MS_EXTRA: extra flags for TC / parity-ms (the null sink's switches)
#   L1_SKIP_RE_RESTART=1: skip phase I
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
IE=$(cd "$HERE/../../.." && pwd)
WS=$(cd "$IE/.." && pwd)
DEPLOY=$WS/parity-deploy
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=${1:-$IE/docs/evidence/step6/run-$STAMP}
WORK=${L1_WORK_ROOT:-${TMPDIR:-/var/tmp}}/ie6-l1-$STAMP
SECRETS=$WS/secrets/local-dev/ie6-l1
LOCK=$WS/.e2e.lock.d
PROJECT=ie6-l1
DRIVER_PORT=18990
PLAYWRIGHT=mcr.microsoft.com/playwright:v1.64.0-noble@sha256:06a9939e57531807f8d5fd76ce44b53165ffb7d7501d87ab10e285c20b1e971f
TOOLS=localhost/parity-tools:dev
LABEL=parity.test=ie6-l1
export L1_IE_IMAGE=${L1_IE_IMAGE:-localhost/parity-interaction-engine:ie6}
export L1_GW_IMAGE=${L1_GW_IMAGE:-localhost/parity-gateway:ie6}
export L1_RE_IMAGE=${L1_RE_IMAGE:-localhost/parity-cc-routing:ie6}
export L1_TC_IMAGE=${L1_TC_IMAGE:-localhost/parity-telephony-controller:dev}
export L1_MS_IMAGE=${L1_MS_IMAGE:-localhost/parity-ms:dev}
RECORDING=${L1_RECORDING:-null_sink}

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
die() { log "FAILED: $*"; exit 1; }

[[ ! -e "$OUT" ]] || { echo "run.sh: $OUT exists" >&2; exit 2; }
mkdir -p "$OUT"
mkdir "$WORK"
for i in "$L1_IE_IMAGE" "$L1_GW_IMAGE" "$L1_RE_IMAGE" "$L1_TC_IMAGE" "$L1_MS_IMAGE" "$TOOLS" "$PLAYWRIGHT"; do
  docker image inspect "$i" >/dev/null 2>&1 || die "image $i missing"
done

# ---- the shared e2e lock (host ports): taken here, released on exit --------------------------
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "run.sh: the e2e lock is held: $(cat "$LOCK/owner.txt" 2>/dev/null)" >&2
  rm -rf "$OUT" "$WORK"
  exit 75
fi
echo "ie-integration-1 (IE tests/e2e/l1/run.sh, pid $$) since $(date -Iseconds)" >"$LOCK/owner.txt"

export PARITY_UID PARITY_GID IE_FLOWS_FILE IE_TENANTS_FILE IE_MIGRATIONS_DIR PARITY_RE_FIRST_START L1_WORK=$WORK L1_SECRETS=$SECRETS
PARITY_UID=$(id -u); PARITY_GID=$(id -g)
IE_FLOWS_FILE=$HERE/flows.json
IE_TENANTS_FILE=$WS/parity-contracts/tenants/test-tenants.v1.json
IE_MIGRATIONS_DIR=$IE/migrations
PARITY_RE_FIRST_START=yes
export PARITY_TURN_URLS=
export L1_MS_COMMAND="--node-id=media-1 --control-listen=127.0.0.1:2223 --media-ip=127.0.0.1 --advertise-ip=127.0.0.1 --port-min=16384 --port-max=32767 --rtp-threads=2 --sched-rtp=other:0 --sched-watchdog=other:0 --prompt-dir=/prompts --recording-dir=/recordings --health-dir=/health --log-json ${L1_MS_EXTRA:-}"
# The dev registrar: TC dials agents of acme.agents.localhost registered with freeswitch itself.
export L1_TC_COMMAND="--spring.profiles.active=test --parity.telephony-controller.grpc.test-peer-header=true --parity.telephony-controller.node.agent-registrar-domain=acme.agents.localhost ${L1_TC_EXTRA:-}"
COMPOSE=(docker compose -p "$PROJECT" -f "$DEPLOY/compose/compose.yml" -f "$HERE/compose.l1.yml"
  --profile media --profile core --profile app)
SERVICES=(etcd freeswitch parity-ms telephony-controller postgres parity-cc-routing interaction-engine parity-gateway)

api() {  # api METHOD /a1/what [json body]
  local m=$1 p=$2 b=${3:-}
  if [[ -n "$b" ]]; then
    curl -s --max-time 60 -o "$WORK/api.out" -w '%{http_code}' -X "$m" -H 'content-type: application/json' --data-binary "$b" "http://127.0.0.1:$DRIVER_PORT$p"
  else
    curl -s --max-time 60 -o "$WORK/api.out" -w '%{http_code}' -X "$m" "http://127.0.0.1:$DRIVER_PORT$p"
  fi
}
login() {  # the token goes through a pipe from its 600 file: never on a command line or in a log
  python3 -I -c 'import json,sys; print(json.dumps({"token": open(sys.argv[1]).read().strip()}))' "$SECRETS/token-$1" \
    | curl -s --max-time 60 -o "$WORK/api.out" -w '%{http_code}' -X POST -H 'content-type: application/json' --data-binary @- "http://127.0.0.1:$DRIVER_PORT/$1/login"
}
cid() { "${COMPOSE[@]}" ps -a -q "$1"; }
healthy() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "$(cid "$1")" 2>/dev/null)" == healthy ]]; }
wait_healthy() { for _ in $(seq 1 180); do healthy "$1" && return 0; sleep 1; done; die "$1 never healthy"; }
ie_logs() { docker logs "$(cid interaction-engine)" 2>&1; }
wait_ie() {  # wait_ie <seconds> <grep -E pattern>
  for _ in $(seq 1 $(( $1 * 2 ))); do ie_logs | grep -Eq -- "$2" && return 0; sleep 0.5; done; return 1
}
metric() { curl -s --max-time 2 "http://127.0.0.1:$1/metrics" | awk -v m="$2" '$1 ~ "^"m {s+=$2} END {print s+0}'; }
mark() { echo "{\"t\": $(date +%s.%N), \"what\": \"$1\"}" >>"$OUT/marks.jsonl"; log "mark: $1"; }
caller() {  # caller <name> <stay ms> <iid>
  sed "s/@STAY_MS@/$2/" "$HERE/sipp/caller.xml.in" >"$WORK/sipp/caller-$1.xml"
  mkdir -p "$WORK/sipp-$1"
  docker run -d --name "ie6-l1-caller-$1" --label "$LABEL" --network host --user "$PARITY_UID:$PARITY_GID" \
    --cap-drop ALL --cap-add NET_RAW --cap-add NET_ADMIN --security-opt label=disable --read-only --tmpfs /tmp \
    -v "$WORK/sipp:/scen:ro" -v "$WORK/media:/media:ro" -v "$WORK/sipp-$1:/out" -w /out "$TOOLS" \
    sipp 127.0.0.1:5080 -sf "/scen/caller-$1.xml" -d 9500 -s +15550100100 -m 1 -l 1 -i 127.0.0.1 -p 5062 \
    -mi 127.0.0.1 -mp 6000 -key iid "$3" -key tenant acme -key bin bin-a -nostdin -timeout $(( $2 / 1000 + 90 ))s \
    -cid_str "ie6-$1-$STAMP-%u@%s" \
    -trace_msg -message_file /out/sipp-messages.log -trace_err -error_file /out/sipp-errors.log >/dev/null
}

down() {
  set +e
  log "logs and teardown"
  [[ -s "$WORK/driver.cid" ]] && { api POST /quit/x >/dev/null; sleep 2; }
  for c in ie6-l1-browsers ie6-l1-pcap ie6-l1-caller-a ie6-l1-caller-b ie6-l1-caller-c; do
    docker logs "$c" >"$OUT/container-$c.log" 2>&1
    docker rm -f "$c" >/dev/null 2>&1
  done
  curl -s --max-time 2 http://127.0.0.1:9810/metrics >"$OUT/re-metrics-end.txt" 2>/dev/null
  curl -s --max-time 2 http://127.0.0.1:9600/metrics >"$OUT/ie-metrics-end.txt" 2>/dev/null
  pg=$(cid postgres)
  if [[ -n "$pg" ]]; then
    docker exec "$pg" psql -U postgres -d parity_interaction_engine -At -c \
      "SELECT row_to_json(c) FROM cdr c ORDER BY started_at" >"$OUT/cdr.jsonl" 2>"$OUT/cdr.err"
    docker exec "$pg" psql -U postgres -d parity_interaction_engine -At -c \
      "SELECT row_to_json(p) FROM cdr_progress p ORDER BY started_at, progress_seq" >"$OUT/cdr-progress.jsonl" 2>>"$OUT/cdr.err"
  fi
  for s in "${SERVICES[@]}"; do
    "${COMPOSE[@]}" logs --no-color --no-log-prefix "$s" >"$OUT/$s.log" 2>&1
  done
  # PL-083: freeswitch's log dir is a tmpfs (its expanded configuration holds the ESL password):
  # only freeswitch.log comes out, streamed, with any line naming a password dropped on the way.
  "${COMPOSE[@]}" exec -T freeswitch cat /var/log/freeswitch/freeswitch.log 2>/dev/null \
    | grep -viE 'passw|secret' >"$OUT/freeswitch-file.log"
  "${COMPOSE[@]}" down -v --remove-orphans --timeout 20 >>"$OUT/run.log" 2>&1
  # log-record.v1: IE's whole log against the schema, its catalogue and the masking rules.
  "$WS/parity-contracts/rust/target/debug/contract-check" --root "$WS/parity-contracts" \
    --catalogue "$IE/logging/events.parity-interaction-engine.v1.json" --logs "$OUT/interaction-engine.log" \
    --skip-non-json >"$OUT/ie-log-check.txt" 2>&1
  cp "$WORK"/browser/*.jsonl "$OUT/" 2>/dev/null
  for x in a b c; do
    for f in sipp-messages.log sipp-errors.log; do cp "$WORK/sipp-$x/$f" "$OUT/caller-$x-$f" 2>/dev/null; done
  done
  [[ -s "$WORK/capture/lo.pcap" ]] && python3 -I "$HERE/media.py" analyse "$WORK/capture/lo.pcap" 6000 \
    "$(python3 -I -c 'import json,sys; m=[json.loads(l) for l in open(sys.argv[1])]; print(next((x["t"] for x in m if x["what"]=="F ie killed"), 0))' "$OUT/marks.jsonl" 2>/dev/null || echo 0)" \
    >"$OUT/media.json" 2>"$OUT/media.err"
  python3 -I "$HERE/analyse.py" "$OUT" >"$OUT/analyse.out" 2>&1; ok=$?
  log "remaining $PROJECT containers: $(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | wc -l)"
  "$DEPLOY/scripts/check-no-secrets.sh" "$OUT" >"$OUT/check-no-secrets.txt" 2>&1 \
    || { echo "run.sh: the evidence held a secret value: removed" >&2; rm -rf "$OUT"; ok=1; }
  rm -rf "$SECRETS" "$WORK"
  rm -rf "$LOCK"
  log "result: $([[ $ok == 0 ]] && echo PASS || echo FAIL) ($OUT/summary.md)"
  exit $ok
}
trap down EXIT

# ---- preflight: ports, secrets, inputs ----------------------------------------------------------
for p in 2379 2380 2223 5062 5066 5080 5090 6000 8021 9600 9700 9701 9800 9810 18090 18091 $DRIVER_PORT; do
  if ss -Hltun "sport = :$p" | grep -q .; then die "port $p is in use"; fi
done
mkdir -m 700 "$SECRETS" || die "$SECRETS exists (a previous run's?)"
mkdir -m 700 "$SECRETS/fs-directory"
mkdir -p "$WORK"/{fs-log/json_cdr,ms-recordings,ms-health,tc-lock,tc-journal,gw,capture,browser,driver,sipp}
chmod 777 "$WORK"/browser "$WORK"/capture
# Per-run dev SIP passwords (GW-003's dev path) and freeswitch's directory with the same ones.
python3 -I - "$SECRETS" <<'PY'
import os, secrets, sys
d = sys.argv[1]
pw = {a: secrets.token_urlsafe(24) for a in ("agent-a1", "agent-a2")}
def write(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
write(os.path.join(d, "sip-agents.properties"), "".join(f"acme/{a}={p}\n" for a, p in pw.items()))
users = "".join(f'      <user id="{a}"><params><param name="password" value="{p}"/></params></user>\n' for a, p in pw.items())
dial = "{^^:sip_invite_domain=${dialed_domain}:presence_id=${dialed_user}@${dialed_domain}}${sofia_contact(*/${dialed_user}@127.0.0.1)}"
write(os.path.join(d, "fs-directory", "agents.xml"),
      "<include>\n  <!-- IE step 6 e2e, generated per run: never committed. -->\n"
      '  <domain name="127.0.0.1">\n    <params><param name="jsonrpc-allowed-methods" value=""/></params>\n'
      '    <groups><group name="agents"><users>\n' + users + "    </users></group></groups>\n  </domain>\n"
      '  <domain name="acme.agents.localhost">\n    <params>\n'
      f'      <param name="dial-string" value="{dial}"/>\n      <param name="jsonrpc-allowed-methods" value=""/>\n'
      "    </params>\n    <groups><group name=\"agents\"><users>"
      + "".join(f'<user id="{a}"/>' for a in pw) + "</users></group></groups>\n  </domain>\n</include>\n")
PY
# freeswitch runs as this UID and reads its directory through the read-only mount.
node "$HERE/tokens.mjs" "$WS/secrets/local-dev/gateway-dev-jwt.jwk.json" "$WORK/gw/jwks.json" "$SECRETS" agent-a1 agent-a2
chmod 644 "$WORK/gw/jwks.json"
python3 -I "$HERE/media.py" gen "$WORK"
chmod -R a+rX "$WORK/prompts" "$WORK/media" "$WORK/in"
cp "$HERE/agents.mjs" "$WORK/driver/"
cp "$WS/parity-gateway/e2e/package.json" "$WS/parity-gateway/e2e/package-lock.json" "$WORK/driver/"
sed "s/@RECORDING@/$RECORDING/" "$HERE/ie.toml.in" >"$WORK/ie.toml"
cp "$HERE"/sipp/*.in "$WORK/sipp/"
{
  echo "# Inputs of $(basename "$OUT")"
  for i in "$L1_IE_IMAGE" "$L1_GW_IMAGE" "$L1_RE_IMAGE" "$L1_TC_IMAGE" "$L1_MS_IMAGE" "$TOOLS" "$PLAYWRIGHT"; do
    echo "$i $(docker image inspect -f '{{.Id}}' "$i")"
  done
  sha256sum "$DEPLOY/compose/compose.yml" "$HERE/compose.l1.yml" "$WORK/ie.toml" "$IE_FLOWS_FILE" "$IE_TENANTS_FILE" \
    "$HERE/sipp/caller.xml.in" "$HERE/agents.mjs" "$WS/parity-cc-routing/seed/local-dev.json" "$IE/migrations/"*.sql
  echo "parity-contracts $(cat "$WS/parity-contracts/VERSION")"
  echo "recording mode: $RECORDING; TC extra: ${L1_TC_EXTRA:-none}; parity-ms extra: ${L1_MS_EXTRA:-none}"
} >"$OUT/inputs.txt"
# Playwright's npm package (the image's version), installed into the driver dir from the gateway's lock.
docker run --rm --user "$PARITY_UID:$PARITY_GID" -e HOME=/tmp -e npm_config_cache=/tmp/npm --security-opt label=disable \
  -v "$WORK/driver:/driver" -w /driver "$PLAYWRIGHT" npm ci --ignore-scripts --no-audit --no-fund >"$OUT/npm.log" 2>&1 \
  || die "npm ci for the driver failed (npm.log)"

# ---- up ---------------------------------------------------------------------------------------
log "up: ${SERVICES[*]} (project $PROJECT, recording $RECORDING)"
"${COMPOSE[@]}" up -d "${SERVICES[@]}" >>"$OUT/run.log" 2>&1 || die "compose up"
for s in telephony-controller parity-cc-routing interaction-engine parity-gateway; do wait_healthy "$s"; done
log "healthy: TC, RE (incarnation $(metric 9810 parity_cc_routing_incarnation)), IE, gateway"
docker run -d --name ie6-l1-pcap --label "$LABEL" --network host --user 0:0 --cap-drop ALL --cap-add NET_RAW \
  --cap-add NET_ADMIN --security-opt label=disable -v "$WORK/capture:/out" "$TOOLS" tcpdump -Z root -i lo -U \
  --immediate-mode -s 0 -w /out/lo.pcap 'udp and (port 5062 or port 6000 or portrange 16384-32767)' >/dev/null
docker run -d --name ie6-l1-browsers --label "$LABEL" --network host --user pwuser --cap-drop ALL --cap-add SYS_CHROOT \
  --security-opt label=disable --security-opt "seccomp=$IE/compose/chromium-seccomp.json" --shm-size 512m \
  -v "$WORK/driver:/driver:ro" -v "$WORK/in:/in:ro" -v "$WORK/browser:/out" --entrypoint node "$PLAYWRIGHT" \
  /driver/agents.mjs /out "http://acme.localhost:18090/agent/index.html" "$DRIVER_PORT" >"$WORK/driver.cid"
for _ in $(seq 1 60); do curl -s --max-time 1 "http://127.0.0.1:$DRIVER_PORT/" >/dev/null 2>&1 && break; sleep 0.5; done

# ---- A: the agents log in (DEMO 1) --------------------------------------------------------------
mark "A login"
[[ "$(login a1)" == 200 ]] || die "a1 login: $(cat "$WORK/api.out")"
[[ "$(login a2)" == 200 ]] || die "a2 login: $(cat "$WORK/api.out")"
api POST /a1/ready >/dev/null; sleep 1; api POST /a1/not_ready >/dev/null
log "A: a1 and a2 registered over WS; a1 Ready then Not ready (both not ready)"

# ---- B: call A queues (DEMO 2, 3) ---------------------------------------------------------------
IID_A=$(python3 -I -c 'import uuid; print(uuid.uuid7())')
echo "$IID_A" >"$OUT/call-a.iid"
mark "B call A dials"
caller a 110000 "$IID_A"
wait_ie 40 '"event":"routing.enqueued"' || die "call A was never enqueued"
mark "B call A queued"
sleep 14   # hold music and the position announcement

# ---- C: longest ready, the ignored offer (DEMO 4, PLAN 5) ---------------------------------------
api POST /a2/ready >/dev/null; mark "C a2 ready"
sleep 3
api POST /a1/ready >/dev/null; mark "C a1 ready"
[[ "$(api GET '/a2/offer?wait_ms=10000')" == 200 ]] || die "a2 (ready longest) got no offer"
mark "C a2 offered (ignores it)"
[[ "$(api GET '/a1/offer?wait_ms=0')" == 404 ]] || die "a1 was offered while a2 held the offer"
[[ "$(api GET '/a1/offer?wait_ms=25000')" == 200 ]] || die "the ignored offer never reached a1"
mark "C a1 offered after the 15 s timeout"

# ---- D: a1 reconnects, accepts from the page's view, auto-answers (DEMO 3, GW 5b) ---------------
[[ "$(api POST /a1/reload)" == 200 ]] || die "a1 reload: $(cat "$WORK/api.out")"
cp "$WORK/api.out" "$OUT/a1-reload.json"
mark "D a1 reloaded"
[[ "$(api GET '/a1/offer?wait_ms=20000')" == 200 ]] || die "no offer on a1's page after the reconnect"
api POST /a1/accept >/dev/null; mark "D a1 accepted"
[[ "$(api GET '/a1/answered?wait_ms=20000')" == 200 ]] || die "a1's softphone never answered the agent leg"
mark "D a1 answered"
api POST /a1/capture >/dev/null
sleep 8

# ---- E: the agent's DTMF codes -------------------------------------------------------------------
n=0
for c in '*1' '*2' '*3' '*3'; do
  api POST /a1/dtmf "{\"codes\": \"$c\"}" >/dev/null
  mark "E dtmf $((++n))"
  sleep 4
done

# ---- F: kill -9 of IE mid-call (DEMO 5) -----------------------------------------------------------
mark "F ie killed"
docker kill -s KILL "$(cid interaction-engine)" >>"$OUT/run.log" 2>&1
sleep 8
api GET /a1/heard >/dev/null && cp "$WORK/api.out" "$OUT/a1-heard-after-kill.json"
# The same container started again (no compose operation: nothing else restarts with it).
docker start "$(cid interaction-engine)" >>"$OUT/run.log" 2>&1
wait_healthy interaction-engine
mark "F ie restarted"

# ---- G: the caller hangs up; the CDR closes; a1 wraps up (DEMO 5, 6) ------------------------------
rc_a=$(docker wait ie6-l1-caller-a)
mark "G caller A hung up (sipp exit $rc_a)"
[[ "$(api GET '/a1/wrap?wait_ms=30000')" == 200 ]] || log "G: a1 got no wrap-up (the agent wasn't freed)"
api GET /a1/heard >/dev/null && cp "$WORK/api.out" "$OUT/a1-heard.json"
api POST /a1/wrap_done >/dev/null; mark "G a1 wrap-up done"
sleep 3

# ---- H: a caller abandons while queued (PLAN 5) ----------------------------------------------------
api POST /a1/not_ready >/dev/null; api POST /a2/not_ready >/dev/null
IID_B=$(python3 -I -c 'import uuid; print(uuid.uuid7())')
echo "$IID_B" >"$OUT/call-b.iid"
mark "H call B dials"
caller b 10000 "$IID_B"
rc_b=$(docker wait ie6-l1-caller-b)
mark "H caller B hung up while queued (sipp exit $rc_b)"
wait_ie 10 "\"event\":\"routing.dequeued\".*\"xsid\":\"$IID_B\"" || log "H: no Dequeue seen for call B"
sleep 2

# ---- I: IE-059, routing-engine restarted alone mid-call (a new leader_epoch) ----------------------
if [[ "${L1_SKIP_RE_RESTART:-0}" != 1 ]]; then
  IID_C=$(python3 -I -c 'import uuid; print(uuid.uuid7())')
  echo "$IID_C" >"$OUT/call-c.iid"
  mark "I call C dials"
  caller c 50000 "$IID_C"
  wait_ie 40 "\"event\":\"routing.enqueued\".*\"xsid\":\"$IID_C\"" || die "call C was never enqueued"
  mark "I call C queued"
  # MF-1a: C gets a routing message before the restart: a2 accepts it, but its softphone never
  # sees the INVITE, so the agent leg goes unanswered (AGENT_NO_ANSWER) and C waits again.
  api POST /a2/drop_invites '{"on": true}' >/dev/null
  api POST /a2/ready >/dev/null
  [[ "$(api GET '/a2/offer?wait_ms=20000')" == 200 ]] || die "a2 was never offered call C"
  api POST /a2/accept >/dev/null
  mark "I a2 accepted call C (its softphone won't ring)"
  wait_ie 25 "\"event\":\"routing.outcome_recorded\".*\"xsid\":\"$IID_C\"" || log "I: no outcome for C's unanswered leg"
  mark "I call C waits again"
  api POST /a2/drop_invites '{"on": false}' >/dev/null
  mark "I re killed"
  inc=$(metric 9810 parity_cc_routing_incarnation)
  docker kill -s KILL "$(cid parity-cc-routing)" >>"$OUT/run.log" 2>&1
  # Started alone, outside compose (a supervisor's crash restart): IE and the gateway are not.
  docker start "$(cid parity-cc-routing)" >>"$OUT/run.log" 2>&1
  wait_healthy parity-cc-routing
  mark "I re restarted alone (incarnation $inc -> $(metric 9810 parity_cc_routing_incarnation))"
  wait_ie 20 '"event":"routing.leader_changed"' || log "I: IE saw no leader change"
  wait_ie 20 "\"event\":\"routing.reattached\".*\"xsid\":\"$IID_C\"" || log "I: call C didn't re-attach"
  sleep 2
  # IE never exits on it: running, exit code 0, no restart.
  docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' "$(cid interaction-engine)" >"$OUT/ie-after-re-restart.txt"
  mark "I ie: $(cat "$OUT/ie-after-re-restart.txt")"
  # The queued caller kept its place: a2 turns ready and is offered call C. This RE (phase 1, no
  # re.state) forgot its agents too: the gateway's re-attach got a new attach_generation and
  # closed the desks (4009, GW-037), so a2 logs in again first (an RE with re.state keeps both).
  [[ "$(api POST /a2/reload)" == 200 ]] || log "I: a2 couldn't log in again: $(cat "$WORK/api.out")"
  api POST /a2/ready >/dev/null
  if [[ "$(api GET '/a2/offer?wait_ms=20000')" == 200 ]]; then
    mark "I a2 offered call C"
    api POST /a2/accept >/dev/null
    if [[ "$(api GET '/a2/answered?wait_ms=20000')" == 200 ]]; then mark "I a2 answered call C"; else log "I: a2 never answered C"; fi
  else
    log "I: call C was never offered to a2"
  fi
  rc_c=$(docker wait ie6-l1-caller-c)
  mark "I caller C hung up (sipp exit $rc_c)"
  sleep 2
fi
api GET /a1/log >/dev/null; api GET /a2/log >/dev/null
log "scenario done"
