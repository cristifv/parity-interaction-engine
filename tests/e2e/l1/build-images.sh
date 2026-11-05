#!/usr/bin/env bash
# The e2e's own image tags (so another agent's rebuild of a :dev tag never changes a run):
#   localhost/parity-interaction-engine:ie6  IE's release binary (cargo build --release on this
#        Fedora 43 host, offline) on the runtime base of IE's Dockerfile (the same Fedora 43 digest).
#        IE's own Dockerfile builds inside docker and needs crates.io, which docker builds here
#        can't reach.
#   localhost/parity-gateway:ie6             the gateway's Dockerfile with its packaged jar
#   localhost/parity-cc-routing:ie6          a tag of an existing RE image (`re`, default :dev), or
#        (`re-build`) RE's Dockerfile with the binary, libtorch libraries, trainer and base model
#        its tree's `bazel-bin` holds (built there by RE's owner: read here, never built)
#   localhost/parity-telephony-controller:ie6  TC's Dockerfile with TC's built jar (read, never
#        built here: TC's owner builds it)
#   localhost/parity-ms:ie6                  parity-ms's own Dockerfile on its repo (read-only)
# Usage: build-images.sh [ie] [gateway] [re=<image> | re-build] [tc] [ms]   (default: ie gateway re)
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
IE=$(cd "$HERE/../../.." && pwd)
WS=$(cd "$IE/.." && pwd)
CTX=$(mktemp -d "${TMPDIR:-/var/tmp}/ie6-images.XXXXXX")
trap 'rm -rf "$CTX"' EXIT
want=("$@"); (( ${#want[@]} )) || want=(ie gateway re)
for w in "${want[@]}"; do
  case "$w" in
    ie)
      (cd "$IE" && cargo build --release --locked --offline -p interaction-engine -j 4)
      mkdir "$CTX/ie"
      cp "$IE/target/release/parity-interaction-engine" "$CTX/ie/"
      base=$(sed -n 's/^FROM \(docker.io\/library\/fedora@sha256:[0-9a-f]*\).*/\1/p' "$IE/Dockerfile")
      printf '%s\n' "FROM $base" "COPY parity-interaction-engine /usr/local/bin/parity-interaction-engine" \
        "USER 10001:10001" 'ENTRYPOINT ["/usr/local/bin/parity-interaction-engine"]' >"$CTX/ie/Dockerfile"
      docker build -q -t localhost/parity-interaction-engine:ie6 "$CTX/ie" ;;
    gateway)
      (cd "$WS/parity-gateway" && mvn -B -ntp -o -T 2 -DskipTests package -q)
      mkdir "$CTX/gw"
      cp "$WS/parity-gateway/Dockerfile" "$CTX/gw/"
      cp "$WS/parity-gateway/target/parity-gateway-0.1.0.jar" "$CTX/gw/app.jar"
      docker build -q -t localhost/parity-gateway:ie6 "$CTX/gw" ;;
    re-build)
      re=$WS/parity-cc-routing
      bin=$re/bazel-bin/parity-cc-routing
      trainer=$re/bazel-bin/src/trainer/parity-cc-routing-trainer
      [[ -x "$bin" && -x "$trainer" ]] || { echo "build-images.sh: RE's bazel-bin has no binary or trainer" >&2; exit 2; }
      mkdir "$CTX/re" "$CTX/re/lib" "$CTX/re/model"
      cp "$re/Dockerfile" "$CTX/re/"
      cp --dereference "$bin" "$CTX/re/parity-cc-routing"
      cp --dereference "$trainer" "$CTX/re/parity-cc-routing-trainer"
      solib="$(dirname "$(readlink -f "$bin")")/_solib_k8"
      for so in libtorch_cpu.so libtorch.so libc10.so libgomp.so.1; do
        cp --dereference "$(find "$solib" -name "$so" | head -1)" "$CTX/re/lib/$so"
      done
      cp "$re/model/forecast/base.pt" "$re/model/forecast/base.json" "$CTX/re/model/"
      sha256sum "$CTX/re/parity-cc-routing"
      docker build -q -t localhost/parity-cc-routing:ie6 "$CTX/re" ;;
    re|re=*)
      src=${w#re=}; [[ "$src" == re ]] && src=localhost/parity-cc-routing:dev
      docker tag "$src" localhost/parity-cc-routing:ie6 ;;
    tc)
      jar="$WS/parity-telephony-controller/target/parity-telephony-controller-0.1.0.jar"
      mkdir "$CTX/tc"
      cp "$WS/parity-telephony-controller/Dockerfile" "$CTX/tc/"
      cp "$jar" "$CTX/tc/app.jar"
      sha256sum "$CTX/tc/app.jar"
      docker build -q -t localhost/parity-telephony-controller:ie6 "$CTX/tc" ;;
    ms)
      docker build -q -t localhost/parity-ms:ie6 "$WS/parity-ms" ;;
    *) echo "build-images.sh: unknown image $w" >&2; exit 2 ;;
  esac
done
for i in parity-interaction-engine parity-gateway parity-cc-routing parity-telephony-controller parity-ms; do
  echo "localhost/$i:ie6 $(docker image inspect -f '{{.Id}}' "localhost/$i:ie6" 2>/dev/null || echo missing)"
done
