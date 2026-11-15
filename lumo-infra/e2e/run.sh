#!/usr/bin/env bash
# End-to-end test: build every Lumo service, run them all together against
# real Postgres and Redis plus in-memory Kafka / S3 / Paystack / LiveKit
# (fakes/), then drive real user journeys through the gateway (journey/).
#
# Needs: Go, Postgres (superuser URL in PG_URL), Redis, ffmpeg, openssl.
#   lumo-infra/e2e/run.sh            # build, run, test, stop
#   KEEP=1 lumo-infra/e2e/run.sh     # leave the stack running afterwards
#   ONLY_UP=1 lumo-infra/e2e/run.sh  # start the stack, skip the journey
#   then: (cd lumo-infra/e2e && go run ./load -vus 100 -duration 60s)
set -euo pipefail

ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}
WORK=${WORK:-/tmp/lumo-e2e}
PG=${PG_URL:-postgres://postgres:postgres@localhost:5432}
REDIS=${E2E_REDIS_URL:-redis://localhost:6379/5}
BIN=$WORK/bin LOGS=$WORK/logs
rm -rf "$WORK/logs" "$WORK/tmp" && mkdir -p "$BIN" "$LOGS" "$WORK/tmp"
PIDS=()

cleanup() {
  [ "${KEEP:-}" = 1 ] && { echo "stack left running; logs in $LOGS"; return; }
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "== build"
build() { (cd "$ROOT/$1" && go build -o "$BIN/$2" "./cmd/$3") || exit 1; }
build svc-gateway gateway gateway &
build svc-identity identity identity &
build svc-wallet wallet wallet &
build svc-live live live &
build svc-realtime realtime realtime &
build svc-social social social &
build svc-media media media &
build svc-feed feed feed &
build svc-notifier notifier notifier &
build svc-messaging messaging messaging &
build svc-moderation moderation moderation &
build worker-transcoder transcoder transcoder &
(cd "$ROOT/lumo-infra/e2e" && go build -o "$BIN/fakes" ./fakes) &
wait

echo "== databases"
for db in identity wallet_a wallet_b live social media notifier messaging moderation; do
  if [ "${KEEP_DATA:-}" = 1 ]; then # local dev (local/dev.sh): keep what is there
    psql "$PG/postgres" -tAc "SELECT 1 FROM pg_database WHERE datname = 'e2e_$db'" | grep -q 1 || psql "$PG/postgres" -qc "CREATE DATABASE e2e_$db" >/dev/null
  else
    psql "$PG/postgres" -qc "DROP DATABASE IF EXISTS e2e_$db WITH (FORCE)" -c "CREATE DATABASE e2e_$db" >/dev/null
  fi
done
[ "${KEEP_DATA:-}" = 1 ] || redis-cli -u "$REDIS" FLUSHDB >/dev/null

echo "== fakes"
"$BIN/fakes" >"$LOGS/fakes.log" 2>&1 & PIDS+=($!)
for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/19092) 2>/dev/null && break; sleep 0.1; done

# Secrets for this run only (kept between runs with KEEP_DATA=1, so sign-ins
# and wallet tags survive a restart).
if [ "${KEEP_DATA:-}" != 1 ] || [ ! -f "$WORK/secrets.env" ]; then
  openssl genpkey -algorithm ed25519 -out "$WORK/signing.pem" 2>/dev/null
  printf 'OTP_SECRET=%s\nTAG_SECRET=%s\n' "$(openssl rand -base64 32)" "$(openssl rand -base64 32)" >"$WORK/secrets.env"
fi
# shellcheck disable=SC1091
. "$WORK/secrets.env"
export LOG_LEVEL=${LOG_LEVEL:-warn}  # dev OTP codes are logged at warn
export KAFKA_BROKERS=localhost:19092
export REDIS_URL=$REDIS
export JWKS_URL=http://localhost:18081/.well-known/jwks.json
export IDENTITY_URL=http://localhost:18081 SOCIAL_URL=http://localhost:18085
export INTERNAL_TOKEN=e2e-internal-token
db() { echo "$PG/e2e_$1?sslmode=disable"; }
# Accounts for the load test (load/): load1..200@e2e.test, code 000000.
LOAD_ACCOUNTS=$(seq -f "load%g@e2e.test=000000" 1 200 | paste -sd,)

declare -A ENV
ENV[identity]="DATABASE_URL=$(db identity) SIGNING_KEYS=k1:$(base64 <"$WORK/signing.pem" | tr -d "\n") OTP_HMAC_SECRET=$OTP_SECRET
  OTP_TEST_ACCOUNTS=creator@e2e.test=111111,fan@e2e.test=222222,finance@e2e.test=333333,mod@e2e.test=444444,stranger@e2e.test=555555,$LOAD_ACCOUNTS
  SMS_PROVIDER=log EMAIL_PROVIDER=log GOOGLE_CLIENT_IDS=e2e-web"
ENV[wallet]="SHARDS=0-2047=$(db wallet_a);2048-4095=$(db wallet_b) ACCOUNT_TAG_SECRET=$TAG_SECRET
  MIN_PAYOUT_DIAMONDS=100 PAYOUT_KOBO_PER_DIAMOND=50 PAYSTACK_SECRET_KEY=sk_test_e2e PAYSTACK_BASE_URL=http://localhost:19100"
ENV[live]="DATABASE_URL=$(db live) LIVEKIT_WS_URL=ws://localhost:19200 LIVEKIT_API_URL=http://localhost:19200 LIVEKIT_API_KEY=e2ekey
  LIVEKIT_API_SECRET=e2e-livekit-secret-0123456789abcdef EGRESS_S3_ENDPOINT=http://localhost:19000 EGRESS_S3_BUCKET=lumo-media
  EGRESS_S3_ACCESS_KEY=e2e EGRESS_S3_SECRET=e2e HLS_BASE_URL=http://localhost:19000/lumo-media"
ENV[realtime]="WS_ORIGINS=*"
ENV[social]="DATABASE_URL=$(db social)"
S3ENV="S3_ENDPOINT=http://localhost:19000 S3_REGION=us-east-1 S3_BUCKET=lumo-media S3_ACCESS_KEY=e2e S3_SECRET_KEY=e2e"
ENV[media]="DATABASE_URL=$(db media) $S3ENV CDN_BASE_URL=http://localhost:19000/lumo-media"
ENV[transcoder]="$S3ENV MAX_JOBS=2 TMP_DIR=$WORK/tmp"
ENV[feed]=""
ENV[notifier]="DATABASE_URL=$(db notifier)"
ENV[messaging]="DATABASE_URL=$(db messaging)"
ENV[moderation]="DATABASE_URL=$(db moderation)"
ENV[gateway]="ROUTES=/.well-known=http://localhost:18081,/v1/auth=http://localhost:18081,/v1/me=http://localhost:18081,/v1/users=http://localhost:18081,/v1/wallet=http://localhost:18082,/v1/live=http://localhost:18083,/v1/rt=http://localhost:18084,/v1/social=http://localhost:18085,/v1/media=http://localhost:18086,/v1/feed=http://localhost:18087,/v1/notify=http://localhost:18088,/v1/dm=http://localhost:18089,/v1/moderation=http://localhost:18090
  RATE_PER_USER_MIN=${RATE_PER_USER_MIN:-100000} RATE_PER_IP_MIN=${RATE_PER_IP_MIN:-100000} RATE_AUTH_PER_IP_MIN=${RATE_AUTH_PER_IP_MIN:-100000}
  ALLOWED_ORIGINS=${E2E_ORIGINS:-http://localhost:3000} TRUSTED_PROXY_CIDRS="
declare -A PORT=([gateway]=18080 [identity]=18081 [wallet]=18082 [live]=18083 [realtime]=18084 [social]=18085 [media]=18086
  [feed]=18087 [notifier]=18088 [messaging]=18089 [moderation]=18090 [transcoder]=18091)

run() { # run <svc> <command> <port>
  local svc=$1 cmd=$2 port=$3
  # shellcheck disable=SC2086
  env ${ENV[$svc]} ADDR=":$port" "$BIN/$svc" "$cmd" >>"$LOGS/$svc-$cmd.log" 2>&1 &
  PIDS+=($!)
}

echo "== migrate"
for s in identity wallet live social media notifier messaging moderation; do
  # shellcheck disable=SC2086
  env ${ENV[$s]} "$BIN/$s" migrate >"$LOGS/$s-migrate.log" 2>&1 || { echo "migrate $s failed"; tail -20 "$LOGS/$s-migrate.log"; exit 1; }
done

echo "== start"
for s in identity wallet live realtime social media feed notifier messaging moderation; do
  run "$s" serve "${PORT[$s]}"
  run "$s" worker $((PORT[$s] + 100))
done
run transcoder worker "${PORT[transcoder]}"
run gateway serve "${PORT[gateway]}"

wait_ready() { # wait_ready <port> <name>
  for _ in $(seq 150); do
    curl -sf "http://localhost:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "$2 (:$1) never became ready"; tail -30 "$LOGS/$2"*.log; exit 1
}
for s in "${!PORT[@]}"; do
  wait_ready "${PORT[$s]}" "$s"
  [ "$s" != gateway ] && [ "$s" != transcoder ] && wait_ready $((PORT[$s] + 100)) "$s"
done
echo "== all $(( ${#PIDS[@]} - 1 )) processes ready"

[ "${ONLY_UP:-}" = 1 ] && { KEEP=1; exit 0; }

echo "== journey"
cd "$ROOT/lumo-infra/e2e"
LUMO_E2E_GATEWAY=http://localhost:18080 LUMO_E2E_PAYSTACK=http://localhost:19100 LUMO_E2E_PG="$PG" \
  LUMO_E2E_PAYSTACK_SECRET=sk_test_e2e LUMO_E2E_LOGS="$LOGS" go test ./journey -count=1 -v -timeout 10m ${TEST_ARGS:-}
