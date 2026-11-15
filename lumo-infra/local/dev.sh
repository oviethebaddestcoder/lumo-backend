#!/usr/bin/env bash
# Run the whole Lumo backend on your own computer, with demo data, in one go.
#
#   lumo-infra/local/dev.sh            # start (keeps your data between runs)
#   SEED=1 lumo-infra/local/dev.sh     # also load the demo creators, videos, gifts
#   RESET=1 lumo-infra/local/dev.sh    # wipe local data first
#   lumo-infra/local/dev.sh stop       # stop everything
#
# Needs: Go 1.26+, bash 4+ (macOS: brew install bash), Postgres 16 and Redis 7
# running on localhost (Homebrew, apt, or `docker compose -f
# lumo-infra/local/docker-compose.yml up -d postgres redis`), the psql and
# redis-cli commands, ffmpeg and openssl.
#
# Kafka, S3, Paystack and LiveKit are in-process stand-ins (the same ones the
# end-to-end test uses), so no cloud accounts are needed. Sign-in codes are
# printed in the identity log instead of being sent. The gateway listens on
# http://localhost:18080.
set -euo pipefail

if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "bash 4 or newer is needed (macOS ships 3.2): brew install bash, then run: $(brew --prefix 2>/dev/null || echo /opt/homebrew)/bin/bash $0" >&2
  exit 1
fi
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
export WORK=${WORK:-$HOME/.lumo-dev}
export PG_URL=${PG_URL:-postgres://postgres:postgres@localhost:5432}
export E2E_REDIS_URL=${REDIS_URL:-redis://localhost:6379/5}

if [ "${1:-}" = stop ]; then
  pkill -f "$WORK/bin/" 2>/dev/null && echo "stopped" || echo "nothing running"
  exit 0
fi

for cmd in go psql redis-cli ffmpeg openssl curl; do
  command -v "$cmd" >/dev/null || { echo "missing: $cmd (see the comment at the top of $0)" >&2; exit 1; }
done
psql "$PG_URL/postgres" -qc "SELECT 1" >/dev/null 2>&1 || { echo "can't reach Postgres at $PG_URL (set PG_URL=postgres://user:pass@host:5432)" >&2; exit 1; }
redis-cli -u "$E2E_REDIS_URL" PING >/dev/null 2>&1 || { echo "can't reach Redis at $E2E_REDIS_URL (set REDIS_URL)" >&2; exit 1; }

pkill -f "$WORK/bin/" 2>/dev/null || true
[ "${RESET:-}" = 1 ] && rm -f "$WORK/secrets.env" && export KEEP_DATA= || export KEEP_DATA=1
# Web app on :3000 and the Expo dev server / web build on :8081 may call the API.
export E2E_ORIGINS=${ORIGINS:-http://localhost:3000,http://localhost:8081}
export LOG_LEVEL=${LOG_LEVEL:-warn}
# Comfortable limits for one developer clicking around.
export RATE_PER_USER_MIN=6000 RATE_PER_IP_MIN=6000 RATE_AUTH_PER_IP_MIN=600

KEEP=1 ONLY_UP=1 "$ROOT/lumo-infra/e2e/run.sh"

if [ "${SEED:-}" = 1 ]; then
  echo "== demo data (creators, videos, gifts, DMs, a live room)"
  (cd "$ROOT/lumo-infra/e2e" && LUMO_E2E_DEMO=1 LUMO_E2E_GATEWAY=http://localhost:18080 LUMO_E2E_PAYSTACK=http://localhost:19100 \
    LUMO_E2E_PAYSTACK_SECRET=sk_test_e2e go test ./journey -run Demo -count=1 -timeout 10m) >"$WORK/logs/seed.log" 2>&1 \
    && echo "   done" || { echo "   seeding failed, see $WORK/logs/seed.log"; tail -20 "$WORK/logs/seed.log"; }
fi

cat <<MSG

Lumo backend is running.
  API gateway   http://localhost:18080
  Logs          $WORK/logs   (sign-in codes: grep "DEV OTP" $WORK/logs/identity-serve.log)
  Test logins   load1@e2e.test … load200@e2e.test, code 000000
                (with SEED=1: load1 = Ada's Kitchen, a creator; load5 = Chioma, a viewer; load6 = finance)

Web:     cd ../lumo-web, create .env.local with
           GATEWAY_URL=http://localhost:18080
           NEXT_PUBLIC_MEDIA_ORIGIN=http://localhost:19000
           NEXT_PUBLIC_UPLOAD_ORIGIN=http://localhost:19000
         then npm ci && npm run dev and open http://localhost:3000
Mobile:  cd ../lumo-mobile && npm ci && EXPO_PUBLIC_API_URL=http://<your computer's LAN IP>:18080 npx expo run:android
Stop:    $0 stop
MSG
