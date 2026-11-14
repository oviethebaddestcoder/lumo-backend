#!/usr/bin/env bash
# Visual preview / UI smoke test of the whole product:
#   1. start every backend service (lumo-infra/e2e/run.sh) and seed demo data
#   2. build and start the web app (lumo-web)
#   3. build the mobile app (lumo-mobile) for the browser with react-native-web,
#      swapping device-only modules (LiveKit, Firebase push, keychain, native
#      sign-in SDKs) for stand-ins from mobile-web/
#   4. screenshot every screen and fail on any page error or CSP violation
#
#   WEB_DIR=../lumo-web MOBILE_DIR=../lumo-mobile lumo-infra/preview/run.sh
# Screenshots and index.json land in $OUT (default /tmp/lumo-preview/shots).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
WEB_DIR=$(cd "${WEB_DIR:-$ROOT/../lumo-web}" && pwd)
MOBILE_DIR=$(cd "${MOBILE_DIR:-$ROOT/../lumo-mobile}" && pwd)
WORK=${PREVIEW_WORK:-/tmp/lumo-preview}
export OUT=${OUT:-$WORK/shots}
REDIS=${E2E_REDIS_URL:-redis://localhost:6379/5}
mkdir -p "$WORK"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  pkill -f "/tmp/lumo-e2e/bin/" 2>/dev/null || true
}
trap cleanup EXIT

echo "== backend"
E2E_ORIGINS=http://localhost:3000,http://localhost:8090 KEEP=1 ONLY_UP=1 "$ROOT/lumo-infra/e2e/run.sh"
(cd "$ROOT/lumo-infra/e2e" && LUMO_E2E_DEMO=1 go test ./journey -run Demo -count=1 -v -timeout 10m) >"$WORK/demo.log" 2>&1 || { tail -30 "$WORK/demo.log"; exit 1; }
export VIDEO_ID=$(grep -o "share page: http://localhost:3000/v/[0-9a-f-]*" "$WORK/demo.log" | head -1 | sed 's|.*/v/||')
export CREATOR_ID=$(grep -o "creator Ada=[0-9a-f-]*" "$WORK/demo.log" | head -1 | cut -d= -f2)

echo "== web"
(cd "$WEB_DIR" && [ -d node_modules ] || npm ci --no-audit --no-fund)
(cd "$WEB_DIR" && GATEWAY_URL=http://localhost:18080 NEXT_PUBLIC_MEDIA_ORIGIN=http://localhost:19000 NEXT_PUBLIC_UPLOAD_ORIGIN=http://localhost:19000 \
  NEXT_TELEMETRY_DISABLED=1 npx next build >"$WORK/web-build.log" 2>&1) || { tail -30 "$WORK/web-build.log"; exit 1; }
(cd "$WEB_DIR" && GATEWAY_URL=http://localhost:18080 npx next start -p 3000 >"$WORK/web.log" 2>&1) & PIDS+=($!)

echo "== mobile app for the browser"
rm -rf "$WORK/mweb" && mkdir -p "$WORK/mweb"
(cd "$MOBILE_DIR" && git ls-files -z | xargs -0 -I{} cp --parents {} "$WORK/mweb/")
cd "$WORK/mweb"
npm ci --no-audit --no-fund --legacy-peer-deps >/dev/null 2>&1
REACT=$(node -p "require('./package.json').dependencies.react")
npm install --no-save --no-audit --no-fund --legacy-peer-deps react-native-web@~0.21.0 @expo/metro-runtime "react-dom@$REACT" >/dev/null 2>&1
cp "$HERE/mobile-web/metro.config.js" metro.config.js
mkdir -p preview-stubs && cp "$HERE"/mobile-web/*.js preview-stubs/ && rm preview-stubs/metro.config.js
sed -i "s|  web: { favicon: './assets/favicon.png' },|  web: { favicon: './assets/favicon.png', output: 'single', bundler: 'metro' },|" app.config.ts
EXPO_PUBLIC_API_URL=http://localhost:18080 EXPO_PUBLIC_WEB_DOMAIN=localhost:3000 npx expo export --platform web --output-dir "$WORK/mweb-dist" >"$WORK/mweb-build.log" 2>&1 \
  || { tail -30 "$WORK/mweb-build.log"; exit 1; }
node "$HERE/serve-spa.js" "$WORK/mweb-dist" 8090 & PIDS+=($!)
cd "$HERE"

for _ in $(seq 60); do curl -sf localhost:3000 >/dev/null && curl -sf localhost:8090 >/dev/null && break; sleep 1; done
# The demo seed just signed these accounts in; codes are limited to one a
# minute per address, so clear the test limiter keys (test stack only).
redis-cli -u "$REDIS" --scan --pattern "*otp:*" | xargs -r redis-cli -u "$REDIS" DEL >/dev/null

echo "== screenshots"
node "$HERE/shoot.mjs"
