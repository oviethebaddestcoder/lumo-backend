#!/usr/bin/env bash
# Generates every secret the platform needs and prints them as env lines.
# Store the output in your secret manager (never commit it).
set -euo pipefail
rand() { openssl rand -base64 32 | tr -d '\n'; }
KEY_PEM=$(openssl genpkey -algorithm ed25519)
KID="k$(date +%Y%m%d)"
echo "# svc-identity"
echo "SIGNING_KEYS=${KID}:$(printf '%s' "$KEY_PEM" | base64 | tr -d '\n')"
echo "OTP_HMAC_SECRET=$(rand)"
echo "# svc-wallet"
echo "ACCOUNT_TAG_SECRET=$(rand)"
echo "# shared by svc-social, svc-feed, svc-notifier"
echo "INTERNAL_TOKEN=$(openssl rand -hex 32)"
echo "# LiveKit (also put these in livekit.yaml)"
echo "LIVEKIT_API_KEY=API$(openssl rand -hex 6)"
echo "LIVEKIT_API_SECRET=$(openssl rand -base64 36 | tr -d '\n/+=')"
