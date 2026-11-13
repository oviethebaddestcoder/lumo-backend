#!/usr/bin/env bash
# Creates one Neon project per database-owning service and prints the pooled
# (app) and direct (migrations) connection strings.
# Needs: neonctl (npm i -g neonctl) and `neonctl auth`. REGION e.g. aws-eu-central-1
# (measure latency from your users; see README "Regions").
set -euo pipefail
REGION="${REGION:-aws-eu-central-1}"
for svc in identity wallet live social media notifier moderation messaging; do
  name="lumo-$svc"
  echo "== $name"
  neonctl projects create --name "$name" --region-id "$REGION" --output json > "/tmp/$name.json"
  pid=$(python3 -c "import json;print(json.load(open('/tmp/$name.json'))['project']['id'])")
  neonctl databases create --project-id "$pid" --name "$svc" >/dev/null
  echo "DATABASE_URL=$(neonctl connection-string --project-id "$pid" --database-name "$svc" --pooled)"
  echo "DATABASE_URL_DIRECT=$(neonctl connection-string --project-id "$pid" --database-name "$svc")"
done
echo "Tip: add a read replica (neonctl branches add-compute <branch> --type read_only) for read-heavy services."
