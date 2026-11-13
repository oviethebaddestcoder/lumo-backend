#!/usr/bin/env bash
# Create all topics (and their .dlq) from kafka/topics.yaml.
# Usage: KAFKA_BROKERS=host:9092 [RF=1] scripts/create-topics.sh
# Uses `rpk` (Redpanda CLI) if present, else `kafka-topics.sh`.
set -euo pipefail
cd "$(dirname "$0")/.."
BROKERS="${KAFKA_BROKERS:-localhost:9092}"
RF="${RF:-3}"
python3 - "$BROKERS" "$RF" <<'PY'
import subprocess, sys, shutil, yaml
brokers, rf = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open("kafka/topics.yaml"))
base = dict(cfg["defaults"]["configs"])
if rf == "1":
    base["min.insync.replicas"] = "1"
def create(name, parts, configs):
    if shutil.which("rpk"):
        cmd = ["rpk", "topic", "create", name, "-p", str(parts), "-r", rf, "-X", f"brokers={brokers}"]
        for k, v in configs.items(): cmd += ["-c", f"{k}={v}"]
    else:
        cmd = ["kafka-topics.sh", "--bootstrap-server", brokers, "--create", "--if-not-exists", "--topic", name,
               "--partitions", str(parts), "--replication-factor", rf]
        for k, v in configs.items(): cmd += ["--config", f"{k}={v}"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(name, "ok" if r.returncode == 0 else (r.stdout + r.stderr).strip().splitlines()[-1])
for t in cfg["topics"]:
    create(t["name"], t["partitions"], {**base, **t.get("configs", {})})
    create(t["name"] + ".dlq", 3, {**base, "retention.ms": "1209600000"})
PY
