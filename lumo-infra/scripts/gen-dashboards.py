#!/usr/bin/env python3
"""Generates lumo-infra/k8s/dashboards.yaml (Grafana "Lumo overview").

Edit ROWS, then run from the repo root:  python3 lumo-infra/scripts/gen-dashboards.py
Every alert in k8s/alerts.yaml has a matching panel here.
"""
import json
import pathlib

ROWS = [
    ("Availability", [
        ("Requests / s by service", 'sum by (service) (rate(http_requests_total[5m]))', "reqps"),
        ("5xx rate by route", 'sum by (service, route) (rate(http_requests_total{status=~"5.."}[5m])) / sum by (service, route) (rate(http_requests_total[5m]))', "percentunit"),
        ("p99 latency by route", 'histogram_quantile(0.99, sum by (le, service, route) (rate(http_request_duration_seconds_bucket[5m])))', "s"),
        ("Gateway upstream errors", 'sum by (upstream) (rate(gateway_upstream_errors_total[5m]))', "reqps"),
        ("Gateway denied (auth, rate limits, bad paths)", 'sum by (reason) (rate(gateway_denied_total[5m]))', "reqps"),
    ]),
    ("Events", [
        ("Outbox oldest unpublished event", 'max by (relay) (outbox_oldest_age_seconds)', "s"),
        ("Events consumed by group and result", 'sum by (group, result) (rate(kafka_events_consumed_total[5m]))', "ops"),
        ("Dead-lettered events", 'sum by (group, topic) (increase(kafka_events_dlq_total[1h]))', "short"),
        ("Consumer lag", 'sum by (consumergroup) (kafka_consumergroup_lag)', "short"),
        ("Handler p95 by group", 'histogram_quantile(0.95, sum by (le, group) (rate(kafka_event_handle_seconds_bucket[5m])))', "s"),
    ]),
    ("Money", [
        ("Gift coins / min by gift", 'sum by (gift) (rate(wallet_gift_coins_total[5m])) * 60', "short"),
        ("Cash-outs waiting", 'sum by (status) (wallet_payouts)', "short"),
        ("Oldest waiting cash-out", 'max by (status) (wallet_payout_oldest_seconds)', "s"),
        ("Ledger reconciliation problems", 'sum by (shard, kind) (wallet_reconcile_problems)', "short"),
        ("Paystack webhook responses", 'sum by (status) (rate(http_requests_total{service="svc-wallet", route=~".*webhooks/paystack.*"}[5m]))', "reqps"),
    ]),
    ("Abuse", [
        ("Sign-in codes by channel and result", 'sum by (channel, result) (rate(identity_otp_sent_total[5m])) * 60', "short"),
        ("SMS codes vs yesterday", 'sum(rate(identity_otp_sent_total{channel="sms", result="sent"}[10m])) / (sum(rate(identity_otp_sent_total{channel="sms", result="sent"}[10m] offset 1d)) + 0.001)', "short"),
    ]),
    ("Feed and content", [
        ("Completion rate by A/B arm", 'sum by (arm) (rate(feed_views_total{outcome="complete"}[1h])) / sum by (arm) (rate(feed_views_total[1h]))', "percentunit"),
        ("Skip rate by retrieval source", 'sum by (source) (rate(feed_views_total{outcome="skip"}[1h])) / sum by (source) (rate(feed_views_total[1h]))', "percentunit"),
        ("Videos served by source", 'sum by (source) (rate(feed_served_total[5m]))', "ops"),
        ("Ranking p95", 'histogram_quantile(0.95, sum by (le) (rate(feed_rank_seconds_bucket[5m])))', "s"),
        ("Viewer signals by kind", 'sum by (kind) (rate(feed_signals_total[5m]))', "ops"),
        ("Transcode jobs by result", 'sum by (result) (rate(transcode_jobs_total[15m]))', "ops"),
        ("Transcode p95", 'histogram_quantile(0.95, sum by (le) (rate(transcode_job_seconds_bucket[15m])))', "s"),
    ]),
    ("Live and realtime", [
        ("Live rooms by category", 'max by (category) (live_rooms_active)', "short"),
        ("WebSocket connections", 'sum(realtime_connections)', "short"),
        ("Messages delivered / s", 'sum(rate(realtime_messages_delivered_total[5m]))', "ops"),
        ("Slow clients dropped", 'sum(rate(realtime_slow_clients_total[5m]))', "ops"),
        ("Push notifications by result", 'sum by (result) (rate(push_sent_total[5m]))', "ops"),
    ]),
]


def build():
    panels, pid, y = [], 1, 0
    for title, ps in ROWS:
        panels.append({"type": "row", "title": title, "id": pid, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "collapsed": False})
        pid, y = pid + 1, y + 1
        for i, (t, expr, unit) in enumerate(ps):
            panels.append({
                "type": "timeseries", "title": t, "id": pid,
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "gridPos": {"h": 8, "w": 8, "x": (i % 3) * 8, "y": y + (i // 3) * 8},
                "targets": [{"expr": expr, "refId": "A", "legendFormat": "__auto"}],
                "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]}},
            })
            pid += 1
        y += ((len(ps) + 2) // 3) * 8
    return {
        "title": "Lumo overview", "uid": "lumo-overview", "schemaVersion": 39, "version": 1,
        "time": {"from": "now-6h", "to": "now"}, "refresh": "30s", "tags": ["lumo"],
        "templating": {"list": [{"name": "datasource", "type": "datasource", "query": "prometheus", "current": {}}]},
        "panels": panels,
    }


def main():
    body = json.dumps(build(), indent=2)
    out = (
        "# Grafana dashboard, loaded by the Grafana sidecar (label grafana_dashboard=1).\n"
        "# Generated: edit the panel list in lumo-infra/scripts/gen-dashboards.py and re-run.\n"
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lumo-dashboards\n  namespace: monitoring\n"
        '  labels: { grafana_dashboard: "1" }\ndata:\n  lumo-overview.json: |\n'
        + "\n".join("    " + line for line in body.splitlines()) + "\n"
    )
    pathlib.Path(__file__).resolve().parent.parent.joinpath("k8s", "dashboards.yaml").write_text(out)


if __name__ == "__main__":
    main()
