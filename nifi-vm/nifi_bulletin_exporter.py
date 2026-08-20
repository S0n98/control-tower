#!/usr/bin/env python3
"""
Polls NiFi's REST API bulletin board (not covered by PrometheusReportingTask) and
exposes bulletin counts as Prometheus metrics, so ERROR-level bulletins can page
and WARN-level ones can show up on a dashboard.

Run as a systemd service alongside Grafana Agent, which scrapes it on
EXPORTER_PORT (see grafana-agent-config.yaml, job "nifi-bulletin-exporter").

Requires: pip install prometheus_client requests
"""
import os
import time
import requests
from prometheus_client import start_http_server, Gauge, Counter

NIFI_API_BASE = os.environ.get("NIFI_API_BASE", "https://localhost:8443/nifi-api")
NIFI_CA_BUNDLE = os.environ.get("NIFI_CA_BUNDLE")  # path to CA cert if NiFi uses a private CA; None = verify with system CAs
NIFI_TOKEN = os.environ.get("NIFI_API_TOKEN")  # bearer token if NiFi API auth is enabled
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9622"))

bulletin_current = Gauge(
    "nifi_bulletin_current",
    "Bulletins currently on the board by level and source",
    ["level", "source_name", "group_id"],
)
bulletin_seen_total = Counter(
    "nifi_bulletin_seen_total",
    "Total bulletins observed since the exporter started, by level and source",
    ["level", "source_name", "group_id"],
)

_seen_ids = set()


def poll_once(session):
    resp = session.get(f"{NIFI_API_BASE}/flow/bulletin-board", timeout=10)
    resp.raise_for_status()
    bulletins = resp.json().get("bulletinBoard", {}).get("bulletins", [])

    current_counts = {}
    for b in bulletins:
        bulletin = b.get("bulletin", {})
        level = bulletin.get("level", "UNKNOWN")
        source_name = bulletin.get("sourceName", "unknown")
        group_id = bulletin.get("groupId", "unknown")
        key = (level, source_name, group_id)
        current_counts[key] = current_counts.get(key, 0) + 1

        bulletin_id = bulletin.get("id")
        if bulletin_id is not None and bulletin_id not in _seen_ids:
            _seen_ids.add(bulletin_id)
            bulletin_seen_total.labels(level=level, source_name=source_name, group_id=group_id).inc()

    # Reset gauges each poll so resolved bulletins drop back to zero instead of
    # staying stuck at their last observed count.
    bulletin_current.clear()
    for (level, source_name, group_id), count in current_counts.items():
        bulletin_current.labels(level=level, source_name=source_name, group_id=group_id).set(count)


def main():
    session = requests.Session()
    if NIFI_CA_BUNDLE:
        session.verify = NIFI_CA_BUNDLE
    if NIFI_TOKEN:
        session.headers["Authorization"] = f"Bearer {NIFI_TOKEN}"

    start_http_server(EXPORTER_PORT)
    print(f"nifi_bulletin_exporter listening on :{EXPORTER_PORT}, polling {NIFI_API_BASE} every {POLL_INTERVAL_SECONDS}s")

    while True:
        try:
            poll_once(session)
        except requests.RequestException as e:
            print(f"poll failed: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
