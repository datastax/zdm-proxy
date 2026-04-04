#!/usr/bin/env python3
"""
ZDM Proxy health check script.

Scrapes the Prometheus metrics endpoint, checks for signs of trouble,
and sends alerts to Slack and/or PagerDuty when thresholds are breached.
No external dependencies — stdlib Python 3 only.

Usage:
    # One-shot check (stdout only)
    python3 zdm-health-check.py

    # Run every 60 seconds with Slack alerts
    python3 zdm-health-check.py --interval 60 --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ

    # Run with PagerDuty alerts
    python3 zdm-health-check.py --interval 60 --pagerduty-routing-key abc123...

    # Both Slack and PagerDuty
    python3 zdm-health-check.py --interval 60 \
        --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ \
        --pagerduty-routing-key abc123...

    # Custom thresholds
    python3 zdm-health-check.py --interval 60 --failed-writes-threshold 10 --write-timeout-threshold 10

Environment variables (all optional, CLI args take precedence):
    ZDM_METRICS_URL                - metrics endpoint (default: http://localhost:14001/metrics)
    ZDM_CHECK_INTERVAL             - seconds between checks when running in loop mode
    ZDM_FAILED_WRITES_THRESHOLD    - alert if failed writes increase by more than this per interval (default: 5)
    ZDM_WRITE_TIMEOUT_THRESHOLD    - alert if target write timeouts increase by more than this per interval (default: 5)
    ZDM_WRITE_DIVERGENCE_THRESHOLD - alert if origin/target per-table write counts differ by more than this (default: 0)
    ZDM_SLACK_WEBHOOK_URL          - Slack incoming webhook URL
    ZDM_PAGERDUTY_ROUTING_KEY      - PagerDuty Events API v2 integration/routing key
    ZDM_PAGERDUTY_SOURCE           - source field for PagerDuty events (default: zdm-proxy)
"""

import argparse
import json
import os
import signal
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

MAX_RESPONSE_BYTES = 1024 * 1024  # 1MB safety cap on metrics response
SLACK_COOLDOWN_SECONDS = 300      # Don't send more than one Slack alert per 5 minutes


# ---------------------------------------------------------------------------
# Metrics parsing
# ---------------------------------------------------------------------------

def parse_prometheus_text(text):
    """Parse Prometheus text exposition format into {metric_name{labels}: value}."""
    metrics = {}
    for line in text.strip().split("\n"):
        if line.startswith("#") or not line.strip():
            continue
        try:
            parts = line.rsplit(" ", 1)
            metrics[parts[0].strip()] = float(parts[1].strip())
        except (IndexError, ValueError):
            continue
    return metrics


def fetch_metrics(url):
    """Fetch and parse metrics from the ZDM proxy. Returns (dict, error_string)."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "text/plain"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read(MAX_RESPONSE_BYTES)
            return parse_prometheus_text(body.decode("utf-8")), None
    except urllib.error.URLError as e:
        return None, f"Cannot reach metrics endpoint {url}: {e}"
    except Exception as e:
        return None, f"Error fetching metrics from {url}: {e}"


def get_metric(metrics, name, default=0.0):
    return metrics.get(name, default)


def sum_metrics_matching(metrics, prefix, label_filter):
    """Sum all metric values whose key starts with prefix and contains label_filter."""
    return sum(v for k, v in metrics.items() if k.startswith(prefix) and label_filter in k)


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def check_health(metrics, prev_metrics, config):
    """Compare current vs previous metrics snapshot. Returns list of (severity, message) tuples."""
    problems = []

    if metrics is None:
        return [("critical", "Metrics endpoint is unreachable - proxy may be down")]

    def delta(current, previous):
        d = current - previous
        if d < 0:
            # Counter reset (proxy restarted) — skip this interval
            return 0
        return d

    # Failed writes on target / both
    for label in ["target", "both"]:
        key = f'zdm_proxy_failed_writes_total{{failed_on="{label}"}}'
        cur = get_metric(metrics, key)
        prev = get_metric(prev_metrics, key) if prev_metrics else cur
        d = delta(cur, prev)
        if d > config["failed_writes_threshold"]:
            problems.append(("warning", f"Failed writes (failed_on={label}): +{int(d)} in last interval (total: {int(cur)})"))

    # Target error types
    for error_type in ["write_timeout", "unavailable", "overloaded"]:
        cur = sum_metrics_matching(metrics, "zdm_target_requests_failed_total", f'error="{error_type}"')
        prev = sum_metrics_matching(prev_metrics, "zdm_target_requests_failed_total", f'error="{error_type}"') if prev_metrics else cur
        d = delta(cur, prev)
        if d > config["write_timeout_threshold"]:
            problems.append(("warning", f"Target {error_type}: +{int(d)} in last interval (total: {int(cur)})"))

    # Origin failures — any at all is critical
    for error_type in ["write_timeout", "unavailable"]:
        cur = sum_metrics_matching(metrics, "zdm_origin_requests_failed_total", f'error="{error_type}"')
        prev = sum_metrics_matching(prev_metrics, "zdm_origin_requests_failed_total", f'error="{error_type}"') if prev_metrics else cur
        d = delta(cur, prev)
        if d > 0:
            problems.append(("critical", f"ORIGIN {error_type}: +{int(d)} in last interval (total: {int(cur)})"))

    # Connection failures
    for cluster in ["origin", "target"]:
        cur = sum_metrics_matching(metrics, f"zdm_{cluster}_failed_connections_total", "")
        prev = sum_metrics_matching(prev_metrics, f"zdm_{cluster}_failed_connections_total", "") if prev_metrics else cur
        d = delta(cur, prev)
        if d > 3:
            problems.append(("warning", f"{cluster.title()} connection failures: +{int(d)} in last interval"))

    # Per-table write divergence: compare origin vs target successful writes
    # If origin and target counts differ for a table, data may be diverging
    write_counts = parse_per_table_writes(metrics)
    for table_key, counts in write_counts.items():
        origin_count = counts.get("origin", 0)
        target_count = counts.get("target", 0)
        if origin_count > 0 and target_count == 0:
            problems.append(("critical",
                f"Write divergence on {table_key}: origin={int(origin_count)} target=0 — target may be down"))
        elif origin_count != target_count:
            diff = abs(origin_count - target_count)
            if diff > config["write_divergence_threshold"]:
                problems.append(("warning",
                    f"Write divergence on {table_key}: origin={int(origin_count)} target={int(target_count)} (diff={int(diff)})"))

    return problems


def parse_per_table_writes(metrics):
    """Parse zdm_proxy_write_success_total metrics into {keyspace.table: {origin: N, target: N}}."""
    result = {}
    prefix = "zdm_proxy_write_success_total{"
    for key, value in metrics.items():
        if not key.startswith(prefix):
            continue
        # Extract labels from key like: zdm_proxy_write_success_total{cluster="origin",keyspace="ks",table="t"}
        labels_str = key[len(prefix):-1]  # strip prefix and trailing }
        labels = {}
        for part in labels_str.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                labels[k.strip()] = v.strip().strip('"')
        cluster = labels.get("cluster", "")
        keyspace = labels.get("keyspace", "")
        table = labels.get("table", "")
        if cluster and (keyspace or table):
            table_key = f"{keyspace}.{table}" if keyspace else table
            if table_key not in result:
                result[table_key] = {}
            result[table_key][cluster] = value
    return result


# ---------------------------------------------------------------------------
# Alerting — Slack
# ---------------------------------------------------------------------------

_last_slack_alert_time = 0.0


def send_slack(webhook_url, problems, metrics_url):
    global _last_slack_alert_time
    if not webhook_url:
        return

    # Rate-limit Slack messages to avoid alert storms during sustained outages
    elapsed = time.monotonic() - _last_slack_alert_time
    if _last_slack_alert_time > 0 and elapsed < SLACK_COOLDOWN_SECONDS:
        print(f"  Slack alert suppressed (cooldown, {int(SLACK_COOLDOWN_SECONDS - elapsed)}s remaining)")
        return

    worst = "critical" if any(s == "critical" for s, _ in problems) else "warning"
    emoji = ":rotating_light:" if worst == "critical" else ":warning:"

    text = f"{emoji} *ZDM Proxy Alert*\n*Host:* `{metrics_url}`\n*Time:* {now()}\n"
    for severity, msg in problems:
        icon = ":red_circle:" if severity == "critical" else ":large_orange_circle:"
        text += f"{icon} {msg}\n"

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=payload,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 204):
                print(f"  Slack webhook returned status {resp.status}", file=sys.stderr)
            else:
                _last_slack_alert_time = time.monotonic()
    except Exception as e:
        print(f"  Failed to send Slack alert: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Alerting — PagerDuty Events API v2
# ---------------------------------------------------------------------------

PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"


def send_pagerduty(routing_key, source, problems, metrics_url):
    if not routing_key:
        return

    worst = "critical" if any(s == "critical" for s, _ in problems) else "warning"

    summary_parts = []
    for _, msg in problems:
        summary_parts.append(msg)
    summary = f"ZDM Proxy: {'; '.join(summary_parts)}"
    # PagerDuty summary max 1024 chars
    if len(summary) > 1024:
        summary = summary[:1021] + "..."

    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "dedup_key": f"zdm-proxy-health-{metrics_url}",
        "payload": {
            "summary": summary,
            "source": source,
            "severity": worst,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "component": "zdm-proxy",
            "group": "cassandra-migration",
            "class": "health-check",
            "custom_details": {
                "metrics_url": metrics_url,
                "problems": [{"severity": s, "message": m} for s, m in problems],
            },
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(PAGERDUTY_EVENTS_URL, data=data,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            if resp.status == 202:
                result = json.loads(body)
                print(f"  PagerDuty alert triggered: {result.get('dedup_key', 'unknown')}")
            else:
                print(f"  PagerDuty returned status {resp.status}: {body}", file=sys.stderr)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  PagerDuty error {e.code}: {body}", file=sys.stderr)
    except Exception as e:
        print(f"  Failed to send PagerDuty alert: {e}", file=sys.stderr)


def resolve_pagerduty(routing_key, metrics_url):
    """Send a resolve event to auto-close the PagerDuty incident when things recover."""
    if not routing_key:
        return

    payload = {
        "routing_key": routing_key,
        "event_action": "resolve",
        "dedup_key": f"zdm-proxy-health-{metrics_url}",
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(PAGERDUTY_EVENTS_URL, data=data,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"  PagerDuty incident resolved")
    except Exception as e:
        print(f"  Failed to resolve PagerDuty incident: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(metrics):
    if metrics is None:
        print(f"[{now()}] UNREACHABLE")
        return

    writes_target = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="target"}')
    writes_origin = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="origin"}')
    writes_both = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="both"}')
    clients = get_metric(metrics, "zdm_client_connections_total")
    inflight = get_metric(metrics, 'zdm_proxy_inflight_requests_total{type="writes"}')

    print(
        f"[{now()}] OK | "
        f"clients={int(clients)} "
        f"inflight_writes={int(inflight)} "
        f"failed_writes: origin={int(writes_origin)} target={int(writes_target)} both={int(writes_both)}"
    )


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ZDM Proxy health checker with Slack and PagerDuty alerting")

    parser.add_argument("--metrics-url",
                        default=os.environ.get("ZDM_METRICS_URL", "http://localhost:14001/metrics"),
                        help="ZDM proxy metrics endpoint")
    parser.add_argument("--interval", type=int,
                        default=int(os.environ.get("ZDM_CHECK_INTERVAL", "0")),
                        help="Seconds between checks. 0 = one-shot.")
    parser.add_argument("--failed-writes-threshold", type=int,
                        default=int(os.environ.get("ZDM_FAILED_WRITES_THRESHOLD", "5")),
                        help="Alert if failed writes increase by more than this per interval")
    parser.add_argument("--write-timeout-threshold", type=int,
                        default=int(os.environ.get("ZDM_WRITE_TIMEOUT_THRESHOLD", "5")),
                        help="Alert if target timeouts increase by more than this per interval")
    parser.add_argument("--write-divergence-threshold", type=int,
                        default=int(os.environ.get("ZDM_WRITE_DIVERGENCE_THRESHOLD", "0")),
                        help="Alert if origin/target per-table write counts differ by more than this")

    # Slack
    parser.add_argument("--slack-webhook-url",
                        default=os.environ.get("ZDM_SLACK_WEBHOOK_URL", ""),
                        help="Slack incoming webhook URL")

    # PagerDuty
    parser.add_argument("--pagerduty-routing-key",
                        default=os.environ.get("ZDM_PAGERDUTY_ROUTING_KEY", ""),
                        help="PagerDuty Events API v2 integration/routing key")
    parser.add_argument("--pagerduty-source",
                        default=os.environ.get("ZDM_PAGERDUTY_SOURCE", "zdm-proxy"),
                        help="Source field for PagerDuty events (default: zdm-proxy)")

    args = parser.parse_args()

    config = {
        "failed_writes_threshold": args.failed_writes_threshold,
        "write_timeout_threshold": args.write_timeout_threshold,
        "write_divergence_threshold": args.write_divergence_threshold,
    }

    has_alerting = bool(args.slack_webhook_url or args.pagerduty_routing_key)
    if not has_alerting:
        print("Warning: no alerting configured. Set --slack-webhook-url and/or --pagerduty-routing-key for alerts.",
              file=sys.stderr)

    # Clean shutdown on SIGINT/SIGTERM
    def handle_signal(signum, frame):
        print(f"\n[{now()}] Shutting down.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    prev_metrics = None
    was_alerting = False

    while True:
        metrics, err = fetch_metrics(args.metrics_url)

        if err:
            print(f"[{now()}] ERROR: {err}", file=sys.stderr)
            problems = [("critical", err)]
        else:
            print_summary(metrics)
            problems = check_health(metrics, prev_metrics, config)

        if problems:
            for severity, msg in problems:
                print(f"  ALERT [{severity}]: {msg}")
            send_slack(args.slack_webhook_url, problems, args.metrics_url)
            send_pagerduty(args.pagerduty_routing_key, args.pagerduty_source, problems, args.metrics_url)
            was_alerting = True
        elif was_alerting:
            # Problems cleared — resolve PagerDuty incident
            resolve_pagerduty(args.pagerduty_routing_key, args.metrics_url)
            was_alerting = False

        prev_metrics = metrics

        if args.interval <= 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
