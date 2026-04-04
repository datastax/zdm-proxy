# ZDM Proxy Health Check Script

Lightweight health checker for the ZDM proxy that scrapes its built-in Prometheus metrics endpoint and alerts via Slack, PagerDuty, or both when things go wrong.

No external dependencies — runs on any system with Python 3.6+.

## What it monitors

The script scrapes the ZDM proxy's Prometheus endpoint (`/metrics` on port 14001 by default) every interval, compares counter values against the previous check, and alerts when deltas exceed thresholds.

When problems clear, PagerDuty incidents are automatically resolved. Slack alerts are rate-limited to one every 5 minutes to prevent alert storms during sustained outages.

### Monitored metrics and what they mean

**1. Failed writes — `zdm_proxy_failed_writes_total`**

This counter tracks writes that the proxy could not complete successfully. It has a `failed_on` label indicating which cluster caused the failure.

- `failed_on="target"` — The write succeeded on origin but failed on target. **This is the most common alert during migration.** It means the target cluster is struggling (node down, overloaded, CL not met) but your source of truth (origin) is fine. Data will be inconsistent until repaired.
- `failed_on="both"` — The write failed on both clusters. Usually means a query-level problem (bad schema, invalid CL for the topology) rather than a cluster health issue.
- `failed_on="origin"` — The write failed on origin but succeeded on target. Rare. Monitored separately (see below).

*Alert triggers when:* delta > `--failed-writes-threshold` (default: 5) per interval.

**2. Target error breakdown — `zdm_target_requests_failed_total`**

Per-node counters with an `error` label giving the specific Cassandra error type. The script monitors:

- `error="write_timeout"` — Target node accepted the write but couldn't replicate to enough nodes within the timeout. Typically means nodes in the target cluster are slow or down.
- `error="unavailable"` — Target coordinator knows there aren't enough live replicas to satisfy the consistency level before even attempting the write. Means nodes are down.
- `error="overloaded"` — Target node is rejecting requests because it's overwhelmed (too many pending requests, compaction backlog, etc).

*Alert triggers when:* delta > `--write-timeout-threshold` (default: 5) per interval.

**3. Origin failures — `zdm_origin_requests_failed_total`** (CRITICAL)

Same error types as target, but for origin. **Any origin failure is critical** because origin is the source of truth. The script alerts on any non-zero delta, with no threshold — even a single origin failure warrants investigation.

- `error="write_timeout"` — Origin can't replicate writes. Your production database is degraded.
- `error="unavailable"` — Origin doesn't have enough replicas. Active outage.

*Alert triggers when:* any increase at all.

**4. Connection failures — `zdm_{origin,target}_failed_connections_total`**

Tracks failed TCP connection attempts from the proxy to cluster nodes. A spike means the proxy can't reach the cluster — network partition, node crash, or firewall issue. This is a per-node metric summed across all nodes.

*Alert triggers when:* delta > 3 per interval.

### Alert severity levels

| Severity | When | What it means |
|----------|------|---------------|
| **critical** | Origin failures, metrics endpoint unreachable | Production is impacted right now |
| **warning** | Target failures, connection problems | Migration data flow is degraded but origin (source of truth) is fine |

### Why target failures matter even though origin is fine

The ZDM proxy waits for **both** clusters to respond before returning a result to the client. If the target is slow or failing, the proxy either returns the target's error to the client or times out waiting. This means **a target outage can cause client-visible errors even though origin writes succeed**. Monitoring target health is essential to avoid this cascading into a perceived production outage.

The `ZDM_TARGET_CONSISTENCY_LEVEL` config option (available in the AxonOps build) can reduce the impact by using a weaker CL on the target side, but it won't help if the target is completely unreachable.

## Quick start

```bash
# One-shot check, output to stdout only
python3 zdm-health-check.py --metrics-url http://zdm-proxy-host:14001/metrics

# Run every 60 seconds with Slack alerts
python3 zdm-health-check.py \
  --metrics-url http://zdm-proxy-host:14001/metrics \
  --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ \
  --interval 60

# Run every 60 seconds with PagerDuty alerts
python3 zdm-health-check.py \
  --metrics-url http://zdm-proxy-host:14001/metrics \
  --pagerduty-routing-key YOUR_INTEGRATION_KEY_HERE \
  --interval 60

# Both Slack and PagerDuty
python3 zdm-health-check.py \
  --metrics-url http://zdm-proxy-host:14001/metrics \
  --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ \
  --pagerduty-routing-key YOUR_INTEGRATION_KEY_HERE \
  --interval 60
```

## Configuration

Everything can be set via CLI flags or environment variables. CLI flags take precedence.

| CLI flag | Environment variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--metrics-url` | `ZDM_METRICS_URL` | `http://localhost:14001/metrics` | ZDM proxy Prometheus endpoint |
| `--interval` | `ZDM_CHECK_INTERVAL` | `0` (one-shot) | Seconds between checks |
| `--failed-writes-threshold` | `ZDM_FAILED_WRITES_THRESHOLD` | `5` | Alert if failed writes increase by more than this per interval |
| `--write-timeout-threshold` | `ZDM_WRITE_TIMEOUT_THRESHOLD` | `5` | Alert if target timeouts increase by more than this per interval |
| `--slack-webhook-url` | `ZDM_SLACK_WEBHOOK_URL` | *(none)* | Slack incoming webhook URL |
| `--pagerduty-routing-key` | `ZDM_PAGERDUTY_ROUTING_KEY` | *(none)* | PagerDuty Events API v2 integration/routing key |
| `--pagerduty-source` | `ZDM_PAGERDUTY_SOURCE` | `zdm-proxy` | Source field in PagerDuty events |

## PagerDuty setup

1. In PagerDuty, create a service (or use an existing one)
2. Add an integration of type **Events API v2**
3. Copy the **Integration Key** (this is your routing key)
4. Pass it as `--pagerduty-routing-key` or `ZDM_PAGERDUTY_ROUTING_KEY`

The script uses dedup keys based on the metrics URL, so repeated alerts for the same proxy won't create duplicate incidents. When problems clear, the incident is automatically resolved.

Severity mapping:
- **critical** — origin failures, metrics endpoint unreachable
- **warning** — target failures, connection problems

## Slack setup

1. Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in your Slack workspace
2. Copy the webhook URL
3. Pass it as `--slack-webhook-url` or `ZDM_SLACK_WEBHOOK_URL`

## Running as a systemd service

```ini
[Unit]
Description=ZDM Proxy Health Check
After=network.target

[Service]
Type=simple
Environment=ZDM_METRICS_URL=http://localhost:14001/metrics
Environment=ZDM_CHECK_INTERVAL=60
Environment=ZDM_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
Environment=ZDM_PAGERDUTY_ROUTING_KEY=your-key-here
ExecStart=/usr/bin/python3 /opt/zdm-proxy/scripts/zdm-health-check.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Example output

Normal operation:
```
[2026-04-03 12:30:00 UTC] OK | clients=42 inflight_writes=3 failed_writes: origin=0 target=0 both=0
[2026-04-03 12:31:00 UTC] OK | clients=42 inflight_writes=5 failed_writes: origin=0 target=0 both=0
```

When target starts failing:
```
[2026-04-03 12:32:00 UTC] OK | clients=42 inflight_writes=2 failed_writes: origin=0 target=12 both=0
  ALERT [warning]: Failed writes (failed_on=target): +12 in last interval (total: 12)
  ALERT [warning]: Target write_timeout: +12 in last interval (total: 12)
  PagerDuty alert triggered: zdm-proxy-health-http://localhost:14001/metrics
```

When it recovers:
```
[2026-04-03 12:35:00 UTC] OK | clients=42 inflight_writes=4 failed_writes: origin=0 target=12 both=0
  PagerDuty incident resolved
```
