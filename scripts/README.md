# ZDM Proxy Health Check Script

Lightweight health checker for the ZDM proxy that scrapes its built-in Prometheus metrics endpoint and alerts via Slack, PagerDuty, or both when things go wrong.

No external dependencies — runs on any system with Python 3.6+.

## What it monitors

The script compares metrics between consecutive checks and alerts when:

- **Failed writes on target** increase by more than the threshold (default: 5 per interval)
- **Target write timeouts / unavailable / overloaded errors** spike
- **Any origin failures at all** — this is always critical, origin should never fail
- **Connection failures** to either cluster (more than 3 per interval)

When problems clear, PagerDuty incidents are automatically resolved.

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
