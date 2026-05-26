# Alerting Integration

SimpleETL provides an alerting framework with pluggable notification channels.
Define rules that are evaluated against job context, and dispatch alerts via
webhooks, Slack, or email stubs.

## Overview

The alerting system consists of three main components:

1. **AlertRule**: Defines a condition and message for an alert
2. **AlertChannel**: A notification channel (webhook, Slack, email)
3. **AlertManager**: Manages rules and evaluates them against context

## Quick Start

```python
from simpleetl.core.lineage import (
    AlertRule,
    AlertManager,
    WebhookChannel,
    SlackChannel,
)

# Create an alert rule
rule = AlertRule(
    name="row_count_drop",
    severity="critical",
    message_template="Row count dropped below threshold: {row_count}",
    condition=lambda ctx: ctx.get("row_count", 0) < 1000,
)

# Configure channels
rule.channel_instances = [
    WebhookChannel(url="https://hooks.example.com/alerts"),
    SlackChannel(webhook_url="https://hooks.slack.com/services/..."),
]

# Add rule to manager
manager = AlertManager()
manager.add_rule(rule)

# Evaluate rules (typically at the end of a job)
results = manager.check_and_dispatch({"row_count": 500})
for r in results:
    print(f"Alert: {r['rule']} — Dispatched: {r['dispatch_results']}")
```

## AlertRule

An `AlertRule` defines when and how to alert:

```python
@dataclass
class AlertRule:
    name: str                                    # Human-readable name
    condition: Callable[[Dict[str, Any]], bool]  # Returns True to trigger
    severity: str = "warning"                    # "warning" or "critical"
    message_template: str = "Alert '{name}' triggered"
    channels: List[str] = []                     # Channel names
    channel_instances: List[AlertChannel] = []   # Actual channel objects
```

### Methods

- **`evaluate(context)`** → `Optional[str]`: Tests the condition against context.
  Returns the formatted message if triggered, `None` otherwise.
- **`dispatch(message)`** → `Dict[str, bool]`: Sends the alert through all
  channel instances. Returns dispatch results.

### Example Rules

```python
# Row count drop
AlertRule(
    name="low_row_count",
    condition=lambda ctx: ctx.get("row_count", 0) < 100,
    severity="warning",
    message_template="Only {row_count} rows processed (expected > 100)",
)

# Processing time exceeded
AlertRule(
    name="slow_job",
    condition=lambda ctx: ctx.get("duration_seconds", 0) > 3600,
    severity="critical",
    message_template="Job exceeded 1 hour: {duration_seconds}s",
)

# Error rate check
AlertRule(
    name="high_error_rate",
    condition=lambda ctx: ctx.get("error_rate", 0) > 0.05,
    severity="critical",
    message_template="Error rate is {error_rate:.1%}",
)
```

## Alert Channels

### WebhookChannel

Sends alerts via HTTP POST with JSON payload:

```python
channel = WebhookChannel(
    url="https://hooks.example.com/etl-alerts",
    timeout=10.0,
)

success = channel.send(
    message="Row count dropped to 500",
    severity="critical",
    rule_name="low_row_count",
)
```

The JSON payload format:

```json
{
  "message": "Row count dropped to 500",
  "severity": "critical",
  "rule": "low_row_count",
  "source": "simpleetl"
}
```

### SlackChannel

Sends formatted alerts to Slack via incoming webhooks:

```python
channel = SlackChannel(
    webhook_url="https://hooks.slack.com/services/T00/B00/token",
)

success = channel.send(
    message="Job completed with warnings",
    severity="warning",
    rule_name="data_quality_check",
)
```

Features:
- **Color coding**: Critical alerts are red (`#ff0000`), warnings are orange (`#ffaa00`)
- **Rich formatting**: Includes severity and source fields
- **Attachments format**: Uses Slack's message attachments for better display

### EmailChannel

Stub implementation that logs alerts (actual SMTP delivery is platform-specific):

```python
channel = EmailChannel(
    recipients=["team@example.com", "oncall@example.com"],
    smtp_host="smtp.example.com",
)

success = channel.send(
    message="Alert: data quality issue detected",
    severity="warning",
    rule_name="quality_check",
)
```

The email channel logs alerts via `logging.info`:
```
[EMAIL ALERT] To: ['team@example.com'] | Severity: warning | Rule: quality_check | Message: Alert: data quality issue detected
```

## AlertManager

Manages a collection of alert rules:

```python
manager = AlertManager()

# Add rules
manager.add_rule(AlertRule(...))
manager.add_rule(AlertRule(...))

# Evaluate rules (legacy method - returns messages)
messages = manager.check_alerts({"row_count": 50})

# Evaluate and dispatch (preferred - sends notifications)
results = manager.check_and_dispatch({"row_count": 50})
# Returns: [
#   {
#     "rule": "low_row_count",
#     "message": "Row count dropped",
#     "severity": "warning",
#     "dispatch_results": {"WebhookChannel": True, "SlackChannel": False}
#   }
# ]

# Clear all rules
manager.clear_rules()
```

## Integration with ETL Jobs

Use alerting in your ETL job by checking conditions at the end:

```python
from simpleetl.core.job import ETLJob
from simpleetl.core.lineage import AlertManager, AlertRule, WebhookChannel

job = ETLJob(config)

# Set up alerting
manager = AlertManager()
rule = AlertRule(
    name="data_quality",
    condition=lambda ctx: ctx.get("output_rows", 0) == 0,
    severity="critical",
    message_template="No rows produced by job {job_name}",
    channel_instances=[WebhookChannel(url="https://hooks.example.com/alerts")],
)
manager.add_rule(rule)

job.run()

# Check alerts after execution
context = {
    "job_name": config.name,
    "output_rows": job.output_rows,
    "duration_seconds": job.duration,
}
results = manager.check_and_dispatch(context)

if results:
    for r in results:
        print(f"Alert fired: {r['rule']} (severity: {r['severity']})")
```

## Custom Channels

Extend `AlertChannel` to create custom notification channels:

```python
from simpleetl.core.lineage import AlertChannel

class PagerDutyChannel(AlertChannel):
    def __init__(self, routing_key: str) -> None:
        self.routing_key = routing_key

    def send(self, message: str, severity: str, rule_name: str) -> bool:
        import json
        import urllib.request

        payload = json.dumps({
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": message,
                "severity": "critical" if severity == "critical" else "warning",
                "source": "simpleetl",
            }
        }).encode()

        req = urllib.request.Request(
            "https://events.pagerduty.com/v2/enqueue",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 202
        except Exception:
            return False
```
