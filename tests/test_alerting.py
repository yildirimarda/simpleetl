"""
Tests for the alerting integration hooks in the lineage module.
"""

import json
import logging
from unittest.mock import patch, MagicMock

from simpleetl.core.lineage import (
    AlertChannel,
    AlertManager,
    AlertRule,
    EmailChannel,
    SlackChannel,
    WebhookChannel,
)


# ---------------------------------------------------------------------------
# WebhookChannel tests
# ---------------------------------------------------------------------------


class TestWebhookChannel:
    """Test WebhookChannel HTTP POST behavior."""

    def test_send_success(self, capsys):
        """Test successful webhook POST returns True."""
        channel = WebhookChannel(url="http://example.com/webhook")
        mock_response = MagicMock()
        mock_response.status = 200

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = MagicMock(
                return_value=mock_response
            )
            mock_urlopen.return_value.__exit__ = MagicMock(
                return_value=False
            )
            result = channel.send("test message", "critical", "rule1")

        assert result is True
        # Verify URL and method
        call_args = mock_urlopen.call_args
        req = call_args.args[0]
        assert req.get_method() == "POST"
        assert req.full_url == "http://example.com/webhook"
        assert req.get_header("Content-type") == "application/json"

    def test_send_non_200_status_returns_false(self):
        """Test that non-200 status returns False."""
        channel = WebhookChannel(url="http://example.com/webhook")
        mock_response = MagicMock()
        mock_response.status = 500

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = MagicMock(
                return_value=mock_response
            )
            mock_urlopen.return_value.__exit__ = MagicMock(
                return_value=False
            )
            result = channel.send("test", "warning", "rule1")

        assert result is False

    def test_send_connection_error_returns_false(self):
        """Test that connection errors return False."""
        channel = WebhookChannel(url="http://example.com/webhook")

        with patch(
            "urllib.request.urlopen", side_effect=Exception("Connection refused")
        ):
            result = channel.send("test", "warning", "rule1")

        assert result is False

    def test_send_payload_format(self):
        """Test that payload contains expected JSON structure."""
        channel = WebhookChannel(url="http://example.com/webhook")
        mock_response = MagicMock()
        mock_response.status = 200
        captured_data = {}

        def capture_call(req, timeout):
            captured_data["body"] = req.data
            captured_data["timeout"] = timeout
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=mock_response)
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=capture_call):
            channel.send("hello world", "critical", "my_rule")

        payload = json.loads(captured_data["body"])
        assert payload["message"] == "hello world"
        assert payload["severity"] == "critical"
        assert payload["rule"] == "my_rule"
        assert payload["source"] == "simpleetl"
        assert captured_data["timeout"] == 10.0

    def test_send_custom_timeout(self):
        """Test that custom timeout is passed to urlopen."""
        channel = WebhookChannel(url="http://example.com/wh", timeout=5.0)
        mock_response = MagicMock()
        mock_response.status = 200

        def capture_call(req, timeout):
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=mock_response)
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch(
            "urllib.request.urlopen", side_effect=capture_call
        ) as mock_urlopen:
            channel.send("msg", "warning", "r1")

        assert mock_urlopen.call_args.kwargs["timeout"] == 5.0


# ---------------------------------------------------------------------------
# SlackChannel tests
# ---------------------------------------------------------------------------


class TestSlackChannel:
    """Test SlackChannel webhook behavior."""

    def test_send_success(self):
        """Test successful Slack webhook POST returns True."""
        channel = SlackChannel(webhook_url="https://hooks.slack.com/test")
        mock_response = MagicMock()
        mock_response.status = 200

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = MagicMock(
                return_value=mock_response
            )
            mock_urlopen.return_value.__exit__ = MagicMock(
                return_value=False
            )
            result = channel.send("test msg", "warning", "rule1")

        assert result is True

    def test_send_error_returns_false(self):
        """Test that errors return False."""
        channel = SlackChannel(webhook_url="https://hooks.slack.com/test")

        with patch(
            "urllib.request.urlopen", side_effect=Exception("Network error")
        ):
            result = channel.send("test", "warning", "rule1")

        assert result is False

    def test_payload_critical_color(self):
        """Test that critical severity uses red color."""
        channel = SlackChannel(webhook_url="https://hooks.slack.com/test")
        mock_response = MagicMock()
        mock_response.status = 200
        captured_data = {}

        def capture_call(req, timeout):
            captured_data["body"] = req.data
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=mock_response)
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=capture_call):
            channel.send("alert msg", "critical", "crit_rule")

        payload = json.loads(captured_data["body"])
        attachment = payload["attachments"][0]
        assert attachment["color"] == "#ff0000"
        assert attachment["title"] == "SimpleETL Alert: crit_rule"
        assert attachment["text"] == "alert msg"

    def test_payload_warning_color(self):
        """Test that warning severity uses orange color."""
        channel = SlackChannel(webhook_url="https://hooks.slack.com/test")
        mock_response = MagicMock()
        mock_response.status = 200
        captured_data = {}

        def capture_call(req, timeout):
            captured_data["body"] = req.data
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=mock_response)
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=capture_call):
            channel.send("alert msg", "warning", "warn_rule")

        payload = json.loads(captured_data["body"])
        attachment = payload["attachments"][0]
        assert attachment["color"] == "#ffaa00"

    def test_payload_has_fields(self):
        """Test that Slack payload includes severity and source fields."""
        channel = SlackChannel(webhook_url="https://hooks.slack.com/test")
        mock_response = MagicMock()
        mock_response.status = 200
        captured_data = {}

        def capture_call(req, timeout):
            captured_data["body"] = req.data
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=mock_response)
            m.__exit__ = MagicMock(return_value=False)
            return m

        with patch("urllib.request.urlopen", side_effect=capture_call):
            channel.send("msg", "critical", "r1")

        payload = json.loads(captured_data["body"])
        fields = payload["attachments"][0]["fields"]
        field_titles = {f["title"] for f in fields}
        assert "Severity" in field_titles
        assert "Source" in field_titles
        severity_field = next(f for f in fields if f["title"] == "Severity")
        assert severity_field["value"] == "critical"
        source_field = next(f for f in fields if f["title"] == "Source")
        assert source_field["value"] == "simpleetl"


# ---------------------------------------------------------------------------
# EmailChannel tests
# ---------------------------------------------------------------------------


class TestEmailChannel:
    """Test EmailChannel stub behavior."""

    def test_send_returns_true(self):
        """Test that EmailChannel.send always returns True."""
        channel = EmailChannel(recipients=["admin@example.com"])
        result = channel.send("test message", "warning", "rule1")
        assert result is True

    def test_send_logs_message(self, caplog):
        """Test that EmailChannel logs the alert message."""
        channel = EmailChannel(recipients=["admin@example.com", "ops@example.com"])
        with caplog.at_level(logging.INFO):
            channel.send("alert body", "critical", "crit_rule")
        assert "[EMAIL ALERT]" in caplog.text
        assert "admin@example.com" in caplog.text
        assert "ops@example.com" in caplog.text
        assert "critical" in caplog.text
        assert "crit_rule" in caplog.text
        assert "alert body" in caplog.text

    def test_send_default_smtp_host(self):
        """Test default SMTP host is localhost."""
        channel = EmailChannel(recipients=["a@b.com"])
        assert channel.smtp_host == "localhost"

    def test_send_custom_smtp_host(self):
        """Test custom SMTP host configuration."""
        channel = EmailChannel(recipients=["a@b.com"], smtp_host="smtp.example.com")
        assert channel.smtp_host == "smtp.example.com"


# ---------------------------------------------------------------------------
# AlertRule.evaluate tests
# ---------------------------------------------------------------------------


class TestAlertRuleEvaluate:
    """Test AlertRule.evaluate method."""

    def test_evaluate_returns_message_when_condition_true(self):
        """Test evaluate returns formatted message when condition is True."""
        rule = AlertRule(
            name="test_rule",
            condition=lambda ctx: True,
            severity="warning",
            message_template="{name}: {count} items",
        )
        result = rule.evaluate({"count": 5})
        assert result == "test_rule: 5 items"

    def test_evaluate_returns_none_when_condition_false(self):
        """Test evaluate returns None when condition is False."""
        rule = AlertRule(
            name="test_rule",
            condition=lambda ctx: False,
        )
        result = rule.evaluate({})
        assert result is None

    def test_evaluate_uses_context_values(self):
        """Test evaluate formats message with context values."""
        rule = AlertRule(
            name="err_check",
            condition=lambda ctx: ctx.get("errors", 0) > 0,
            message_template="{name}: {errors} errors found",
            severity="critical",
        )
        result = rule.evaluate({"errors": 3})
        assert result == "err_check: 3 errors found"

    def test_evaluate_no_trigger_returns_none(self):
        """Test evaluate returns None when condition does not match."""
        rule = AlertRule(
            name="err_check",
            condition=lambda ctx: ctx.get("errors", 0) > 5,
        )
        result = rule.evaluate({"errors": 2})
        assert result is None

    def test_evaluate_handles_exception_gracefully(self):
        """Test evaluate catches exceptions and returns None."""
        def bad_condition(ctx):
            raise RuntimeError("condition error")

        rule = AlertRule(name="bad_rule", condition=bad_condition)
        result = rule.evaluate({})
        assert result is None

    def test_evaluate_name_in_severity_placeholder(self):
        """Test evaluate includes severity in format context."""
        rule = AlertRule(
            name="sev_test",
            condition=lambda ctx: True,
            severity="critical",
            message_template="{name} [{severity}]",
        )
        result = rule.evaluate({})
        assert result == "sev_test [critical]"


# ---------------------------------------------------------------------------
# AlertRule.dispatch tests
# ---------------------------------------------------------------------------


class TestAlertRuleDispatch:
    """Test AlertRule.dispatch method."""

    def test_dispatch_no_channels(self):
        """Test dispatch with no channels returns empty dict."""
        rule = AlertRule(
            name="r1",
            condition=lambda ctx: True,
        )
        results = rule.dispatch("test message")
        assert results == {}

    def test_dispatch_single_channel_success(self):
        """Test dispatch through a single successful channel."""
        mock_channel = MagicMock(spec=AlertChannel)
        mock_channel.send.return_value = True

        rule = AlertRule(
            name="r1",
            condition=lambda ctx: True,
            severity="warning",
            channel_instances=[mock_channel],
        )
        results = rule.dispatch("alert msg")

        assert "MagicMock" in results
        assert results["MagicMock"] is True
        mock_channel.send.assert_called_once_with("alert msg", "warning", "r1")

    def test_dispatch_multiple_channels(self):
        """Test dispatch through multiple channels."""
        ch1 = MagicMock(spec=AlertChannel)
        ch1.send.return_value = True
        ch2 = MagicMock(spec=AlertChannel)
        ch2.send.return_value = False

        rule = AlertRule(
            name="r1",
            condition=lambda ctx: True,
            severity="critical",
            channel_instances=[ch1, ch2],
        )
        results = rule.dispatch("msg")

        # Both are MagicMock type, so second overwrites first in dict keys
        # The last MagicMock result (False) wins since they share type name
        assert len(results) == 1  # Same type name
        assert results["MagicMock"] is False  # Last one wins

    def test_dispatch_real_channel_types(self):
        """Test dispatch with real channel types (Webhook + Email)."""
        mock_response = MagicMock()
        mock_response.status = 200

        email_ch = EmailChannel(recipients=["admin@test.com"])
        webhook_ch = WebhookChannel(url="http://example.com/wh")

        rule = AlertRule(
            name="r1",
            condition=lambda ctx: True,
            severity="warning",
            channel_instances=[email_ch, webhook_ch],
        )

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = MagicMock(
                return_value=mock_response
            )
            mock_urlopen.return_value.__exit__ = MagicMock(
                return_value=False
            )
            results = rule.dispatch("hello")

        assert results["EmailChannel"] is True
        assert results["WebhookChannel"] is True


# ---------------------------------------------------------------------------
# AlertManager.check_and_dispatch tests
# ---------------------------------------------------------------------------


class TestAlertManagerDispatch:
    """Test AlertManager.check_and_dispatch method."""

    def setup_method(self):
        """Create a fresh alert manager for each test."""
        self.manager = AlertManager()

    def test_no_rules_returns_empty(self):
        """Test check_and_dispatch with no rules returns empty list."""
        result = self.manager.check_and_dispatch({"errors": 10})
        assert result == []

    def test_rule_triggered_dispatches(self):
        """Test triggered rule dispatches and returns dispatch results."""
        email_ch = EmailChannel(recipients=["admin@test.com"])
        rule = AlertRule(
            name="err_rule",
            condition=lambda ctx: ctx.get("errors", 0) > 0,
            severity="critical",
            channel_instances=[email_ch],
        )
        self.manager.add_rule(rule)

        result = self.manager.check_and_dispatch({"errors": 5})

        assert len(result) == 1
        assert result[0]["rule"] == "err_rule"
        assert result[0]["message"] == "Alert 'err_rule' triggered"
        assert result[0]["severity"] == "critical"
        assert "EmailChannel" in result[0]["dispatch_results"]

    def test_rule_not_triggered_skips(self):
        """Test non-triggered rule is not dispatched."""
        rule = AlertRule(
            name="err_rule",
            condition=lambda ctx: ctx.get("errors", 0) > 10,
        )
        self.manager.add_rule(rule)

        result = self.manager.check_and_dispatch({"errors": 2})
        assert result == []

    def test_multiple_rules_partial_trigger(self):
        """Test only triggered rules are dispatched."""
        rule_a = AlertRule(
            name="fires",
            condition=lambda ctx: True,
        )
        rule_b = AlertRule(
            name="no_fire",
            condition=lambda ctx: False,
        )
        self.manager.add_rule(rule_a)
        self.manager.add_rule(rule_b)

        result = self.manager.check_and_dispatch({})
        assert len(result) == 1
        assert result[0]["rule"] == "fires"

    def test_multiple_rules_all_trigger(self):
        """Test all triggered rules are dispatched."""
        rule_a = AlertRule(
            name="rule_a",
            condition=lambda ctx: ctx.get("x", 0) > 0,
            severity="warning",
        )
        rule_b = AlertRule(
            name="rule_b",
            condition=lambda ctx: ctx.get("y", 0) > 0,
            severity="critical",
        )
        self.manager.add_rule(rule_a)
        self.manager.add_rule(rule_b)

        result = self.manager.check_and_dispatch({"x": 1, "y": 1})
        assert len(result) == 2
        assert result[0]["rule"] == "rule_a"
        assert result[1]["rule"] == "rule_b"

    def test_condition_exception_handled(self):
        """Test that condition exceptions are handled gracefully."""
        def bad_condition(ctx):
            raise ValueError("boom")

        rule = AlertRule(name="bad", condition=bad_condition)
        self.manager.add_rule(rule)

        result = self.manager.check_and_dispatch({})
        assert result == []

    def test_dispatch_result_structure(self):
        """Test the structure of each result entry."""
        mock_ch = MagicMock(spec=AlertChannel)
        mock_ch.send.return_value = True

        rule = AlertRule(
            name="struct_test",
            condition=lambda ctx: True,
            severity="warning",
            message_template="{name}: value={val}",
            channel_instances=[mock_ch],
        )
        self.manager.add_rule(rule)

        result = self.manager.check_and_dispatch({"val": 42})
        assert len(result) == 1
        entry = result[0]
        assert entry["rule"] == "struct_test"
        assert entry["message"] == "struct_test: value=42"
        assert entry["severity"] == "warning"
        assert "dispatch_results" in entry
        assert isinstance(entry["dispatch_results"], dict)

    def test_channel_failure_in_dispatch(self):
        """Test that channel failure is recorded in results."""
        mock_ch = MagicMock(spec=AlertChannel)
        mock_ch.send.return_value = False

        rule = AlertRule(
            name="fail_test",
            condition=lambda ctx: True,
            channel_instances=[mock_ch],
        )
        self.manager.add_rule(rule)

        result = self.manager.check_and_dispatch({})
        assert result[0]["dispatch_results"]["MagicMock"] is False

    def test_channel_instances_not_in_repr(self):
        """Test that channel_instances are excluded from repr."""
        rule = AlertRule(
            name="r1",
            condition=lambda ctx: True,
        )
        repr_str = repr(rule)
        assert "channel_instances=" not in repr_str
