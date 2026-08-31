"""
Tests for the metrics collection module.
"""

import json
from datetime import datetime, timedelta

import pytest

pytest.importorskip("prometheus_client")
from prometheus_client import CollectorRegistry

from simpleetl.core.metrics import MetricsCollector, get_metrics


class TestMetricsCollector:
    """Test MetricsCollector."""

    def test_counter_creation(self):
        """Test counter creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        counter = collector.counter("test_counter", "A test counter")
        assert counter is not None

    def test_gauge_creation(self):
        """Test gauge creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        gauge = collector.gauge("test_gauge", "A test gauge")
        assert gauge is not None

    def test_histogram_creation(self):
        """Test histogram creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        histogram = collector.histogram("test_histogram", "A test histogram")
        assert histogram is not None

    def test_inc_counter(self):
        """Test incrementing a counter."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter("etl_jobs_total", 1.0)
        # Should not raise

    def test_set_gauge(self):
        """Test setting a gauge value."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.set_gauge("etl_active_jobs", 5.0)
        # Should not raise

    def test_observe_histogram(self):
        """Test observing a histogram value."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.observe_histogram("etl_job_duration_seconds", 1.5)
        # Should not raise

    def test_context_timer(self):
        """Test context timer."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with collector.context_timer("etl_job_duration_seconds"):
            pass
        # Should not raise

    def test_get_metrics_text(self):
        """Test getting metrics in text format."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        metrics_text = collector.get_metrics("text")
        assert isinstance(metrics_text, str)

    def test_get_metrics_invalid_format(self):
        """Test getting metrics with invalid format raises ValueError."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with pytest.raises(ValueError, match="Unsupported output format"):
            collector.get_metrics("xml")

    def test_default_metrics_initialized(self):
        """Test that default metrics are initialized."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        assert "etl_jobs_total" in collector._counters
        assert "etl_active_jobs" in collector._gauges
        assert "etl_job_duration_seconds" in collector._histograms


class TestMetricsJSON:
    """Test JSON serialization of collected metrics."""

    def _collector(self):
        """Create a collector with an isolated registry."""
        return MetricsCollector(registry=CollectorRegistry())

    def test_json_output_parses(self):
        """Test that the JSON output is valid and well-shaped."""
        collector = self._collector()
        payload = json.loads(collector.get_metrics("json"))
        assert "timestamp" in payload
        assert isinstance(payload["metrics"], list)
        assert len(payload["metrics"]) > 0
        for entry in payload["metrics"]:
            assert "name" in entry
            assert entry["type"] in {"counter", "gauge", "histogram"}
            assert "value" in entry

    def test_json_timestamp_is_iso_utc(self):
        """Test that the timestamp is ISO 8601 with UTC offset."""
        collector = self._collector()
        payload = json.loads(collector.get_metrics("json"))
        ts = datetime.fromisoformat(payload["timestamp"])
        assert ts.tzinfo is not None
        assert ts.utcoffset() == timedelta(0)

    def test_json_counter_value(self):
        """Test that counter increments appear in the JSON output."""
        collector = self._collector()
        collector.inc_counter("etl_jobs_total", 3.0)
        payload = json.loads(collector.get_metrics("json"))
        entry = next(m for m in payload["metrics"] if m["name"] == "etl_jobs_total")
        assert entry["type"] == "counter"
        assert entry["value"] == 3.0
        assert "labels" not in entry

    def test_json_gauge_value(self):
        """Test that gauge values appear in the JSON output."""
        collector = self._collector()
        collector.set_gauge("etl_active_jobs", 7.0)
        payload = json.loads(collector.get_metrics("json"))
        entry = next(m for m in payload["metrics"] if m["name"] == "etl_active_jobs")
        assert entry["type"] == "gauge"
        assert entry["value"] == 7.0

    def test_json_labeled_counter(self):
        """Test that labeled counter children include their labels."""
        collector = self._collector()
        collector.counter("jobs_by_status", "Jobs by status", labelnames=("status",))
        collector.inc_counter("jobs_by_status", 2.0, labels={"status": "ok"})
        collector.inc_counter("jobs_by_status", 1.0, labels={"status": "failed"})
        payload = json.loads(collector.get_metrics("json"))
        entries = [m for m in payload["metrics"] if m["name"] == "jobs_by_status"]
        assert all(e["type"] == "counter" for e in entries)
        by_status = {e["labels"]["status"]: e["value"] for e in entries}
        assert by_status == {"ok": 2.0, "failed": 1.0}

    def test_json_labeled_gauge(self):
        """Test that labeled gauge children include their labels."""
        collector = self._collector()
        collector.gauge("queue_depth", "Queue depth", labelnames=("queue",))
        collector.set_gauge("queue_depth", 4.0, labels={"queue": "main"})
        payload = json.loads(collector.get_metrics("json"))
        entry = next(m for m in payload["metrics"] if m["name"] == "queue_depth")
        assert entry["type"] == "gauge"
        assert entry["value"] == 4.0
        assert entry["labels"] == {"queue": "main"}

    def test_json_histogram_value(self):
        """Test histogram serialization includes count, sum, buckets."""
        collector = self._collector()
        collector.observe_histogram("etl_job_duration_seconds", 1.5)
        collector.observe_histogram("etl_job_duration_seconds", 0.5)
        payload = json.loads(collector.get_metrics("json"))
        entry = next(
            m for m in payload["metrics"] if m["name"] == "etl_job_duration_seconds"
        )
        assert entry["type"] == "histogram"
        assert entry["value"]["count"] == 2.0
        assert entry["value"]["sum"] == 2.0
        assert entry["value"]["buckets"]["+Inf"] == 2.0
        assert entry["value"]["buckets"]["1.0"] == 1.0

    def test_json_labeled_histogram(self):
        """Test labeled histogram children include their labels."""
        collector = self._collector()
        collector.histogram("stage_duration", "Stage duration", labelnames=("stage",))
        collector.observe_histogram("stage_duration", 0.2, labels={"stage": "read"})
        payload = json.loads(collector.get_metrics("json"))
        entry = next(m for m in payload["metrics"] if m["name"] == "stage_duration")
        assert entry["type"] == "histogram"
        assert entry["labels"] == {"stage": "read"}
        assert entry["value"]["count"] == 1.0
        assert entry["value"]["sum"] == pytest.approx(0.2)

    def test_json_contains_all_default_metrics(self):
        """Test that all default metric names are serialized."""
        collector = self._collector()
        payload = json.loads(collector.get_metrics("json"))
        names = {m["name"] for m in payload["metrics"]}
        assert "etl_jobs_total" in names
        assert "etl_jobs_failed" in names
        assert "etl_active_jobs" in names
        assert "etl_last_job_timestamp" in names
        assert "etl_job_duration_seconds" in names

    def test_json_export_to_file(self):
        """Test that JSON metrics can be exported to a file."""
        import os
        import tempfile

        collector = self._collector()
        collector.inc_counter("etl_jobs_total", 1.0)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_file = f.name

        try:
            collector.export_to_file(temp_file, format="json")
            with open(temp_file, "r") as f:
                payload = json.load(f)
            assert "metrics" in payload
        finally:
            os.unlink(temp_file)


class TestTimerContext:
    """Test TimerContext."""

    def test_timer_context_records_duration(self):
        """Test that TimerContext records duration."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with collector.context_timer("etl_job_duration_seconds"):
            import time

            time.sleep(0.01)
        # Should complete without error


class TestGetMetrics:
    """Test get_metrics function."""

    def test_get_metrics_returns_collector(self):
        """Test that get_metrics returns a MetricsCollector."""
        result = get_metrics()
        assert isinstance(result, MetricsCollector)

    def test_time_function_decorator(self):
        """Test time_function decorator."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)

        @collector.time_function("etl_job_duration_seconds")
        def slow_func():
            import time

            time.sleep(0.01)
            return 42

        result = slow_func()
        assert result == 42

    def test_register_custom_metric(self):
        """Test registering a custom metric."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.register_custom_metric(
            "custom_gauge", "gauge_family", "A custom gauge"
        )
        assert "custom_gauge" in collector._custom_metrics

    def test_export_to_file(self):
        """Test exporting metrics to a file."""
        import tempfile
        import os

        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter("etl_jobs_total", 1.0)

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            temp_file = f.name

        try:
            collector.export_to_file(temp_file, format="text")
            assert os.path.exists(temp_file)
            with open(temp_file, "r") as f:
                content = f.read()
            assert len(content) > 0
        finally:
            os.unlink(temp_file)


class TestMetricsFallback:
    """Test graceful no-op fallback when prometheus_client is absent."""

    def test_fallback_no_op(self, monkeypatch):
        """MetricsCollector works and is a no-op when missing."""
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        assert collector._available is False
        collector.inc_counter("etl_jobs_total", 1.0)
        collector.set_gauge("etl_active_jobs", 5.0)
        collector.observe_histogram("etl_job_duration_seconds", 1.5)
        # Should not raise

    def test_fallback_counter_returns_dummy(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        counter = collector.counter("test", "desc")
        assert counter is not None
        counter.inc(1)

    def test_fallback_gauge_returns_dummy(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        gauge = collector.gauge("test", "desc")
        assert gauge is not None
        gauge.set(1)

    def test_fallback_histogram_returns_dummy(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        hist = collector.histogram("test", "desc")
        assert hist is not None
        hist.observe(1.0)

    def test_fallback_text_metrics_empty(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        text = collector.get_metrics("text")
        assert text == ""

    def test_fallback_json_metrics_empty(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        payload = json.loads(collector.get_metrics("json"))
        assert payload["metrics"] == []
        assert "timestamp" in payload

    def test_fallback_timer_context(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        with collector.context_timer("test_timer"):
            pass

    def test_fallback_time_function_decorator(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()

        @collector.time_function("test_timer")
        def sample():
            return 42

        assert sample() == 42

    def test_fallback_export_to_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        collector = MetricsCollector()
        filepath = str(tmp_path / "metrics.json")
        collector.export_to_file(filepath, format="json")
        with open(filepath) as f:
            payload = json.load(f)
        assert payload["metrics"] == []

    def test_fallback_get_metrics_global(self, monkeypatch):
        monkeypatch.setattr(
            "simpleetl.core.metrics.is_metrics_available", lambda: False
        )
        from simpleetl.core.metrics import get_metrics

        result = get_metrics()
        assert isinstance(result, MetricsCollector)
        # The global instance was initialized before the monkeypatch,
        # so we just verify it returns the same class.
