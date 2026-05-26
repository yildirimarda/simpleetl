"""
Tests for the metrics collection module.
"""

import pytest
from prometheus_client import CollectorRegistry
from simpleetl.core.metrics import MetricsCollector, get_metrics


class TestMetricsCollector:
    """Test MetricsCollector."""

    def test_counter_creation(self):
        """Test counter creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        counter = collector.counter('test_counter', 'A test counter')
        assert counter is not None

    def test_gauge_creation(self):
        """Test gauge creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        gauge = collector.gauge('test_gauge', 'A test gauge')
        assert gauge is not None

    def test_histogram_creation(self):
        """Test histogram creation."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        histogram = collector.histogram('test_histogram', 'A test histogram')
        assert histogram is not None

    def test_inc_counter(self):
        """Test incrementing a counter."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter('etl_jobs_total', 1.0)
        # Should not raise

    def test_set_gauge(self):
        """Test setting a gauge value."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.set_gauge('etl_active_jobs', 5.0)
        # Should not raise

    def test_observe_histogram(self):
        """Test observing a histogram value."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.observe_histogram('etl_job_duration_seconds', 1.5)
        # Should not raise

    def test_context_timer(self):
        """Test context timer."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with collector.context_timer('etl_job_duration_seconds'):
            pass
        # Should not raise

    def test_get_metrics_text(self):
        """Test getting metrics in text format."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        metrics_text = collector.get_metrics('text')
        assert isinstance(metrics_text, str)

    def test_get_metrics_invalid_format(self):
        """Test getting metrics with invalid format raises ValueError."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with pytest.raises(ValueError, match="Unsupported output format"):
            collector.get_metrics('xml')

    def test_default_metrics_initialized(self):
        """Test that default metrics are initialized."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        assert 'etl_jobs_total' in collector._counters
        assert 'etl_active_jobs' in collector._gauges
        assert 'etl_job_duration_seconds' in collector._histograms


class TestTimerContext:
    """Test TimerContext."""

    def test_timer_context_records_duration(self):
        """Test that TimerContext records duration."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with collector.context_timer('etl_job_duration_seconds'):
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

        @collector.time_function('etl_job_duration_seconds')
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
        collector.register_custom_metric('custom_gauge', 'gauge_family', 'A custom gauge')
        assert 'custom_gauge' in collector._custom_metrics

    def test_export_to_file(self):
        """Test exporting metrics to a file."""
        import tempfile
        import os
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter('etl_jobs_total', 1.0)

        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            temp_file = f.name

        try:
            collector.export_to_file(temp_file, format='text')
            assert os.path.exists(temp_file)
            with open(temp_file, 'r') as f:
                content = f.read()
            assert len(content) > 0
        finally:
            os.unlink(temp_file)
