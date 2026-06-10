"""
Metrics collection hooks with Prometheus compatibility.
"""

import time
from typing import Dict, List, Optional, Callable, Any, Tuple
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
import threading


class MetricsCollector:
    """Metrics collector with Prometheus compatibility."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        """Initialize metrics collector."""
        self.registry = registry or CollectorRegistry()
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._custom_metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()

        # Initialize default metrics
        self._initialize_default_metrics()

    def _initialize_default_metrics(self) -> None:
        """Initialize default metrics."""
        # Job execution metrics
        self.counter('etl_jobs_total', 'Total number of ETL jobs executed')
        self.counter('etl_jobs_failed', 'Total number of failed ETL jobs')

        self.gauge('etl_active_jobs', 'Number of currently active ETL jobs')
        self.gauge('etl_last_job_timestamp', 'Timestamp of last job execution')

        self.histogram('etl_job_duration_seconds',
                      'Duration of ETL job execution in seconds')
        self.histogram('etl_records_processed_total',
                      'Total number of records processed')
        self.histogram('etl_read_duration_seconds',
                      'Duration of data read operations in seconds')
        self.histogram('etl_transform_duration_seconds',
                      'Duration of data transformation operations in seconds')
        self.histogram('etl_write_duration_seconds',
                      'Duration of data write operations in seconds')

    def counter(self, name: str, description: str,
                labelnames: Optional[tuple] = None) -> Counter:
        """Get or create a counter metric."""
        key = name
        with self._lock:
            if key not in self._counters:
                self._counters[key] = Counter(
                    name, description, labelnames or [], registry=self.registry
                )
            return self._counters[key]

    def gauge(self, name: str, description: str,
              labelnames: Optional[tuple] = None) -> Gauge:
        """Get or create a gauge metric."""
        key = name
        with self._lock:
            if key not in self._gauges:
                self._gauges[key] = Gauge(
                    name, description, labelnames or [], registry=self.registry
                )
            return self._gauges[key]

    def histogram(self, name: str, description: str,
                  labelnames: Optional[tuple] = None,
                  buckets: Optional[tuple] = None) -> Histogram:
        """Get or create a histogram metric."""
        key = name
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(
                    name, description, labelnames or [],
                    buckets=buckets or (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
                                     0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0),
                    registry=self.registry
                )
            return self._histograms[key]

    def inc_counter(self, name: str, value: float = 1.0,
                   labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        metric = self.counter(name, '')
        if labels:
            metric.labels(**labels).inc(value)
        else:
            metric.inc(value)

    def set_gauge(self, name: str, value: float,
                 labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric value."""
        metric = self.gauge(name, '')
        if labels:
            metric.labels(**labels).set(value)
        else:
            metric.set(value)

    def observe_histogram(self, name: str, value: float,
                         labels: Optional[Dict[str, str]] = None) -> None:
        """Observe a value in a histogram."""
        metric = self.histogram(name, '')
        if labels:
            metric.labels(**labels).observe(value)
        else:
            metric.observe(value)

    def time_function(self, name: str, labels: Optional[Dict[str, str]] = None) -> Callable:
        """Decorator to time function execution."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start_time
                    self.observe_histogram(name, duration, labels)
            return wrapper
        return decorator

    def context_timer(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        return TimerContext(name, self, labels)

    def get_metrics(self, output_format: str = 'text') -> str:
        """Get metrics in the specified format."""
        if output_format == 'text':
            return generate_latest(self.registry).decode('utf-8')
        elif output_format == 'json':
            return self._get_metrics_json()
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    def _get_metrics_json(self) -> str:
        """Serialize all collected metrics to a JSON string.

        The document contains a ``timestamp`` (ISO 8601, UTC) and a
        ``metrics`` list. Each entry has ``name``, ``type``
        (counter/gauge/histogram) and ``value``; entries for labeled
        metric children also include ``labels``. Histogram values are
        dictionaries with ``count``, ``sum`` and ``buckets`` keys.
        """
        import json
        from datetime import datetime, timezone

        entries: List[Dict[str, Any]] = []

        for name, counter_metric in self._counters.items():
            for family in counter_metric.collect():
                for sample in family.samples:
                    if not sample.name.endswith('_total'):
                        continue
                    entry: Dict[str, Any] = {
                        'name': name,
                        'type': 'counter',
                        'value': sample.value,
                    }
                    if sample.labels:
                        entry['labels'] = dict(sample.labels)
                    entries.append(entry)

        for name, gauge_metric in self._gauges.items():
            for family in gauge_metric.collect():
                for sample in family.samples:
                    entry = {
                        'name': name,
                        'type': 'gauge',
                        'value': sample.value,
                    }
                    if sample.labels:
                        entry['labels'] = dict(sample.labels)
                    entries.append(entry)

        for name, histogram_metric in self._histograms.items():
            entries.extend(
                self._histogram_json_entries(name, histogram_metric)
            )

        payload: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': entries,
        }
        return json.dumps(payload, indent=2)

    @staticmethod
    def _histogram_json_entries(name: str,
                                metric: Histogram) -> List[Dict[str, Any]]:
        """Build JSON entries for a histogram, one per label set.

        Groups the histogram's Prometheus samples (``*_bucket``,
        ``*_count``, ``*_sum``) by their label values (excluding the
        bucket boundary label ``le``).
        """
        series: Dict[Tuple[Tuple[str, str], ...], Dict[str, Any]] = {}
        for family in metric.collect():
            for sample in family.samples:
                labels = {
                    k: v for k, v in sample.labels.items() if k != 'le'
                }
                key = tuple(sorted(labels.items()))
                data = series.setdefault(key, {
                    'labels': labels,
                    'count': 0.0,
                    'sum': 0.0,
                    'buckets': {},
                })
                if sample.name.endswith('_bucket'):
                    data['buckets'][sample.labels['le']] = sample.value
                elif sample.name.endswith('_count'):
                    data['count'] = sample.value
                elif sample.name.endswith('_sum'):
                    data['sum'] = sample.value

        entries: List[Dict[str, Any]] = []
        for data in series.values():
            entry: Dict[str, Any] = {
                'name': name,
                'type': 'histogram',
                'value': {
                    'count': data['count'],
                    'sum': data['sum'],
                    'buckets': data['buckets'],
                },
            }
            if data['labels']:
                entry['labels'] = data['labels']
            entries.append(entry)
        return entries

    def register_custom_metric(self, name: str, metric_type: str,
                             description: str) -> None:
        """Register a custom metric."""
        with self._lock:
            if metric_type == 'gauge_family':
                self._custom_metrics[name] = GaugeMetricFamily(
                    name, description, labels=['label']
                )
            # Add more metric types as needed

    def export_to_file(self, filepath: str, format: str = 'text') -> None:
        """Export metrics to a file."""
        metrics = self.get_metrics(format)
        with open(filepath, 'w') as f:
            f.write(metrics)


class TimerContext:
    """Context manager for timing operations."""

    def __init__(self, name: str, collector: MetricsCollector,
                 labels: Optional[Dict[str, str]] = None):
        self.name = name
        self.collector = collector
        self.labels = labels or {}
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.collector.observe_histogram(self.name, duration, self.labels)


# Global metrics collector instance
metrics = MetricsCollector()


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    return metrics


# Decorators for common metrics
def job_timer(labels: Optional[Dict[str, str]] = None):
    """Decorator to time job execution."""
    return metrics.time_function('etl_job_duration_seconds', labels)