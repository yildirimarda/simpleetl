"""Tests for health check and metrics HTTP endpoints."""

import pytest
import time
import urllib.request
import urllib.error

from simpleetl.core.health import HealthServer


@pytest.fixture
def health_server():
    """Start a health server on a random port for testing."""
    server = HealthServer(port=0)
    server.start()
    # Get the actual port assigned
    port = server._server.server_address[1]
    time.sleep(0.1)  # Brief wait for server to start
    yield server, port
    server.stop()


class TestHealthEndpoints:
    """Test health check HTTP endpoints."""

    def test_health_endpoint(self, health_server):
        """Test /health returns 200."""
        _, port = health_server
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/health")
        assert resp.status == 200
        body = resp.read()
        assert b'"status":"ok"' in body

    def test_readiness_endpoint(self, health_server):
        """Test /ready returns 200."""
        _, port = health_server
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/ready")
        assert resp.status == 200
        body = resp.read()
        assert b'"status":"ready"' in body

    def test_metrics_endpoint(self, health_server):
        """Test /metrics returns 200 with Prometheus format."""
        _, port = health_server
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics")
        assert resp.status == 200
        body = resp.read()
        assert b'etl_jobs_total' in body

    def test_unknown_endpoint_returns_404(self, health_server):
        """Test unknown path returns 404."""
        _, port = health_server
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown")
        assert exc_info.value.code == 404

    def test_healthz_alias(self, health_server):
        """Test /healthz alias works."""
        _, port = health_server
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert resp.status == 200

    def test_readyz_alias(self, health_server):
        """Test /readyz alias works."""
        _, port = health_server
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz")
        assert resp.status == 200
