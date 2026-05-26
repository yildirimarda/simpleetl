"""
Health and readiness check endpoints for Kubernetes.

Provides HTTP endpoints for liveness and readiness probes,
plus Prometheus metrics export.
"""

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from urllib.parse import urlparse

from .metrics import get_metrics
from .logger import get_logger

logger = get_logger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health, readiness, and metrics endpoints."""

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/health" or path == "/healthz":
            self._handle_health()
        elif path == "/ready" or path == "/readyz":
            self._handle_readiness()
        elif path == "/metrics":
            self._handle_metrics()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def _handle_health(self) -> None:
        """Liveness probe - always returns 200 if the process is running."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def _handle_readiness(self) -> None:
        """Readiness probe - returns 200 if the service is ready to accept traffic."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ready"}')

    def _handle_metrics(self) -> None:
        """Prometheus metrics endpoint."""
        metrics = get_metrics()
        output = metrics.get_metrics("text")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(output.encode("utf-8"))

    def log_message(self, format: str, *args) -> None:
        """Suppress default HTTP request logging to reduce noise."""
        pass


class HealthServer:
    """HTTP server for health checks and metrics."""

    def __init__(self, port: int = 8000):
        self.port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the health server in a background thread."""
        self._server = HTTPServer(("0.0.0.0", self.port), HealthHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(f"Health server started on port {self.port}")

    def stop(self) -> None:
        """Stop the health server."""
        if self._server:
            self._server.shutdown()
            logger.info("Health server stopped")


# Global health server instance
health_server = HealthServer()


def start_health_server(port: int = 8000) -> HealthServer:
    """Start the global health server."""
    server = HealthServer(port)
    server.start()
    return server
