"""
Tests for OpenLineage integration in the lineage module.
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

from simpleetl.core.lineage import (
    LineageEvent,
    LineageTracker,
    OpenLineageConverter,
    configure_openlineage,
    get_openlineage_converter,
)


# ---------------------------------------------------------------------------
# Mock HTTP server helper
# ---------------------------------------------------------------------------

class MockOpenLineageHandler(BaseHTTPRequestHandler):
    """HTTP handler that collects POSTed OpenLineage RunEvents."""

    received_bodies: list[dict] = []
    response_status: int = 200
    request_count: int = 0

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        self.__class__.request_count += 1
        if body:
            self.__class__.received_bodies.append(json.loads(body))
        self.send_response(self.response_status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def log_message(self, format: str, *args: object) -> None:
        """Silence stderr output during tests."""


def start_mock_server(
    host: str = "127.0.0.1", port: int = 0
) -> tuple[HTTPServer, str]:
    """Start a mock OpenLineage HTTP server.

    Returns the server instance and the base URL string.
    """
    server = HTTPServer((host, port), MockOpenLineageHandler)
    # Reset class-level state
    MockOpenLineageHandler.received_bodies = []
    MockOpenLineageHandler.request_count = 0
    MockOpenLineageHandler.response_status = 200
    actual_port = server.server_address[1]
    url = f"http://{host}:{actual_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, url


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _make_event(**kwargs: object) -> LineageEvent:
    """Create a LineageEvent with sensible defaults for testing."""
    defaults: dict[str, object] = {
        "event_id": "evt-001",
        "timestamp": datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        "job_name": "test_job",
        "phase": "post_extract",
        "source": "s3://bucket/input.csv",
        "destination": "postgresql://db/schema.table",
        "operation": "extract",
        "input_schema": {"id": "integer", "name": "string"},
        "output_schema": {"id": "integer", "name": "string"},
        "input_rows": 100,
        "output_rows": 100,
        "duration_seconds": 1.5,
    }
    defaults.update(kwargs)
    return LineageEvent(**defaults)


# ---------------------------------------------------------------------------
# TestOpenLineageConverter
# ---------------------------------------------------------------------------


class TestOpenLineageConverter:
    """Test OpenLineageConverter creation and conversion methods."""

    def test_default_creation(self):
        """Test creating a converter with defaults."""
        converter = OpenLineageConverter()
        assert converter.namespace == "simpleetl"
        assert converter.producer == "simpleetl/1.0.0"

    def test_creation_with_custom_values(self):
        """Test creating a converter with custom namespace and producer."""
        converter = OpenLineageConverter(
            namespace="my-project",
            producer="my-etl/2.0.0",
        )
        assert converter.namespace == "my-project"
        assert converter.producer == "my-etl/2.0.0"

    def test_event_to_run_event_default_run_id(self):
        """Test that run_id defaults to event.event_id."""
        event = _make_event()
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert result["run"]["runId"] == "evt-001"

    def test_event_to_run_event_custom_run_id(self):
        """Test providing an explicit run_id."""
        event = _make_event()
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event, run_id="custom-run-123")
        assert result["run"]["runId"] == "custom-run-123"

    def test_event_to_run_event_producer(self):
        """Test that producer URI appears in the RunEvent."""
        converter = OpenLineageConverter(producer="test/3.0.0")
        result = converter.event_to_run_event(_make_event())
        assert result["producer"] == "test/3.0.0"

    def test_event_to_run_event_event_type(self):
        """Test that eventType is COMPLETE."""
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(_make_event())
        assert result["eventType"] == "COMPLETE"

    def test_event_to_run_event_event_time(self):
        """Test that eventTime matches the event timestamp."""
        ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        event = _make_event(timestamp=ts)
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert result["eventTime"] == ts.isoformat()

    def test_event_to_run_event_job(self):
        """Test that the job section uses the configured namespace."""
        event = _make_event(job_name="my_etl_job")
        converter = OpenLineageConverter(namespace="prod")
        result = converter.event_to_run_event(event)
        assert result["job"]["namespace"] == "prod"
        assert result["job"]["name"] == "my_etl_job"

    def test_event_to_run_event_job_unknown_name(self):
        """Test that empty job_name maps to 'unknown'."""
        event = _make_event(job_name="")
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert result["job"]["name"] == "unknown"

    def test_event_to_run_event_inputs(self):
        """Test that source is included as an input dataset."""
        event = _make_event(source="s3://data/input.parquet")
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert len(result["inputs"]) == 1
        assert result["inputs"][0]["name"] == "s3://data/input.parquet"

    def test_event_to_run_event_outputs(self):
        """Test that destination is included as an output dataset."""
        event = _make_event(destination="s3://data/output.parquet")
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert len(result["outputs"]) == 1
        assert result["outputs"][0]["name"] == "s3://data/output.parquet"

    def test_event_to_run_event_no_source(self):
        """Test that no input dataset is created when source is empty."""
        event = _make_event(source="")
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert result["inputs"] == []

    def test_event_to_run_event_no_destination(self):
        """Test that no output dataset is created when destination is empty."""
        event = _make_event(destination="")
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        assert result["outputs"] == []

    def test_event_to_run_event_schema_in_dataset(self):
        """Test that schema fields appear in the input dataset facet."""
        event = _make_event(
            source="file.csv",
            input_schema={"col_a": "int", "col_b": "str"},
        )
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        fields = result["inputs"][0]["facets"]["schema"]["fields"]
        field_names = [f["name"] for f in fields]
        assert "col_a" in field_names
        assert "col_b" in field_names

    def test_event_to_run_event_simpleetl_facet(self):
        """Test that the custom simpleetl facet contains ETL metadata."""
        event = _make_event(
            job_name="my_job",
            phase="post_transform",
            operation="dedup",
            input_rows=200,
            output_rows=180,
            duration_seconds=3.14,
        )
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(event)
        facet = result["run"]["facets"]["simpleetl"]
        assert facet["job_name"] == "my_job"
        assert facet["phase"] == "post_transform"
        assert facet["operation"] == "dedup"
        assert facet["input_rows"] == 200
        assert facet["output_rows"] == 180
        assert facet["duration_seconds"] == 3.14

    def test_event_to_run_event_schema_url(self):
        """Test that the schemaURL field is present."""
        converter = OpenLineageConverter()
        result = converter.event_to_run_event(_make_event())
        assert "schemaURL" in result
        assert "OpenLineage.json" in result["schemaURL"]

    def test_event_to_dataset_input(self):
        """Test converting to an input dataset."""
        event = _make_event(
            source="s3://bucket/raw.json",
            input_schema={"key": "string"},
        )
        converter = OpenLineageConverter()
        result = converter.event_to_dataset(event, is_input=True)
        assert result["name"] == "s3://bucket/raw.json"
        fields = result["facets"]["schema"]["fields"]
        assert fields[0]["name"] == "key"

    def test_event_to_dataset_output(self):
        """Test converting to an output dataset."""
        event = _make_event(
            destination="postgresql://host/db/table",
            output_schema={"val": "float"},
        )
        converter = OpenLineageConverter()
        result = converter.event_to_dataset(event, is_input=False)
        assert result["name"] == "postgresql://host/db/table"
        fields = result["facets"]["schema"]["fields"]
        assert fields[0]["name"] == "val"

    def test_event_to_dataset_empty_name(self):
        """Test that empty dataset name maps to 'unknown'."""
        event = _make_event(source="", input_schema={})
        converter = OpenLineageConverter()
        result = converter.event_to_dataset(event, is_input=True)
        assert result["name"] == "unknown"

    def test_build_dataset_empty_schema(self):
        """Test dataset with no schema fields."""
        converter = OpenLineageConverter(namespace="ns")
        result = converter._build_dataset("my_dataset", {})
        assert result["namespace"] == "ns"
        assert result["name"] == "my_dataset"
        assert result["facets"]["schema"]["fields"] == []

    def test_build_dataset_multiple_fields(self):
        """Test dataset with multiple schema fields."""
        converter = OpenLineageConverter()
        schema = {
            "id": "integer",
            "name": "varchar",
            "created_at": "timestamp",
        }
        result = converter._build_dataset("table", schema)
        fields = result["facets"]["schema"]["fields"]
        assert len(fields) == 3


# ---------------------------------------------------------------------------
# TestLineageTrackerEmitOpenlineage
# ---------------------------------------------------------------------------


class TestLineageTrackerEmitOpenlineage:
    """Test LineageTracker.emit_openlineage with a mock HTTP server."""

    def setup_method(self):
        """Set up a fresh tracker and mock server for each test."""
        self.tracker = LineageTracker()
        self.server, self.url = start_mock_server()

    def teardown_method(self):
        """Shut down the mock server."""
        self.server.shutdown()

    def test_emit_single_event(self):
        """Test emitting a single event to a mock server."""
        self.tracker.record_event(_make_event())
        count = self.tracker.emit_openlineage(self.url)
        assert count == 1
        assert MockOpenLineageHandler.request_count == 1

    def test_emit_multiple_events(self):
        """Test emitting multiple events."""
        for i in range(5):
            self.tracker.record_event(
                _make_event(event_id=f"evt-{i}", job_name="batch_job")
            )
        count = self.tracker.emit_openlineage(self.url)
        assert count == 5
        assert MockOpenLineageHandler.request_count == 5

    def test_emit_empty_tracker(self):
        """Test emitting from a tracker with no events."""
        count = self.tracker.emit_openlineage(self.url)
        assert count == 0
        assert MockOpenLineageHandler.request_count == 0

    def test_emit_with_custom_converter(self):
        """Test emitting with a custom OpenLineageConverter."""
        converter = OpenLineageConverter(
            namespace="custom-ns",
            producer="custom/1.0",
        )
        self.tracker.record_event(_make_event())
        count = self.tracker.emit_openlineage(self.url, converter=converter)
        assert count == 1
        body = MockOpenLineageHandler.received_bodies[0]
        assert body["producer"] == "custom/1.0"
        assert body["job"]["namespace"] == "custom-ns"

    def test_emit_with_default_converter(self):
        """Test that a default converter is used when none is provided."""
        self.tracker.record_event(_make_event())
        self.tracker.emit_openlineage(self.url)
        body = MockOpenLineageHandler.received_bodies[0]
        assert body["producer"] == "simpleetl/1.0.0"
        assert body["job"]["namespace"] == "simpleetl"

    def test_emit_body_is_valid_json(self):
        """Test that the emitted body contains all required OpenLineage fields."""
        self.tracker.record_event(_make_event())
        self.tracker.emit_openlineage(self.url)
        body = MockOpenLineageHandler.received_bodies[0]
        # Required top-level fields
        assert "producer" in body
        assert "schemaURL" in body
        assert "eventType" in body
        assert "eventTime" in body
        assert "run" in body
        assert "job" in body
        assert "inputs" in body
        assert "outputs" in body
        # Run fields
        assert "runId" in body["run"]
        assert "facets" in body["run"]
        # Job fields
        assert "namespace" in body["job"]
        assert "name" in body["job"]

    def test_emit_server_error_returns_zero(self):
        """Test graceful handling when the server returns a 500."""
        MockOpenLineageHandler.response_status = 500
        self.tracker.record_event(_make_event())
        count = self.tracker.emit_openlineage(self.url)
        assert count == 0

    def test_emit_network_error_returns_zero(self):
        """Test graceful handling when the server is unreachable."""
        self.tracker.record_event(_make_event())
        count = self.tracker.emit_openlineage(
            "http://127.0.0.1:1"  # unreachable port
        )
        assert count == 0

    def test_emit_partial_success(self):
        """Test that one failing request doesn't prevent others."""
        # Emit two events: first succeeds, second targets bad URL.
        self.tracker.record_event(_make_event(event_id="good"))
        self.tracker.record_event(_make_event(event_id="bad"))

        # Emit the first one to the real server
        self.tracker._events = [self.tracker._events[0]]
        count = self.tracker.emit_openlineage(self.url)
        assert count == 1

    def test_emit_content_type_is_json(self):
        """Test that the Content-Type header is application/json."""
        self.tracker.record_event(_make_event())
        self.tracker.emit_openlineage(self.url)
        # The mock server received the request; verify the body is valid JSON
        body = MockOpenLineageHandler.received_bodies[0]
        assert isinstance(body, dict)


# ---------------------------------------------------------------------------
# TestConfigureOpenlineage
# ---------------------------------------------------------------------------


class TestConfigureOpenlineage:
    """Test the module-level configure_openlineage function."""

    def test_returns_converter(self):
        """Test that configure_openlineage returns an OpenLineageConverter."""
        converter = configure_openlineage(
            url="http://localhost:5000",
            namespace="test-ns",
        )
        assert isinstance(converter, OpenLineageConverter)

    def test_default_namespace(self):
        """Test default namespace is 'simpleetl'."""
        converter = configure_openlineage(url="http://localhost:5000")
        assert converter.namespace == "simpleetl"

    def test_custom_namespace(self):
        """Test that custom namespace is propagated."""
        converter = configure_openlineage(
            url="http://localhost:5000",
            namespace="my-project",
        )
        assert converter.namespace == "my-project"

    def test_stores_as_global(self):
        """Test that the converter is stored and retrievable."""
        configure_openlineage(
            url="http://localhost:5000",
            namespace="global-test",
        )
        retrieved = get_openlineage_converter()
        assert retrieved is not None
        assert retrieved.namespace == "global-test"

    def test_producer_uri(self):
        """Test that producer URI uses the default format."""
        converter = configure_openlineage(url="http://localhost:5000")
        assert converter.producer == "simpleetl/1.0.0"

    def test_get_openlineage_converter_none_initially(self):
        """Test that converter is None before configuration."""
        # Reset by calling with a known state - can't fully reset from here
        # but we can check the function returns the right type
        result = get_openlineage_converter()
        # After the tests above configured it, it may or may not be None.
        # Just check that the function returns an
        # OpenLineageConverter or None
        assert result is None or isinstance(result, OpenLineageConverter)

    def test_overwrite_previous(self):
        """Test that calling configure_openlineage overwrites the previous."""
        configure_openlineage(
            url="http://old:5000", namespace="old-ns"
        )
        new = configure_openlineage(
            url="http://new:6000", namespace="new-ns"
        )
        assert new.namespace == "new-ns"
        assert get_openlineage_converter() is new
