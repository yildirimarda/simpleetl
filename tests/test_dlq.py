"""
Tests for the Dead Letter Queue module.
"""

import tempfile
from pathlib import Path

import pytest

from simpleetl.core.dlq import DLQEntry, DeadLetterQueue


class TestDLQEntry:
    """Tests for the DLQEntry dataclass."""

    def test_create_entry(self):
        entry = DLQEntry(record_data={"id": 1}, error="bad data", phase="transform")
        assert entry.record_data == {"id": 1}
        assert entry.error == "bad data"
        assert entry.phase == "transform"
        assert entry.record_index == -1
        assert entry.timestamp is not None

    def test_to_dict(self):
        entry = DLQEntry(
            record_data={"id": 1},
            error="err",
            phase="load",
            record_index=5,
            error_type="ValueError",
        )
        d = entry.to_dict()
        assert d["record_data"] == {"id": 1}
        assert d["error"] == "err"
        assert d["phase"] == "load"
        assert d["record_index"] == 5
        assert d["error_type"] == "ValueError"

    def test_from_dict(self):
        data = {
            "record_data": {"name": "test"},
            "error": "missing field",
            "phase": "extract",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "record_index": 3,
            "error_type": "KeyError",
            "metadata": {"source": "csv"},
        }
        entry = DLQEntry.from_dict(data)
        assert entry.record_data == {"name": "test"}
        assert entry.error == "missing field"
        assert entry.record_index == 3
        assert entry.error_type == "KeyError"
        assert entry.metadata == {"source": "csv"}


class TestDeadLetterQueue:
    """Tests for the DeadLetterQueue class."""

    def setup_method(self):
        self.dlq = DeadLetterQueue()

    def test_add_entry_with_string_error(self):
        entry = self.dlq.add_entry(
            record_data={"id": 1},
            error="something failed",
            phase="transform",
        )
        assert entry.error == "something failed"
        assert entry.error_type == ""
        assert self.dlq.count == 1

    def test_add_entry_with_exception(self):
        exc = ValueError("bad value")
        entry = self.dlq.add_entry(
            record_data={"id": 2},
            error=exc,
            phase="load",
            record_index=10,
        )
        assert entry.error == "bad value"
        assert entry.error_type == "ValueError"
        assert entry.record_index == 10

    def test_entries_returns_copy(self):
        self.dlq.add_entry(record_data={"id": 1}, error="err")
        entries = self.dlq.entries
        assert len(entries) == 1
        entries.clear()
        assert self.dlq.count == 1  # Original not affected

    def test_clear(self):
        self.dlq.add_entry(record_data={"id": 1}, error="err")
        self.dlq.add_entry(record_data={"id": 2}, error="err")
        assert self.dlq.count == 2
        self.dlq.clear()
        assert self.dlq.count == 0

    def test_write_jsonl(self):
        self.dlq.add_entry(record_data={"id": 1}, error="err1", phase="extract")
        self.dlq.add_entry(record_data={"id": 2}, error="err2", phase="transform")

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name

        count = self.dlq.write_to_dlq(path, format="jsonl")
        assert count == 2

        content = Path(path).read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 2

    def test_write_csv(self):
        self.dlq.add_entry(record_data={"id": 1}, error="err1", phase="extract")
        self.dlq.add_entry(record_data={"id": 2}, error="err2", phase="transform")

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name

        count = self.dlq.write_to_dlq(path, format="csv")
        assert count == 2

        content = Path(path).read_text()
        assert "record_index" in content
        assert "err1" in content
        assert "err2" in content

    def test_write_unsupported_format(self):
        self.dlq.add_entry(record_data={"id": 1}, error="err")
        with pytest.raises(ValueError, match="Unsupported DLQ format"):
            self.dlq.write_to_dlq("/tmp/test.xml", format="xml")

    def test_read_jsonl(self):
        self.dlq.add_entry(
            record_data={"id": 1},
            error="err1",
            phase="extract",
            record_index=0,
        )
        self.dlq.add_entry(
            record_data={"id": 2},
            error="err2",
            phase="transform",
            record_index=1,
        )

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            path = f.name
            for entry in self.dlq.entries:
                import json
                f.write(json.dumps(entry.to_dict()) + "\n")

        new_dlq = DeadLetterQueue()
        entries = new_dlq.read_from_dlq(path, format="jsonl")
        assert len(entries) == 2
        assert entries[0].error == "err1"
        assert entries[1].error == "err2"

    def test_read_csv(self):
        self.dlq.add_entry(
            record_data={"id": 1},
            error="err1",
            phase="extract",
            record_index=0,
        )

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name

        self.dlq.write_to_dlq(path, format="csv")

        new_dlq = DeadLetterQueue()
        entries = new_dlq.read_from_dlq(path, format="csv")
        assert len(entries) == 1
        assert entries[0].error == "err1"

    def test_read_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            self.dlq.read_from_dlq("/nonexistent/path/file.jsonl")

    def test_read_unsupported_format(self):
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
            path = f.name
        with pytest.raises(ValueError, match="Unsupported DLQ format"):
            self.dlq.read_from_dlq(path, format="xml")

    def test_write_empty_dlq_jsonl(self):
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        count = self.dlq.write_to_dlq(path, format="jsonl")
        assert count == 0
        assert Path(path).read_text() == ""

    def test_write_empty_dlq_csv(self):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        count = self.dlq.write_to_dlq(path, format="csv")
        assert count == 0

    def test_roundtrip_jsonl(self):
        """Test write then read preserves data."""
        self.dlq.add_entry(
            record_data={"name": "Alice", "age": 30},
            error="validation failed",
            phase="transform",
            record_index=42,
            metadata={"rule": "age_check"},
        )

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name

        self.dlq.write_to_dlq(path, format="jsonl")

        new_dlq = DeadLetterQueue()
        entries = new_dlq.read_from_dlq(path, format="jsonl")
        assert len(entries) == 1
        assert entries[0].record_data == {"name": "Alice", "age": 30}
        assert entries[0].error == "validation failed"
        assert entries[0].phase == "transform"
        assert entries[0].record_index == 42
        assert entries[0].metadata == {"rule": "age_check"}
