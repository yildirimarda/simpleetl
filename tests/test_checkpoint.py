"""
Tests for the checkpoint management module.
"""

import tempfile
from pathlib import Path


from simpleetl.core.checkpoint import (
    Checkpoint,
    CheckpointManager,
    InMemoryCheckpointStore,
    FileCheckpointStore,
)


class TestCheckpoint:
    """Tests for the Checkpoint dataclass."""

    def test_create_checkpoint(self):
        cp = Checkpoint(job_id="abc-123", phase="extract", records_processed=100)
        assert cp.job_id == "abc-123"
        assert cp.phase == "extract"
        assert cp.records_processed == 100
        assert cp.watermark is None
        assert cp.timestamp is not None

    def test_to_dict(self):
        cp = Checkpoint(job_id="abc", phase="transform", records_processed=50)
        d = cp.to_dict()
        assert d["job_id"] == "abc"
        assert d["phase"] == "transform"
        assert d["records_processed"] == 50

    def test_from_dict(self):
        data = {
            "job_id": "xyz",
            "job_name": "test_job",
            "phase": "load",
            "records_processed": 200,
            "watermark": "2024-01-01",
            "metadata": {"key": "value"},
            "timestamp": "2024-01-01T00:00:00+00:00",
        }
        cp = Checkpoint.from_dict(data)
        assert cp.job_id == "xyz"
        assert cp.job_name == "test_job"
        assert cp.phase == "load"
        assert cp.records_processed == 200
        assert cp.watermark == "2024-01-01"
        assert cp.metadata == {"key": "value"}

    def test_from_dict_ignores_extra_fields(self):
        data = {"job_id": "abc", "extra_field": "ignored"}
        cp = Checkpoint.from_dict(data)
        assert cp.job_id == "abc"
        assert not hasattr(cp, "extra_field")


class TestInMemoryCheckpointStore:
    """Tests for InMemoryCheckpointStore."""

    def setup_method(self):
        self.store = InMemoryCheckpointStore()

    def test_save_and_load(self):
        cp = Checkpoint(job_id="job1", phase="extract", records_processed=10)
        self.store.save(cp)
        loaded = self.store.load("job1")
        assert loaded is not None
        assert loaded.job_id == "job1"
        assert loaded.phase == "extract"
        assert loaded.records_processed == 10

    def test_load_nonexistent(self):
        assert self.store.load("nonexistent") is None

    def test_overwrite(self):
        cp1 = Checkpoint(job_id="job1", phase="extract", records_processed=10)
        cp2 = Checkpoint(job_id="job1", phase="transform", records_processed=20)
        self.store.save(cp1)
        self.store.save(cp2)
        loaded = self.store.load("job1")
        assert loaded.phase == "transform"
        assert loaded.records_processed == 20

    def test_delete(self):
        cp = Checkpoint(job_id="job1", phase="extract")
        self.store.save(cp)
        self.store.delete("job1")
        assert self.store.load("job1") is None

    def test_delete_nonexistent(self):
        self.store.delete("nonexistent")  # Should not raise

    def test_clear(self):
        self.store.save(Checkpoint(job_id="j1"))
        self.store.save(Checkpoint(job_id="j2"))
        self.store.clear()
        assert self.store.load("j1") is None
        assert self.store.load("j2") is None


class TestFileCheckpointStore:
    """Tests for FileCheckpointStore."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.store = FileCheckpointStore(self.tmpdir)

    def test_save_creates_file(self):
        cp = Checkpoint(job_id="job1", phase="extract", records_processed=10)
        self.store.save(cp)
        path = Path(self.tmpdir) / "job1.json"
        assert path.exists()

    def test_save_and_load(self):
        cp = Checkpoint(
            job_id="job1",
            job_name="test",
            phase="transform",
            records_processed=42,
            watermark="2024-06-01",
        )
        self.store.save(cp)
        loaded = self.store.load("job1")
        assert loaded is not None
        assert loaded.job_id == "job1"
        assert loaded.job_name == "test"
        assert loaded.phase == "transform"
        assert loaded.records_processed == 42
        assert loaded.watermark == "2024-06-01"

    def test_load_nonexistent(self):
        assert self.store.load("nonexistent") is None

    def test_delete(self):
        cp = Checkpoint(job_id="job1")
        self.store.save(cp)
        self.store.delete("job1")
        assert self.store.load("job1") is None

    def test_corrupted_file_returns_none(self):
        path = Path(self.tmpdir) / "bad.json"
        path.write_text("not valid json{{{")
        assert self.store.load("bad") is None

    def test_creates_directory(self):
        new_dir = Path(self.tmpdir) / "subdir" / "checkpoints"
        FileCheckpointStore(new_dir)
        assert new_dir.exists()


class TestCheckpointManager:
    """Tests for CheckpointManager."""

    def setup_method(self):
        self.store = InMemoryCheckpointStore()

    def test_save_checkpoint(self):
        mgr = CheckpointManager(store=self.store, job_id="job1", job_name="test")
        cp = mgr.save_checkpoint(phase="extract", records_processed=100)
        assert cp.job_id == "job1"
        assert cp.job_name == "test"
        assert cp.phase == "extract"
        assert cp.records_processed == 100

    def test_load_checkpoint(self):
        mgr = CheckpointManager(store=self.store, job_id="job1")
        mgr.save_checkpoint(phase="transform", records_processed=50)
        loaded = mgr.load_checkpoint()
        assert loaded is not None
        assert loaded.phase == "transform"

    def test_load_nonexistent(self):
        mgr = CheckpointManager(store=self.store, job_id="nonexistent")
        assert mgr.load_checkpoint() is None

    def test_delete_checkpoint(self):
        mgr = CheckpointManager(store=self.store, job_id="job1")
        mgr.save_checkpoint(phase="load", records_processed=200)
        mgr.delete_checkpoint()
        assert mgr.load_checkpoint() is None

    def test_should_resume_true(self):
        mgr = CheckpointManager(store=self.store, job_id="job1")
        mgr.save_checkpoint(phase="extract", records_processed=10)
        assert mgr.should_resume() is True

    def test_should_resume_false(self):
        mgr = CheckpointManager(store=self.store, job_id="no_checkpoint")
        assert mgr.should_resume() is False

    def test_generates_job_id(self):
        mgr = CheckpointManager(store=self.store)
        assert mgr.job_id is not None
        assert len(mgr.job_id) > 0

    def test_save_with_metadata(self):
        mgr = CheckpointManager(store=self.store, job_id="job1")
        cp = mgr.save_checkpoint(
            phase="extract",
            records_processed=10,
            watermark="2024-01-01",
            metadata={"source": "s3://bucket/data"},
        )
        assert cp.watermark == "2024-01-01"
        assert cp.metadata == {"source": "s3://bucket/data"}
