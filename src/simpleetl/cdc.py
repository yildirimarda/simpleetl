"""
CDC ingestion: read Debezium-format change events and apply
insert/update/delete to a Delta or JDBC sink.

Requires optional dependencies depending on sink choice:

    pip install simpleetl[delta]     # for Delta Lake sink
    # or use JDBC (sqlite/postgresql/mysql) via simpleetl[databases]
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

OP_CREATE = "c"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_READ = "r"


class CDCEvent:
    """A single Debezium-format change event.

    Args:
        raw: The raw event dict (typically from JSON or Kafka message).
    """

    def __init__(self, raw: Dict[str, Any]) -> None:
        self.raw = raw
        self.op = raw.get("op")
        self.before = raw.get("before")
        self.after = raw.get("after")
        source = raw.get("source") or {}
        self.source = source
        self.topic = source.get("topic") if isinstance(source, dict) else None

    def is_create(self) -> bool:
        return self.op == OP_CREATE

    def is_update(self) -> bool:
        return self.op == OP_UPDATE

    def is_delete(self) -> bool:
        return self.op == OP_DELETE

    def is_read(self) -> bool:
        return self.op == OP_READ

    def get_target_record(self) -> Optional[Dict[str, Any]]:
        """Get the record to apply.

        - For delete: the ``before`` state (used for key matching).
        - For create/update/read: the ``after`` state.
        """
        if self.is_delete():
            return self.before
        return self.after

    def get_key_dict(self, key_columns: List[str]) -> Optional[Dict[str, Any]]:
        record = self.get_target_record()
        if record is None:
            return None
        return {k: record.get(k) for k in key_columns}

    def __repr__(self) -> str:  # pragma: no cover
        return f"CDCEvent(op={self.op!r})"


class CDCFixtureReader:
    """Read recorded Debezium event fixtures from JSON files.

    Args:
        fixtures_dir: Directory containing ``.json`` fixture files.
    """

    def __init__(self, fixtures_dir: Optional[str] = None) -> None:
        self.fixtures_dir = fixtures_dir

    def read_fixtures(self, fixtures_dir: Optional[str] = None) -> List[CDCEvent]:
        """Read all ``.json`` files from the fixtures directory.

        Each file may contain a single event object or an array of objects.
        """
        dir_path = Path(fixtures_dir or self.fixtures_dir or ".")
        events: List[CDCEvent] = []
        if not dir_path.exists():
            logger.warning("Fixtures directory does not exist: %s", dir_path)
            return events
        for path in sorted(dir_path.glob("*.json")):
            events.extend(self.read_file(str(path)))
        return events

    def read_file(self, file_path: str) -> List[CDCEvent]:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [CDCEvent(item) for item in data]
        if isinstance(data, dict):
            return [CDCEvent(data)]
        logger.warning("Unexpected fixture format in %s", file_path)
        return []


class CDCIngestor:
    """Apply CDC events to a Delta Lake or JDBC sink.

    Args:
        sink_type: ``"delta"`` or ``"jdbc"``.
        sink_path: Path to Delta table (for delta) or connection URL (for jdbc).
        table_name: Target table name (required for jdbc; optional for delta).
        key_columns: Columns that form the unique key for updates/deletes.
        engine: Optional SQLAlchemy engine (for jdbc sink).
    """

    def __init__(
        self,
        sink_type: str = "delta",
        sink_path: Optional[str] = None,
        table_name: Optional[str] = None,
        key_columns: Optional[List[str]] = None,
        engine: Optional[Any] = None,
    ) -> None:
        if sink_type not in ("delta", "jdbc"):
            raise ValueError(
                f"Unknown sink_type '{sink_type}'. Must be 'delta' or 'jdbc'."
            )
        self.sink_type = sink_type
        self.sink_path = sink_path
        self.table_name = table_name or "cdc_target"
        self.key_columns = key_columns or ["id"]
        self.engine = engine

    def _get_writer(self) -> Any:
        if self.sink_type == "delta":
            from .formats.delta import DeltaLakeWriter

            return DeltaLakeWriter()
        if self.sink_type == "jdbc":
            from .formats.database import DatabaseWriter

            return DatabaseWriter()
        raise ValueError(f"Unknown sink_type: {self.sink_type}")

    def apply_events(
        self,
        events: List[CDCEvent],
        sink_path: Optional[str] = None,
        table_name: Optional[str] = None,
        engine: Optional[Any] = None,
    ) -> None:
        """Apply a list of CDC events to the configured sink.

        Args:
            events: List of :class:`CDCEvent` instances.
            sink_path: Override for sink path / URL.
            table_name: Override for table name.
            engine: Override SQLAlchemy engine (jdbc only).
        """
        path = sink_path or self.sink_path
        name = table_name or self.table_name
        eng = engine or self.engine

        for event in events:
            if event.is_read():
                # Read events are typically no-ops for ingestion.
                logger.info("Skipping read event: %s", event)
                continue
            if event.is_create():
                self._apply_insert(event, path=path, table_name=name, engine=eng)
            elif event.is_update():
                self._apply_update(event, path=path, table_name=name, engine=eng)
            elif event.is_delete():
                self._apply_delete(event, path=path, table_name=name, engine=eng)
            else:
                logger.warning("Unknown op '%s' for event: %s", event.op, event)

    # ------------------------------------------------------------------
    # Internal operation handlers
    # ------------------------------------------------------------------

    def _apply_insert(
        self,
        event: CDCEvent,
        path: Optional[str] = None,
        table_name: Optional[str] = None,
        engine: Optional[Any] = None,
    ) -> None:
        record = event.get_target_record()
        if record is None:
            logger.info("Insert skipped: no after record")
            return
        df = pd.DataFrame([record])
        writer = self._get_writer()
        if self.sink_type == "delta":
            writer.write(df, path or self.sink_path, mode="append")
        else:
            writer.write(
                df,
                engine or self.engine,
                table_name=table_name or self.table_name,
                if_exists="append",
            )
        logger.info("Applied insert: %s", record)

    def _apply_update(
        self,
        event: CDCEvent,
        path: Optional[str] = None,
        table_name: Optional[str] = None,
        engine: Optional[Any] = None,
    ) -> None:
        record = event.get_target_record()
        if record is None:
            logger.info("Update skipped: no after record")
            return
        df = pd.DataFrame([record])
        writer = self._get_writer()
        if self.sink_type == "delta":
            # Delta: read-modify-write using overwrite for simplicity.
            self._delta_update(
                df,
                path=path or self.sink_path,
                table_name=table_name or self.table_name,
            )
        else:
            writer.merge(
                df,
                engine or self.engine,
                table_name=table_name or self.table_name,
                key_columns=self.key_columns,
            )
        logger.info("Applied update: %s", record)

    def _apply_delete(
        self,
        event: CDCEvent,
        path: Optional[str] = None,
        table_name: Optional[str] = None,
        engine: Optional[Any] = None,
    ) -> None:
        before = event.get_target_record()
        if before is None:
            logger.info("Delete skipped: no before record")
            return
        if self.sink_type == "delta":
            self._delta_delete(
                before,
                path=path or self.sink_path,
                table_name=table_name or self.table_name,
            )
        else:
            # JDBC delete via SQL on the engine.
            from sqlalchemy import text

            eng = engine or self.engine
            if eng is None:
                raise ValueError("JDBC sink requires an engine for delete operations")
            full_name = f"{table_name or self.table_name}"
            where_parts = []
            params = {}
            for k in self.key_columns:
                val = before.get(k)
                where_parts.append(f"{k} = :{k}")
                params[k] = val
            sql = f"DELETE FROM {full_name} WHERE {' AND '.join(where_parts)}"
            with eng.begin() as conn:
                result = conn.execute(text(sql), params)
                logger.info("Applied delete (%d rows): %s", result.rowcount, before)

    # ------------------------------------------------------------------
    # Delta-specific helpers (read-modify-write)
    # ------------------------------------------------------------------

    def _delta_update(
        self, new_df: pd.DataFrame, path: Optional[str], table_name: str
    ) -> None:
        from .formats.delta import DeltaLakeReader, DeltaLakeWriter

        if path is None:
            raise ValueError("Delta sink requires sink_path")
        try:
            reader = DeltaLakeReader()
            current = reader.read(path)
        except Exception:
            current = pd.DataFrame()
        # Filter out existing rows matching the key columns, then append new.
        key_df = new_df[self.key_columns].drop_duplicates()
        if not key_df.empty:
            # Build a filter to exclude rows with matching keys
            mask = pd.Series(True, index=current.index)
            for _, row in key_df.iterrows():
                mask &= ~current[self.key_columns].eq(row[self.key_columns]).all(axis=1)
            current = current[mask]
        combined = pd.concat([current, new_df], ignore_index=True)
        writer = DeltaLakeWriter()
        writer.write(combined, path, mode="overwrite")

    def _delta_delete(
        self, before: Dict[str, Any], path: Optional[str], table_name: str
    ) -> None:
        from .formats.delta import DeltaLakeReader, DeltaLakeWriter

        if path is None:
            raise ValueError("Delta sink requires sink_path")
        try:
            reader = DeltaLakeReader()
            current = reader.read(path)
        except Exception:
            current = pd.DataFrame()
        # Filter out rows matching delete keys
        mask = pd.Series(True, index=current.index)
        for _, row in pd.DataFrame([before]).iterrows():
            mask &= ~current[self.key_columns].eq(row[self.key_columns]).all(axis=1)
        filtered = current[mask]
        writer = DeltaLakeWriter()
        writer.write(filtered, path, mode="overwrite")
