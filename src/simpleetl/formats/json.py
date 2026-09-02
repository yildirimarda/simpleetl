"""
JSON format reader and writer using pandas and json.
"""

from typing import Iterator

import pandas as pd
from io import StringIO
from .base import DataReader, DataWriter, _chunk_size_from_max_buffer
from ..core.filesystem import is_cloud_path, get_filesystem


class JSONReader(DataReader):
    """Read data from JSON files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from a JSON file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the JSON file or JSON string.
            **kwargs: Additional arguments to pass to pandas.read_json.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.
        """
        # Check if source is a JSON string or file path
        if source.strip().startswith("{") or source.strip().startswith("["):
            return pd.read_json(StringIO(source), **kwargs)

        if is_cloud_path(source):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, "r") as f:
                return pd.read_json(f, **kwargs)

        return pd.read_json(source, **kwargs)

    def read_chunks(
        self, source: str, chunk_size: int = 10000, max_buffer_mb: float = 0, **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Read JSON data in chunks.

        For line-delimited JSON (``lines=True``), reads batches of lines
        and yields DataFrame chunks. For array/object JSON, falls back to
        reading the whole file as a single chunk.

        Args:
            source: Path to the JSON file or JSON string.
            chunk_size: Number of rows per chunk.
            max_buffer_mb: When > 0, chunk_size is derived from this limit.
            **kwargs: Additional arguments passed to pandas.read_json.

        Yields:
            pandas DataFrame chunks.
        """
        import json as _json

        if max_buffer_mb > 0:
            chunk_size = _chunk_size_from_max_buffer(max_buffer_mb)

        if isinstance(source, str) and (source.strip().startswith("{") or source.strip().startswith("[")):
            yield pd.read_json(StringIO(source), **kwargs)
            return

        # Check if we can use line-delimited chunked reading
        lines = kwargs.get("lines", True)
        orient = kwargs.get("orient", "records")

        if lines and (orient == "records" or orient is None or isinstance(orient, str) and "record" in orient):
            file_path = source
            file_obj = None
            try:
                if is_cloud_path(file_path):
                    filesystem = kwargs.pop("filesystem", None)
                    if filesystem is None:
                        filesystem = get_filesystem(file_path)
                    file_obj = filesystem.open(file_path, "r")
                else:
                    file_obj = open(file_path, "r")

                rows = []
                for line in file_obj:
                    line = line.strip()
                    if line:
                        rows.append(_json.loads(line))
                        if len(rows) >= chunk_size:
                            yield pd.DataFrame(rows)
                            rows = []
                if rows:
                    yield pd.DataFrame(rows)
            finally:
                if file_obj is not None:
                    file_obj.close()
            return

        # Fallback: read all at once and split into chunks
        df = self.read(source, **kwargs)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size].reset_index(drop=True)


class JSONWriter(DataWriter):
    """Write data to JSON files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to a JSON file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output JSON file.
            **kwargs: Additional arguments to pass to pandas.DataFrame.to_json.
                Supports 'filesystem' for an fsspec filesystem instance.
        """
        # Default to orient='records' for better JSON structure
        if "orient" not in kwargs:
            kwargs["orient"] = "records"
        if "lines" not in kwargs:
            kwargs["lines"] = True

        # If writing to string, handle differently
        if destination == "-":
            json_str = data.to_json(**kwargs)
            print(json_str, end="")
        elif is_cloud_path(destination):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, "w") as f:
                data.to_json(f, **kwargs)
        else:
            data.to_json(destination, **kwargs)
