"""
JSON format reader and writer using pandas and json.
"""

import pandas as pd
from io import StringIO
from .base import DataReader, DataWriter
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
        if source.strip().startswith('{') or source.strip().startswith('['):
            return pd.read_json(StringIO(source), **kwargs)

        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'r') as f:
                return pd.read_json(f, **kwargs)

        return pd.read_json(source, **kwargs)


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
        if 'orient' not in kwargs:
            kwargs['orient'] = 'records'
        if 'lines' not in kwargs:
            kwargs['lines'] = True

        # If writing to string, handle differently
        if destination == '-':
            json_str = data.to_json(**kwargs)
            print(json_str, end='')
        elif is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, 'w') as f:
                data.to_json(f, **kwargs)
        else:
            data.to_json(destination, **kwargs)