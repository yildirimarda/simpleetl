"""
Avro format reader and writer.

Uses fastavro if available, otherwise falls back to pyarrow for reading.
Install fastavro for full read/write support: pip install simpleetl[avro]
"""

from typing import Any, Dict, List

import pandas as pd

from ..core.filesystem import get_filesystem, is_cloud_path
from .base import DataReader, DataWriter

# fastavro is optional — pyarrow can read Avro but not write it
_fastavro = None  # type: ignore[assignment]


def _get_fastavro():
    """Lazily import fastavro, raising a helpful error if missing."""
    global _fastavro
    if _fastavro is None:
        try:
            import fastavro as _fa
            _fastavro = _fa
        except ImportError:
            raise ImportError(
                "fastavro is required for Avro support. "
                "Install it with: pip install simpleetl[avro]"
            ) from None
    return _fastavro


class AvroReader(DataReader):
    """Read data from Avro files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from an Avro file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.
        Uses fastavro if available, otherwise falls back to pyarrow.

        Args:
            source: Path to the Avro file.
            **kwargs: Additional arguments for the reader.

        Returns:
            pandas DataFrame containing the data.
        """
        try:
            return self._read_with_fastavro(source, **kwargs)
        except ImportError:
            return self._read_with_pyarrow(source, **kwargs)

    @staticmethod
    def _read_with_fastavro(source: str, **kwargs) -> pd.DataFrame:
        fastavro = _get_fastavro()
        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'rb') as f:
                reader = fastavro.reader(f, **kwargs)
                records = [record for record in reader]
        else:
            with open(source, 'rb') as f:
                reader = fastavro.reader(f, **kwargs)
                records = [record for record in reader]
        return pd.DataFrame(records)

    @staticmethod
    def _read_with_pyarrow(source: str, **kwargs) -> pd.DataFrame:
        from pyarrow import avro
        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'rb') as f:
                table = avro.read_avro(f)
        else:
            table = avro.read_avro(source)
        return table.to_pandas()


class AvroWriter(DataWriter):
    """Write data to Avro files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to an Avro file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.
        Requires fastavro for writing.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output Avro file.
            **kwargs: Additional arguments. Supports 'schema' for Avro schema
                and 'filesystem' for an fsspec filesystem instance.
        """
        fastavro = _get_fastavro()
        records = data.to_dict(orient='records')

        schema = kwargs.pop('schema', None)
        if schema is None:
            schema = self._infer_schema(data)

        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, 'wb') as f:
                fastavro.writer(f, schema, records, **kwargs)
        else:
            with open(destination, 'wb') as f:
                fastavro.writer(f, schema, records, **kwargs)

    @staticmethod
    def _infer_schema(df: pd.DataFrame) -> Dict[str, Any]:
        """Infer Avro schema from a DataFrame."""
        type_map = {
            'int64': 'long',
            'int32': 'int',
            'float64': 'double',
            'float32': 'float',
            'bool': 'boolean',
            'object': 'string',
        }
        fields: List[Dict[str, str]] = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            avro_type = type_map.get(dtype, 'string')
            fields.append({'name': col, 'type': avro_type})
        return {
            'type': 'record',
            'name': 'Record',
            'fields': fields,
        }
