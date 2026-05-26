"""
ORC format reader and writer using pyarrow.
"""

import pandas as pd
from .base import DataReader, DataWriter
from ..core.filesystem import is_cloud_path, get_filesystem


class OrcReader(DataReader):
    """Read data from ORC files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from an ORC file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the ORC file.
            **kwargs: Additional arguments (columns, filters, etc.).

        Returns:
            pandas DataFrame containing the data.
        """
        from pyarrow import orc

        if is_cloud_path(source):
            fs = kwargs.pop('filesystem', None)
            if fs is None:
                fs = get_filesystem(source)
            with fs.open(source, 'rb') as f:
                orc_file = orc.ORCFile(f)
                table = orc_file.read(**kwargs)
        else:
            orc_file = orc.ORCFile(source)
            table = orc_file.read(**kwargs)

        return table.to_pandas()


class OrcWriter(DataWriter):
    """Write data to ORC files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to an ORC file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output ORC file.
            **kwargs: Additional arguments (compression, etc.).
        """
        import pyarrow as pa
        from pyarrow import orc

        table = pa.Table.from_pandas(data)
        compression = kwargs.pop("compression", "snappy")

        if is_cloud_path(destination):
            fs = kwargs.pop('filesystem', None)
            if fs is None:
                fs = get_filesystem(destination)
            with fs.open(destination, 'wb') as f:
                orc.write_table(
                    table,
                    f,
                    compression=compression,
                    **kwargs,
                )
        else:
            orc.write_table(
                table, destination, compression=compression, **kwargs
            )
