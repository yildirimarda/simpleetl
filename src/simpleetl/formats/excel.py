"""
Excel format reader and writer using pandas and openpyxl.
"""

import pandas as pd
from io import BytesIO
from .base import DataReader, DataWriter
from ..core.filesystem import is_cloud_path, get_filesystem


class ExcelReader(DataReader):
    """Read data from Excel files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from an Excel file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the Excel file.
            **kwargs: Additional arguments to pass to pandas.read_excel.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.
        """
        sheet_name = kwargs.pop('sheet_name', 0)

        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'rb') as f:
                content = f.read()
            df = pd.read_excel(
                BytesIO(content), sheet_name=sheet_name, **kwargs
            )
        else:
            df = pd.read_excel(source, sheet_name=sheet_name, **kwargs)

        if sheet_name is None:
            return df

        return df


class ExcelWriter(DataWriter):
    """Write data to Excel files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to an Excel file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output Excel file.
            **kwargs: Additional arguments for Excel writing.
                Supports 'filesystem' for an fsspec filesystem instance.
        """
        sheet_name = kwargs.pop('sheet_name', 'Sheet1')

        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            buffer = BytesIO()
            if isinstance(data, dict):
                with pd.ExcelWriter(
                    buffer, engine='openpyxl', **kwargs
                ) as writer:
                    for sname, df in data.items():
                        df.to_excel(
                            writer, sheet_name=sname, index=False
                        )
            else:
                with pd.ExcelWriter(
                    buffer, engine='openpyxl', **kwargs
                ) as writer:
                    data.to_excel(
                        writer, sheet_name=sheet_name, index=False
                    )
            buffer.seek(0)
            with filesystem.open(destination, 'wb') as f:
                f.write(buffer.getvalue())
        else:
            if isinstance(data, dict):
                with pd.ExcelWriter(
                    destination, engine='openpyxl', **kwargs
                ) as writer:
                    for sname, df in data.items():
                        df.to_excel(
                            writer, sheet_name=sname, index=False
                        )
            else:
                with pd.ExcelWriter(
                    destination, engine='openpyxl', **kwargs
                ) as writer:
                    data.to_excel(
                        writer, sheet_name=sheet_name, index=False
                    )