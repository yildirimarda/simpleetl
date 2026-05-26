"""
Format factory for auto-detecting and creating appropriate readers/writers.
"""

import os
from typing import Any, Dict, Type
from .base import DataReader, DataWriter
from . import csv
from . import json
from . import parquet
from . import avro
from . import orc
from . import xml
from . import excel
from . import database


def _get_plugin_registry():
    """Lazily import and return the global PluginRegistry."""
    from ..core.plugins import get_format_registry

    return get_format_registry()


def register_format(
    extensions: list,
    reader_cls: Type[DataReader],
    writer_cls: Type[DataWriter],
    plugin_name: str = "custom-format",
    version: str = "0.1.0",
) -> None:
    """Register a custom format programmatically.

    Args:
        extensions: File extensions handled by this format (e.g. ['.custom']).
        reader_cls: DataReader subclass for reading the format.
        writer_cls: DataWriter subclass for writing the format.
        plugin_name: Name for the auto-generated format plugin.
        version: Version string for the auto-generated format plugin.
    """
    from ..core.plugins import register_format as _register

    _register(
        extensions=extensions,
        reader_cls=reader_cls,
        writer_cls=writer_cls,
        plugin_name=plugin_name,
        version=version,
    )


class FormatFactory:
    """Factory class for creating appropriate readers and writers based on file
    format.

    Supports both local and cloud storage paths (S3, GCS, ABFS). Format is
    always detected from the file extension, even for cloud paths.

    Custom formats registered via the PluginRegistry are checked before
    falling back to built-in formats.
    """

    # Mapping of file extensions to reader/writer classes
    FORMAT_MAP: dict[str, dict[str, Any]] = {
        '.csv': {
            'reader': csv.CSVReader,
            'writer': csv.CSVWriter,
            'mime_type': 'text/csv'
        },
        '.json': {
            'reader': json.JSONReader,
            'writer': json.JSONWriter,
            'mime_type': 'application/json'
        },
        '.parquet': {
            'reader': parquet.ParquetReader,
            'writer': parquet.ParquetWriter,
            'mime_type': 'application/octet-stream'
        },
        '.avro': {
            'reader': avro.AvroReader,
            'writer': avro.AvroWriter,
            'mime_type': 'application/octet-stream'
        },
        '.orc': {
            'reader': orc.OrcReader,
            'writer': orc.OrcWriter,
            'mime_type': 'application/octet-stream'
        },
        '.xml': {
            'reader': xml.XMLReader,
            'writer': xml.XMLWriter,
            'mime_type': 'application/xml'
        },
        '.xlsx': {
            'reader': excel.ExcelReader,
            'writer': excel.ExcelWriter,
            'mime_type': 'application/vnd.openxmlformats-officedocument'
                         '.spreadsheetml.sheet'
        },
        '.xls': {
            'reader': excel.ExcelReader,
            'writer': excel.ExcelWriter,
            'mime_type': 'application/vnd.ms-excel'
        }
    }

    @classmethod
    def _get_extension(cls, path: str) -> str:
        """
        Extract the file extension from a path, handling cloud URLs.

        Cloud paths may contain query strings or fragments, so we extract
        the extension from the path portion only.

        Args:
            path: File path or cloud URL.

        Returns:
            Lowercase file extension including the dot.
        """
        # Strip query parameters and fragments from cloud URLs
        clean_path = path.split('?')[0].split('#')[0]
        _, ext = os.path.splitext(clean_path)
        return ext.lower()

    @classmethod
    def get_reader(cls, source: str, **kwargs) -> DataReader:
        """
        Get appropriate reader for the source file.

        Format is detected from the file extension. Cloud paths
        (s3://, gs://, abfss://) are NOT defaulted to CSV -- the
        extension is always used to determine format.

        Args:
            source: File path or data source.
            **kwargs: Additional arguments for the reader. Supports
                'filesystem' to pass an fsspec filesystem instance.

        Returns:
            Appropriate DataReader instance.
        """
        # Check if it's a database connection
        if source.startswith(
            ('postgresql://', 'mysql://', 'mssql://', 'sqlite://')
        ):
            return database.DatabaseReader(**kwargs)

        ext = cls._get_extension(source)

        if ext in cls.FORMAT_MAP:
            reader_class = cls.FORMAT_MAP[ext]['reader']
            return reader_class(**kwargs)

        # Check PluginRegistry for custom formats
        registry = _get_plugin_registry()
        format_plugin = registry.get_format_for_extension(ext)
        if format_plugin is not None:
            return format_plugin.get_reader()(**kwargs)

        # If no extension found on cloud path, raise an error
        from ..core.filesystem import is_cloud_path
        if is_cloud_path(source):
            raise ValueError(
                f"Cannot detect format from cloud path '{source}': "
                f"no file extension found. Please include the file "
                f"extension in the path (e.g., 's3://bucket/data.csv')."
            )

        # Default to CSV reader for local paths
        return csv.CSVReader(**kwargs)

    @classmethod
    def get_writer(cls, destination: str, **kwargs) -> DataWriter:
        """
        Get appropriate writer for the destination file.

        Format is detected from the file extension. Cloud paths
        (s3://, gs://, abfss://) are NOT defaulted to CSV -- the
        extension is always used to determine format.

        Args:
            destination: File path or destination.
            **kwargs: Additional arguments for the writer. Supports
                'filesystem' to pass an fsspec filesystem instance.

        Returns:
            Appropriate DataWriter instance.
        """
        # Check if it's a database connection
        if destination.startswith(
            ('postgresql://', 'mysql://', 'mssql://', 'sqlite://')
        ):
            return database.DatabaseWriter(**kwargs)

        ext = cls._get_extension(destination)

        if ext in cls.FORMAT_MAP:
            writer_class = cls.FORMAT_MAP[ext]['writer']
            return writer_class(**kwargs)

        # Check PluginRegistry for custom formats
        registry = _get_plugin_registry()
        format_plugin = registry.get_format_for_extension(ext)
        if format_plugin is not None:
            return format_plugin.get_writer()(**kwargs)

        # If no extension found on cloud path, raise an error
        from ..core.filesystem import is_cloud_path
        if is_cloud_path(destination):
            raise ValueError(
                f"Cannot detect format from cloud path '{destination}': "
                f"no file extension found. Please include the file "
                f"extension in the path (e.g., 's3://bucket/data.csv')."
            )

        # Default to CSV writer for local paths
        return csv.CSVWriter(**kwargs)

    @classmethod
    def detect_format(cls, source: str) -> Dict[str, Any]:
        """
        Detect file format from source.

        Args:
            source: File path or data source.

        Returns:
            Dictionary containing format information.
        """
        # Check if it's a database connection
        if source.startswith(
            ('postgresql://', 'mysql://', 'mssql://', 'sqlite://')
        ):
            return {
                'format': 'database',
                'extension': '.db',
                'mime_type': 'application/x-sqlite3'
            }

        ext = cls._get_extension(source)

        if ext in cls.FORMAT_MAP:
            format_info = cls.FORMAT_MAP[ext].copy()
            format_info['extension'] = ext
            format_info['format'] = ext[1:]  # Remove the dot
            return format_info
        else:
            return {
                'format': 'unknown',
                'extension': ext,
                'mime_type': 'application/octet-stream'
            }

    @classmethod
    def supported_formats(cls) -> dict[str, str]:
        """Get list of supported formats.

        Returns:
            Dictionary mapping format names to their extensions.
        """
        result: dict[str, str] = {}
        for ext, format_info in cls.FORMAT_MAP.items():
            format_name = ext[1:]  # Remove the dot
            result[format_name] = ext
        return result