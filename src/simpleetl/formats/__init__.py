"""
SimpleETL Format Support

This package provides readers and writers for various data formats including:
- CSV
- JSON
- Parquet
- Avro
- ORC
- XML
- Excel
- Database (JDBC)
- Glue Data Catalog
"""

from .base import DataReader, DataWriter
from .csv import CSVReader, CSVWriter
from .json import JSONReader, JSONWriter
from .parquet import ParquetReader, ParquetWriter
from .avro import AvroReader, AvroWriter
from .orc import OrcReader, OrcWriter
from .xml import XMLReader, XMLWriter
from .excel import ExcelReader, ExcelWriter
from .database import DatabaseReader, DatabaseWriter
from .glue_catalog import GlueCatalogReader, GlueCatalogWriter
from .factory import FormatFactory

__all__ = [
    # Base classes
    'DataReader',
    'DataWriter',
    # Format readers and writers
    'CSVReader',
    'CSVWriter',
    'JSONReader',
    'JSONWriter',
    'ParquetReader',
    'ParquetWriter',
    'AvroReader',
    'AvroWriter',
    'OrcReader',
    'OrcWriter',
    'XMLReader',
    'XMLWriter',
    'ExcelReader',
    'ExcelWriter',
    'DatabaseReader',
    'DatabaseWriter',
    # Glue Data Catalog
    'GlueCatalogReader',
    'GlueCatalogWriter',
    # Factory
    'FormatFactory',
]