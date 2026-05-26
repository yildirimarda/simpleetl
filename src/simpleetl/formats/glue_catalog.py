"""
Glue Data Catalog reader and writer.

Provides ``GlueCatalogReader`` and ``GlueCatalogWriter`` that interact with
the AWS Glue Data Catalog through ``DynamicFrame`` objects.  Supported formats
include Parquet, JSON, CSV, ORC, and Avro.

These classes are designed to be used inside an AWS Glue environment where
``awsglue`` and ``pyspark`` are available.  Outside of Glue they will raise
``RuntimeError`` unless a mock/test context is supplied.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Formats supported by Glue Data Catalog writes
SUPPORTED_FORMATS = {"parquet", "json", "csv", "orc", "avro", "glueparquet"}


class GlueCatalogReader:
    """Read tables from the AWS Glue Data Catalog as DynamicFrames.

    Args:
        glue_context: A ``GlueContext`` instance.  If ``None``, the reader
            will attempt to create one via :func:`create_glue_context`.

    Example::

        reader = GlueCatalogReader(glue_context)
        df = reader.read(database="my_db", table_name="my_table")
    """

    def __init__(self, glue_context: Any = None) -> None:
        """Initialize the GlueCatalogReader.

        Args:
            glue_context: An optional ``GlueContext`` instance.
        """
        self._glue_context = glue_context

    @property
    def glue_context(self) -> Any:
        """Lazily get or create the GlueContext."""
        if self._glue_context is None:
            from ..platforms.glue import create_glue_context

            self._glue_context = create_glue_context()
        return self._glue_context

    def read(
        self,
        database: str,
        table_name: str,
        transformation_ctx: str = "",
        push_down_predicate: str = "",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Read a table from the Glue Data Catalog.

        Args:
            database: The Glue database name.
            table_name: The Glue table name.
            transformation_ctx: Bookmark tracking context name.
            push_down_predicate: Predicate for partition pruning.
            additional_options: Extra catalog options.

        Returns:
            A ``DynamicFrame`` containing the table data.

        Raises:
            ValueError: If *database* or *table_name* is empty.
        """
        if not database:
            raise ValueError("database must not be empty")
        if not table_name:
            raise ValueError("table_name must not be empty")

        from ..platforms.glue import read_from_catalog

        frame = read_from_catalog(
            glue_context=self.glue_context,
            database=database,
            table_name=table_name,
            transformation_ctx=transformation_ctx,
            push_down_predicate=push_down_predicate,
            additional_options=additional_options,
        )
        logger.info(
            "GlueCatalogReader: read %s.%s (%d records)",
            database,
            table_name,
            frame.count(),
        )
        return frame

    def read_as_pandas(
        self,
        database: str,
        table_name: str,
        transformation_ctx: str = "",
        push_down_predicate: str = "",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Read a table from the Glue Data Catalog and convert to pandas.

        Args:
            database: The Glue database name.
            table_name: The Glue table name.
            transformation_ctx: Bookmark tracking context name.
            push_down_predicate: Predicate for partition pruning.
            additional_options: Extra catalog options.

        Returns:
            A ``pandas.DataFrame``.
        """

        frame = self.read(
            database=database,
            table_name=table_name,
            transformation_ctx=transformation_ctx,
            push_down_predicate=push_down_predicate,
            additional_options=additional_options,
        )
        spark_df = frame.toDF()
        return spark_df.toPandas()


class GlueCatalogWriter:
    """Write DynamicFrames to the AWS Glue Data Catalog.

    Args:
        glue_context: A ``GlueContext`` instance.  If ``None``, the writer
            will attempt to create one via :func:`create_glue_context`.

    Example::

        writer = GlueCatalogWriter(glue_context)
        writer.write(frame=df, database="my_db", table_name="my_table",
                     format="parquet")
    """

    def __init__(self, glue_context: Any = None) -> None:
        """Initialize the GlueCatalogWriter.

        Args:
            glue_context: An optional ``GlueContext`` instance.
        """
        self._glue_context = glue_context

    @property
    def glue_context(self) -> Any:
        """Lazily get or create the GlueContext."""
        if self._glue_context is None:
            from ..platforms.glue import create_glue_context

            self._glue_context = create_glue_context()
        return self._glue_context

    def write(
        self,
        frame: Any,
        database: str,
        table_name: str,
        format: str = "parquet",
        transformation_ctx: str = "",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write a DynamicFrame to the Glue Data Catalog.

        Args:
            frame: The ``DynamicFrame`` to write.
            database: The target Glue database name.
            table_name: The target Glue table name.
            format: Output format (``parquet``, ``json``, ``csv``,
                ``orc``, ``avro``, ``glueparquet``).
            transformation_ctx: Bookmark tracking context name.
            additional_options: Extra catalog options.

        Raises:
            ValueError: If *database*, *table_name* is empty, or *format*
                is unsupported.
        """
        if not database:
            raise ValueError("database must not be empty")
        if not table_name:
            raise ValueError("table_name must not be empty")
        if format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{format}'. "
                f"Supported formats: {sorted(SUPPORTED_FORMATS)}"
            )

        from ..platforms.glue import write_to_catalog

        write_to_catalog(
            frame=frame,
            glue_context=self.glue_context,
            database=database,
            table_name=table_name,
            format=format,
            transformation_ctx=transformation_ctx,
            additional_options=additional_options,
        )
        logger.info(
            "GlueCatalogWriter: wrote %s to %s.%s",
            format,
            database,
            table_name,
        )

    def write_from_pandas(
        self,
        dataframe: Any,
        database: str,
        table_name: str,
        format: str = "parquet",
        transformation_ctx: str = "",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write a pandas DataFrame to the Glue Data Catalog.

        Converts the pandas DataFrame to a Spark DataFrame and then to a
        DynamicFrame before writing.

        Args:
            dataframe: A ``pandas.DataFrame`` to write.
            database: The target Glue database name.
            table_name: The target Glue table name.
            format: Output format.
            transformation_ctx: Bookmark tracking context name.
            additional_options: Extra catalog options.
        """
        if self.glue_context is None:
            raise RuntimeError(
                "GlueContext is not available. Cannot convert pandas "
                "DataFrame to DynamicFrame outside of AWS Glue."
            )

        spark = self.glue_context.spark_session
        spark.createDataFrame(dataframe)
        dynamic_frame = self.glue_context.createDataFrameFromPandas(
            dataframe, table_name
        )
        self.write(
            frame=dynamic_frame,
            database=database,
            table_name=table_name,
            format=format,
            transformation_ctx=transformation_ctx,
            additional_options=additional_options,
        )
