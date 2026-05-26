"""
Example ETL job demonstrating cloud storage paths.

Shows how SimpleETL handles S3, GCS (Google Cloud Storage), and
ABFS (Azure Blob File System / Azure Data Lake Storage) paths.

Cloud paths are supported through fsspec-compatible URI schemes:

- **S3**: ``s3://bucket/path/to/file.csv``
- **GCS**: ``gs://bucket/path/to/file.parquet``
- **ABFS**: ``abfs://container/path/to/file.parquet``

The ``FormatFactory`` detects the format from the file extension,
while the underlying I/O uses the appropriate cloud storage backend.

Prerequisites (install as needed):
    uv add s3fs       # For S3 support
    uv add gcsfs      # For Google Cloud Storage support
    uv add adlfs      # For Azure Blob / ADLS Gen2 support

Authentication is handled via standard SDK mechanisms:
- S3: ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars or IAM role
- GCS: ``GOOGLE_APPLICATION_CREDENTIALS`` env var pointing to a service account key
- ABFS: ``AZURE_STORAGE_ACCOUNT_NAME`` / ``AZURE_STORAGE_ACCOUNT_KEY`` env vars

Environment variables can be referenced in config using the ``${VAR}``
syntax, e.g. ``s3://${S3_BUCKET}/input/data.csv``.
"""

import logging
import os

import pandas as pd

from simpleetl.core.job import ETLJob
from simpleetl.core.logger import get_logger
from simpleetl.formats import FormatFactory
from simpleetl.transformations import filter_data, map_values


class S3ToParquetJob(ETLJob):
    """Read CSV from S3, transform, and write Parquet back to S3.

    Config parameters:
        input_path: S3 URI for the input CSV (e.g. ``s3://my-bucket/input/data.csv``)
        output_path: S3 URI for the output Parquet (e.g. ``s3://my-bucket/output/data.parquet``)
    """

    def __init__(self, config):
        super().__init__(config)
        self.logger = get_logger(__name__)

    def run(self) -> None:
        input_path = self.config.params["input_path"]
        output_path = self.config.params["output_path"]

        self.logger.info("Reading from S3: %s", input_path)
        reader = FormatFactory.get_reader(input_path)
        df = reader.read(input_path)

        self.logger.info("Read %d records from S3", len(df))

        # Example transformation: filter active records
        if "status" in df.columns:
            df = filter_data(df, column="status", min_value=0)

        self.logger.info("Writing to S3: %s", output_path)
        writer = FormatFactory.get_writer(output_path)
        writer.write(df, output_path)

        self.logger.info("Wrote %d records to S3", len(df))


class GcsToCsvJob(ETLJob):
    """Read Parquet from GCS, transform, and write CSV to GCS.

    Config parameters:
        input_path: GCS URI for the input Parquet
        output_path: GCS URI for the output CSV
    """

    def __init__(self, config):
        super().__init__(config)
        self.logger = get_logger(__name__)

    def run(self) -> None:
        input_path = self.config.params["input_path"]
        output_path = self.config.params["output_path"]

        self.logger.info("Reading from GCS: %s", input_path)
        reader = FormatFactory.get_reader(input_path)
        df = reader.read(input_path)

        self.logger.info("Read %d records from GCS", len(df))

        # Example transformation: standardize column names to lowercase
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        self.logger.info("Writing to GCS: %s", output_path)
        writer = FormatFactory.get_writer(output_path)
        writer.write(df, output_path)

        self.logger.info("Wrote %d records to GCS", len(df))


class AbfsMergeJob(ETLJob):
    """Read Parquet from ABFS, merge with existing data, and write back.

    Demonstrates a common pattern: read existing data from cloud storage,
    merge with new data, and write the combined result.

    Config parameters:
        input_path: ABFS URI for new data
        existing_path: ABFS URI for existing data
        output_path: ABFS URI for merged output
    """

    def __init__(self, config):
        super().__init__(config)
        self.logger = get_logger(__name__)

    def run(self) -> None:
        input_path = self.config.params["input_path"]
        existing_path = self.config.params["existing_path"]
        output_path = self.config.params["output_path"]

        self.logger.info("Reading new data from ABFS: %s", input_path)
        reader = FormatFactory.get_reader(input_path)
        new_data = reader.read(input_path)

        self.logger.info("Reading existing data from ABFS: %s", existing_path)
        existing_data = reader.read(existing_path)

        self.logger.info(
            "Merging %d new records with %d existing records",
            len(new_data),
            len(existing_data),
        )

        # Concatenate and deduplicate
        import pandas as pd

        combined = pd.concat([existing_data, new_data], ignore_index=True)

        # Deduplicate on all columns, keeping the last (newest) record
        dedup_cols = self.config.params.get("dedup_columns", None)
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")

        self.logger.info("Writing merged data to ABFS: %s", output_path)
        writer = FormatFactory.get_writer(output_path)
        writer.write(combined, output_path)

        self.logger.info("Wrote %d merged records to ABFS", len(combined))


class MultiCloudCopyJob(ETLJob):
    """Copy data between cloud providers (e.g., S3 to GCS).

    Reads from one cloud provider and writes to another, demonstrating
    that the framework handles cross-cloud data movement seamlessly.

    Config parameters:
        input_path: Source URI (any supported cloud scheme)
        output_path: Destination URI (any supported cloud scheme)
    """

    def __init__(self, config):
        super().__init__(config)
        self.logger = get_logger(__name__)

    def run(self) -> None:
        input_path = self.config.params["input_path"]
        output_path = self.config.params["output_path"]

        self.logger.info("Reading from: %s", input_path)
        reader = FormatFactory.get_reader(input_path)
        df = reader.read(input_path)

        self.logger.info("Read %d records", len(df))

        self.logger.info("Writing to: %s", output_path)
        writer = FormatFactory.get_writer(output_path)
        writer.write(df, output_path)

        self.logger.info("Cross-cloud copy complete: %s -> %s", input_path, output_path)


# ---------------------------------------------------------------------------
# Example configurations (for reference; these would normally be YAML files)
# ---------------------------------------------------------------------------

EXAMPLE_CONFIGS = {
    "s3_to_parquet": {
        "name": "s3_to_parquet",
        "description": "Read CSV from S3, transform, write Parquet to S3",
        "platform": "local",
        "input_format": "csv",
        "output_format": "parquet",
        "log_level": "INFO",
        "max_retries": 3,
        "retry_delay": 2.0,
        "params": {
            "input_path": "s3://my-data-lake/raw/customers.csv",
            "output_path": "s3://my-data-lake/processed/customers.parquet",
        },
    },
    "gcs_to_csv": {
        "name": "gcs_to_csv",
        "description": "Read Parquet from GCS, transform, write CSV to GCS",
        "platform": "local",
        "input_format": "parquet",
        "output_format": "csv",
        "log_level": "INFO",
        "max_retries": 3,
        "retry_delay": 2.0,
        "params": {
            "input_path": "gs://my-bucket/input/sales.parquet",
            "output_path": "gs://my-bucket/output/sales.csv",
        },
    },
    "abfs_merge": {
        "name": "abfs_merge",
        "description": "Merge new data with existing data on ABFS",
        "platform": "local",
        "input_format": "parquet",
        "output_format": "parquet",
        "log_level": "INFO",
        "max_retries": 3,
        "retry_delay": 2.0,
        "params": {
            "input_path": "abfs://data-lake/new/transactions.parquet",
            "existing_path": "abfs://data-lake/curated/transactions.parquet",
            "output_path": "abfs://data-lake/curated/transactions.parquet",
            "dedup_columns": ["transaction_id"],
        },
    },
    "s3_to_gcs": {
        "name": "s3_to_gcs",
        "description": "Copy data from S3 to GCS (cross-cloud)",
        "platform": "local",
        "input_format": "parquet",
        "output_format": "parquet",
        "log_level": "INFO",
        "max_retries": 5,
        "retry_delay": 5.0,
        "params": {
            "input_path": "s3://source-bucket/data/events.parquet",
            "output_path": "gs://dest-bucket/data/events.parquet",
        },
    },
}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Demonstrate with local paths (no cloud credentials needed)
    # In production, replace these with actual cloud URIs.
    print("Cloud Storage Example")
    print("=" * 60)
    print()
    print("This example demonstrates cloud storage path patterns.")
    print("To run with actual cloud storage, install the required fsspec")
    print("backends and set the appropriate credentials:")
    print()
    print("  S3:  uv add s3fs")
    print("  GCS: uv add gcsfs")
    print("  ABFS: uv add adlfs")
    print()
    print("Example config for S3 job:")
    print("-" * 40)
    import yaml

    print(yaml.dump(EXAMPLE_CONFIGS["s3_to_parquet"], default_flow_style=False))
