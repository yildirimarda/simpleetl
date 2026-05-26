"""
Example ETL job using SimpleETL framework.

Demonstrates the recommended pattern: configuration is loaded from a YAML
file rather than hardcoded, keeping the job class reusable across
environments (dev, staging, prod).
"""

import logging

import pandas as pd

from simpleetl.core.job import ETLJob
from simpleetl.core.metrics import get_metrics, job_timer
from simpleetl.formats import FormatFactory
from simpleetl.transformations import (
    aggregate_data,
    filter_data,
    map_values,
)


class SampleETLJob(ETLJob):
    """Example ETL job that processes customer data.

    Configuration is read from a YAML file. The ``run`` method's signature
    matches the pattern expected by ``ETLJob`` subclasses: it takes no
    required positional arguments and uses ``self.config.params`` to
    obtain runtime values.
    """

    def __init__(self, config):
        """Initialize the ETL job.

        Args:
            config: An ``ETLJobConfig`` instance, a path to a YAML/JSON
                config file, or a configuration dictionary.
        """
        super().__init__(config)
        self.metrics = get_metrics()

    @job_timer()
    def extract(self, source_file: str | None = None) -> pd.DataFrame:
        """Extract data from the source file.

        Args:
            source_file: Path to the input file. Falls back to
                ``self.config.params["source_file"]`` when not provided.
        """
        source_file = source_file or self.config.params.get(
            "source_file", "examples/sample_customers.csv"
        )
        self.logger.info("Extracting data from %s", source_file)

        reader = FormatFactory.get_reader(source_file)
        data = reader.read(source_file)

        self.logger.info(
            "Read %d records from %s", len(data), source_file,
            extra={"event": "data_read", "source": source_file,
                   "record_count": len(data)},
        )
        self.metrics.inc_counter("etl_records_extracted", len(data))

        return data

    @job_timer()
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the extracted data.

        The transformation logic reads optional parameters from the
        job configuration so the same class can be tuned per environment.
        """
        self.logger.info("Transforming data")

        # Filter out rows with missing email
        data = filter_data(data, filter_func=lambda row: row["email"] != "N/A")

        # Standardize country names
        country_mapping = self.config.params.get(
            "country_mapping",
            {"USA": "United States", "UK": "United Kingdom", "CA": "Canada"},
        )
        if "country" in data.columns and country_mapping:
            data = map_values(data, "country", country_mapping)

        # Calculate age groups
        if "age" in data.columns:
            data["age_group"] = pd.cut(
                data["age"],
                bins=[0, 30, 50, 100],
                labels=["Under 30", "30-50", "Over 50"],
            )

        # Aggregate by country and age group
        groupby_cols = self.config.params.get(
            "groupby_columns", ["country", "age_group"]
        )
        agg_spec = self.config.params.get(
            "agg_spec", {"customer_id": "count", "age": "mean"}
        )
        available_groupby = [c for c in groupby_cols if c in data.columns]
        available_agg = {
            k: v for k, v in agg_spec.items() if k in data.columns
        }

        if available_groupby and available_agg:
            aggregated = aggregate_data(
                data, groupby=available_groupby, agg=available_agg
            )
        else:
            aggregated = data

        self.logger.info(
            "Transformation complete: %d records", len(aggregated),
            extra={"event": "data_transform", "record_count": len(data)},
        )
        self.metrics.inc_counter("etl_records_transformed", len(data))

        return aggregated

    @job_timer()
    def load(
        self,
        data: pd.DataFrame,
        destination: str | None = None,
    ) -> None:
        """Load transformed data to the destination.

        Args:
            data: Transformed DataFrame.
            destination: Output path. Falls back to
                ``self.config.params["destination_file"]``.
        """
        destination = destination or self.config.params.get(
            "destination_file", "examples/aggregated_customers.parquet"
        )
        self.logger.info("Loading data to %s", destination)

        writer = FormatFactory.get_writer(destination)
        writer.write(data, destination)

        self.logger.info(
            "Wrote %d records to %s", len(data), destination,
            extra={"event": "data_write", "destination": destination,
                   "record_count": len(data)},
        )
        self.metrics.inc_counter("etl_records_loaded", len(data))

    def run(self) -> None:
        """Run the complete ETL job."""
        data = self.extract()
        data = self.transform(data)
        self.load(data)


class ExtractJob(SampleETLJob):
    """DAG node that only extracts data (no transform/load)."""

    def run(self) -> None:
        data = self.extract()
        # Write extracted data so downstream nodes can consume it
        output_path = self.config.params.get(
            "output_path", "examples/output/extracted.parquet"
        )
        writer = FormatFactory.get_writer(output_path)
        writer.write(data, output_path)
        self.logger.info("Extracted %d records to %s", len(data), output_path)


class TransformJob(ETLJob):
    """DAG node that transforms data produced by an upstream ExtractJob."""

    def run(self) -> None:
        input_path = self.config.params.get(
            "input_path", "examples/output/extracted.parquet"
        )
        output_path = self.config.params.get(
            "output_path", "examples/output/transformed.parquet"
        )
        filter_column = self.config.params.get("filter_column", "status")
        filter_value = self.config.params.get("filter_value", "active")

        reader = FormatFactory.get_reader(input_path)
        data = reader.read(input_path)

        # Filter by column value
        if filter_column in data.columns:
            data = data[data[filter_column] == filter_value]

        writer = FormatFactory.get_writer(output_path)
        writer.write(data, output_path)
        self.logger.info(
            "Transformed %d records to %s", len(data), output_path
        )


class LoadJob(ETLJob):
    """DAG node that loads transformed data to a final destination."""

    def run(self) -> None:
        input_path = self.config.params.get(
            "input_path", "examples/output/transformed.parquet"
        )
        destination_path = self.config.params.get(
            "destination_path",
            "examples/output/aggregated_customers.parquet",
        )

        reader = FormatFactory.get_reader(input_path)
        data = reader.read(input_path)

        writer = FormatFactory.get_writer(destination_path)
        writer.write(data, destination_path)
        self.logger.info(
            "Loaded %d records to %s", len(data), destination_path
        )


def _create_sample_data() -> str:
    """Create a sample CSV file for demonstration purposes."""
    sample_data = pd.DataFrame(
        [
            {
                "customer_id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "age": 25,
                "country": "USA",
                "revenue": 1000,
                "status": "active",
            },
            {
                "customer_id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "age": 35,
                "country": "UK",
                "revenue": 1500,
                "status": "active",
            },
            {
                "customer_id": 3,
                "name": "Bob Johnson",
                "email": "N/A",
                "age": 45,
                "country": "CA",
                "revenue": 2000,
                "status": "inactive",
            },
            {
                "customer_id": 4,
                "name": "Alice Brown",
                "email": "alice@example.com",
                "age": 28,
                "country": "USA",
                "revenue": 800,
                "status": "active",
            },
            {
                "customer_id": 5,
                "name": "Charlie Wilson",
                "email": "charlie@example.com",
                "age": 55,
                "country": "UK",
                "revenue": 3000,
                "status": "active",
            },
        ]
    )

    input_file = "examples/sample_customers.csv"
    sample_data.to_csv(input_file, index=False)
    return input_file


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create sample input data
    _create_sample_data()

    # Load configuration from YAML and run the job
    config_path = "examples/sample_job_config.yaml"
    job = SampleETLJob(config_path)
    job.run_with_error_handling()

    print("ETL job completed successfully!")
