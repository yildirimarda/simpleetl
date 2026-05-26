"""
Example ETL job that reads a CSV, filters by age, and writes the result to another CSV.
"""

from simpleetl.core.job import ETLJob
from simpleetl.formats.csv import CSVReader, CSVWriter
import pandas as pd


class ExampleCSVJob(ETLJob):
    """An example ETL job for processing CSV data."""

    def __init__(self, config):
        super().__init__(config)
        self.reader = CSVReader()
        self.writer = CSVWriter()

    def extract(self):
        """Extract data from the CSV file specified in the config."""
        input_path = self.config.params.get("input_path")
        if not input_path:
            raise ValueError("input_path must be specified in config.params")
        return self.reader.read(input_path)

    def transform(self, data):
        """
        Transform the data by filtering rows where the age column is >= the filter_min_value.
        """
        filter_column = self.config.params.get("filter_column", "age")
        filter_min_value = self.config.params.get("filter_min_value", 0)

        if filter_column not in data.columns:
            raise ValueError(f"Column '{filter_column}' not found in data")

        filtered_data = data[data[filter_column] >= filter_min_value]
        self.logger.info(
            f"Filtered data from {len(data)} rows to {len(filtered_data)} rows "
            f"based on {filter_column} >= {filter_min_value}"
        )
        return filtered_data

    def load(self, data):
        """Load the transformed data to the CSV file specified in the config."""
        output_path = self.config.params.get("output_path")
        if not output_path:
            raise ValueError("output_path must be specified in config.params")
        self.writer.write(data, output_path)
        self.logger.info(f"Data written to {output_path}")

    def run(self) -> None:
        """
        Execute the ETL job by calling extract, transform, and load in sequence.
        """
        data = self.extract()
        data = self.transform(data)
        self.load(data)


if __name__ == "__main__":
    # This allows running the job directly for testing
    import logging
    import os
    logging.basicConfig(level=logging.INFO)

    # Load the configuration from the example config file
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "example_job.yaml")
    job = ExampleCSVJob(config_path)
    job.run_with_error_handling()