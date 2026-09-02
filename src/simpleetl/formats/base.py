"""
Base classes for data format readers and writers.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator
import pandas as pd

# Conservative memory estimate per data row (bytes) for chunk-size
# calculations based on max_buffer_mb. Adjusted upward to stay safe.
_ESTIMATED_BYTES_PER_ROW = 1024


def _chunk_size_from_max_buffer(max_buffer_mb: float) -> int:
    """Compute a chunk size that keeps a single chunk under max_buffer_mb."""
    if max_buffer_mb <= 0:
        return 10000
    bytes_limit = max_buffer_mb * 1024 * 1024
    return max(1, int(bytes_limit / _ESTIMATED_BYTES_PER_ROW))


class DataReader(ABC):
    """Abstract base class for data readers."""

    @abstractmethod
    def read(self, source: Any, **kwargs) -> pd.DataFrame:
        """
        Read data from a source into a pandas DataFrame.

        Args:
            source: The data source (file path, URL, database connection, etc.)
            **kwargs: Additional format-specific parameters.

        Returns:
            pandas DataFrame containing the data.
        """
        pass

    def read_chunks(
        self, source: Any, chunk_size: int = 10000, max_buffer_mb: float = 0, **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Read data from a source in chunks.

        Default implementation reads all data and yields it as a single chunk.
        Subclasses should override for true streaming support.

        Args:
            source: The data source.
            chunk_size: Number of rows per chunk.
            max_buffer_mb: Maximum memory in MB allowed for a single chunk.
                When > 0, chunk_size is computed from this limit.
            **kwargs: Additional format-specific parameters.

        Yields:
            pandas DataFrame chunks.
        """
        if max_buffer_mb > 0:
            chunk_size = _chunk_size_from_max_buffer(max_buffer_mb)  # noqa: F841
        yield self.read(source, **kwargs)


class DataWriter(ABC):
    """Abstract base class for data writers."""

    @abstractmethod
    def write(self, data: pd.DataFrame, destination: Any, **kwargs) -> None:
        """
        Write a pandas DataFrame to a destination.

        Args:
            data: pandas DataFrame to write.
            destination: The destination (file path, URL, database connection, etc.)
            **kwargs: Additional format-specific parameters.
        """
        pass

    def write_chunks(
        self, data_iterator: Iterator[pd.DataFrame], destination: Any, **kwargs
    ) -> None:
        """
        Write data from an iterator of DataFrames.

        Default implementation concatenates all chunks and writes at once.
        Subclasses should override for true streaming support.

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: The destination.
            **kwargs: Additional format-specific parameters.
        """
        chunks = list(data_iterator)
        if not chunks:
            self.write(pd.DataFrame(), destination, **kwargs)
        else:
            self.write(pd.concat(chunks, ignore_index=True), destination, **kwargs)
