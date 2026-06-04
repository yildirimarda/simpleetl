"""
Data profiling module for SimpleETL.

Generates statistical summaries of DataFrames including per-column stats,
null rates, distinct counts, value distributions, and dataset-level metrics.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ColumnProfile:
    """Statistical profile of a single DataFrame column."""

    name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    distinct_pct: float
    min: Optional[Any]
    max: Optional[Any]
    mean: Optional[float]
    std: Optional[float]
    top_values: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ProfileReport:
    """Full statistical profile of a DataFrame."""

    row_count: int
    column_count: int
    memory_mb: float
    duplicate_row_count: int
    columns: List[ColumnProfile] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Return the report as a plain Python dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Return the report as a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        """Return the report as a Markdown string suitable for CLI output."""
        lines = [
            "# Data Profile Report",
            "",
            "## Dataset Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Row Count | {self.row_count:,} |",
            f"| Column Count | {self.column_count} |",
            f"| Memory | {self.memory_mb:.2f} MB |",
            f"| Duplicate Rows | {self.duplicate_row_count:,} |",
            "",
            "## Column Profiles",
            "",
        ]

        headers = [
            "Column", "Type", "Nulls", "Null%",
            "Distinct", "Distinct%", "Min", "Max", "Mean",
        ]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

        for col in self.columns:
            row = [
                col.name,
                col.dtype,
                str(col.null_count),
                f"{col.null_pct:.1f}%",
                str(col.distinct_count),
                f"{col.distinct_pct:.1f}%",
                str(col.min) if col.min is not None else "N/A",
                str(col.max) if col.max is not None else "N/A",
                f"{col.mean:.4f}" if col.mean is not None else "N/A",
            ]
            lines.append("| " + " | ".join(row) + " |")

        return "\n".join(lines)

    def to_html(self) -> str:
        """Return the report as a minimal HTML page."""
        rows = ""
        for col in self.columns:
            mean_str = f"{col.mean:.4f}" if col.mean is not None else "N/A"
            rows += (
                f"<tr><td>{col.name}</td><td>{col.dtype}</td>"
                f"<td>{col.null_count}</td><td>{col.null_pct:.1f}%</td>"
                f"<td>{col.distinct_count}</td><td>{col.distinct_pct:.1f}%</td>"
                f"<td>{col.min if col.min is not None else 'N/A'}</td>"
                f"<td>{col.max if col.max is not None else 'N/A'}</td>"
                f"<td>{mean_str}</td></tr>\n"
            )

        return (
            "<!DOCTYPE html>\n<html>\n<head><title>Data Profile Report</title></head>\n"
            "<body>\n"
            "<h1>Data Profile Report</h1>\n"
            "<h2>Summary</h2>\n"
            "<ul>\n"
            f"<li>Rows: {self.row_count:,}</li>\n"
            f"<li>Columns: {self.column_count}</li>\n"
            f"<li>Memory: {self.memory_mb:.2f} MB</li>\n"
            f"<li>Duplicate Rows: {self.duplicate_row_count:,}</li>\n"
            "</ul>\n"
            "<h2>Column Profiles</h2>\n"
            '<table border="1">\n'
            "<tr><th>Column</th><th>Type</th><th>Nulls</th><th>Null%</th>"
            "<th>Distinct</th><th>Distinct%</th><th>Min</th><th>Max</th>"
            "<th>Mean</th></tr>\n"
            f"{rows}"
            "</table>\n"
            "</body>\n</html>"
        )


class DataProfiler:
    """Generates a :class:`ProfileReport` from a pandas DataFrame.

    Args:
        top_n: Number of most-frequent values to include per column.
    """

    def __init__(self, top_n: int = 5) -> None:
        self.top_n = top_n

    def profile(self, df: pd.DataFrame) -> ProfileReport:
        """Profile *df* and return a :class:`ProfileReport`.

        Args:
            df: DataFrame to profile.

        Returns:
            ProfileReport with per-column and dataset-level statistics.
        """
        row_count = len(df)
        column_count = len(df.columns)
        memory_mb = float(df.memory_usage(deep=True).sum()) / (1024 * 1024)
        duplicate_row_count = int(df.duplicated().sum())

        columns: List[ColumnProfile] = []
        for col_name in df.columns:
            col = df[col_name]
            null_count = int(col.isna().sum())
            null_pct = (null_count / row_count * 100) if row_count > 0 else 0.0
            distinct_count = int(col.nunique(dropna=True))
            distinct_pct = (
                (distinct_count / row_count * 100) if row_count > 0 else 0.0
            )

            mean: Optional[float] = None
            std: Optional[float] = None
            col_min: Optional[Any] = None
            col_max: Optional[Any] = None

            try:
                if pd.api.types.is_numeric_dtype(col):
                    numeric = col.dropna()
                    if len(numeric) > 0:
                        mean = float(numeric.mean())
                        std = float(numeric.std()) if len(numeric) > 1 else 0.0
                        col_min = float(numeric.min())
                        col_max = float(numeric.max())
                else:
                    non_null = col.dropna()
                    if len(non_null) > 0:
                        col_min = str(non_null.min())
                        col_max = str(non_null.max())
            except (TypeError, ValueError):
                pass

            top_values: List[Dict[str, Any]] = []
            try:
                vc = col.value_counts(dropna=True).head(self.top_n)
                top_values = [
                    {"value": str(v), "count": int(c)}
                    for v, c in zip(vc.index, vc.values)
                ]
            except TypeError:
                pass

            columns.append(
                ColumnProfile(
                    name=str(col_name),
                    dtype=str(col.dtype),
                    null_count=null_count,
                    null_pct=round(null_pct, 2),
                    distinct_count=distinct_count,
                    distinct_pct=round(distinct_pct, 2),
                    min=col_min,
                    max=col_max,
                    mean=mean,
                    std=std,
                    top_values=top_values,
                )
            )

        return ProfileReport(
            row_count=row_count,
            column_count=column_count,
            memory_mb=round(memory_mb, 4),
            duplicate_row_count=duplicate_row_count,
            columns=columns,
        )
