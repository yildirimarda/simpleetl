# Polars Interop & IO Acceleration

SimpleETL v1.3 adds [Polars](https://pola.rs/) interop: zero-copy bridges
between pandas and Polars, an escape hatch for hot transformation paths,
and a Polars-powered fast path inside the CSV/Parquet readers and writers.

The public API stays pandas-typed — Polars is an acceleration layer, not
an engine switch.

## Installation

```bash
pip install simpleetl[polars]
```

## Interop Bridges

```python
from simpleetl import to_polars, from_polars, is_polars_available

pldf = to_polars(df)      # pandas -> Polars (Arrow-backed)
df = from_polars(pldf)    # Polars DataFrame or LazyFrame -> pandas
```

## Hot-Path Transformations

Run a single expensive transformation in Polars without leaving the
pandas pipeline:

```python
from simpleetl import polars_transform, polars_sql_transform

# Callable form — fn receives a Polars DataFrame, may return a LazyFrame
fast = polars_transform(df, lambda pldf: pldf.filter(pldf["revenue"] > 0))

# SQL form (Polars SQLContext) — complements the DuckDB sql_transform
agg = polars_sql_transform(
    df, "SELECT region, SUM(revenue) AS total FROM df GROUP BY region"
)
```

## IO Fast Path

`CSVReader`/`CSVWriter` and `ParquetReader`/`ParquetWriter` accept an
`engine` option:

```python
from simpleetl import CSVReader

df = CSVReader().read("data.csv", engine="polars")
```

Or via config:

```yaml
format_options:
  csv:
    engine: polars
```

### Fast-Path Rules

- Applies to plain local paths only; cloud paths (`s3://`, `gs://`,
  `abfss://`) use the existing fsspec path with a debug log.
- A minimal, safe set of kwargs is translated (CSV read: `usecols`,
  `sep`/`delimiter`, `nrows`; CSV write: `sep`, `header`, `columns`;
  Parquet read: `columns`; Parquet write: `compression`). Unmapped
  kwargs fall back to pandas with a debug log.
- `engine="polars"` without Polars installed logs a warning and falls
  back to pandas — IO never hard-fails on a missing optional package.
- Unknown engine names raise `ValueError`.
- The default `engine="pandas"` behavior is unchanged.

## When to Use What

| Scenario | Tool |
|----------|------|
| Large CSV/Parquet reads on local disk | `engine="polars"` |
| One expensive groupby/join in a pandas pipeline | `polars_transform` |
| SQL on an in-memory frame, Polars semantics | `polars_sql_transform` |
| SQL on an in-memory frame, DuckDB semantics | `sql_transform` (v1.1) |
| Whole pipeline in Polars | use Polars directly — SimpleETL's API is pandas-typed |
