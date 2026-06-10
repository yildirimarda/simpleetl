# Snowflake & BigQuery Warehouses

SimpleETL v1.3 adds first-class dialect support for the two dominant
cloud data warehouses: native `MERGE` upserts and DDL generation. No
driver is imported by SimpleETL itself — SQLAlchemy resolves the dialect
from the connection URL at runtime.

## Installation

```bash
pip install simpleetl[snowflake]   # snowflake-sqlalchemy
pip install simpleetl[bigquery]    # sqlalchemy-bigquery
```

## Connection URLs

```
snowflake://user:password@account/database/schema
bigquery://my-project/my_dataset
```

These work everywhere a database URL works: `DatabaseReader`,
`DatabaseWriter`, the `Table` abstraction, and job configs.

## Native MERGE Upserts

```python
from simpleetl import Table

table = Table(
    "orders",
    connection_string="snowflake://user:pass@account/db/schema",
)
table.upsert(df, key_columns=["order_id"])
```

Dialect-specific behavior:

- **Snowflake** — rows are staged in a `CREATE TEMPORARY TABLE ... LIKE`
  staging table, then applied with a single atomic
  `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN
  INSERT`. The staging table is dropped in a `finally` block.
- **BigQuery** — same shape with backtick-quoted identifiers and a real
  (non-TEMP) staging table with a unique suffix, since BigQuery TEMP
  tables only exist inside scripts. Also dropped in `finally`.
- Any other dialect continues to use the generic temp-table
  DELETE+INSERT fallback.

## DDL Generation

```python
from simpleetl import Schema, generate_ddl

schema = Schema.from_dataframe(df)
print(generate_ddl(schema, "analytics.orders", dialect="snowflake"))
print(generate_ddl(schema, "analytics.orders", dialect="bigquery"))
```

Type mappings:

| Logical type | Snowflake | BigQuery |
|--------------|-----------|----------|
| integer | NUMBER | INT64 |
| float | FLOAT | FLOAT64 |
| string | VARCHAR | STRING |
| boolean | BOOLEAN | BOOL |
| timestamp | TIMESTAMP_NTZ / TIMESTAMP_TZ | TIMESTAMP |
| date | DATE | DATE |
| struct / array / map | VARIANT | JSON |

`IF NOT EXISTS` and `NOT NULL` clauses are emitted consistently with the
other dialects.

## Credentials

Provide credentials the way the underlying drivers expect (URL fields,
environment, or the platform's auth). SimpleETL's secrets providers
(`${secrets://...}` references) work in warehouse URLs like in any other
config value — see [security.md](security.md).
