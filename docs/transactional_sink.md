# Transactional Sink Contract — Exactly-Once Writes

Every filesystem writer in SimpleETL provides exactly-once output by
default: data is written to a temporary path (`.tmp_<uuid>_` prefix) in the
same directory as the destination, then moved atomically (`os.rename` for
local paths; `fsspec` `mv` for cloud paths).  The temporary file is
cleaned up if any step fails.

Every JDBC writer (`DatabaseWriter`) provides exactly-once writes via a
staging-table + swap: a temporary staging table (`<table>_staging_` +
uuid) is created, loaded, and then the original target is dropped and the
staging table is renamed to the target inside a single SQLAlchemy
transaction.

## Guarantee Per Format

| Format | Sink Type | Guarantee | Mechanism |
|---|---|---|---|
| CSV | Filesystem | Exactly-once | Temp file + atomic rename |
| JSON | Filesystem | Exactly-once | Temp file + atomic rename |
| Parquet | Filesystem | Exactly-once | Temp file + atomic rename |
| Avro | Filesystem | Exactly-once | Temp file + atomic rename |
| ORC | Filesystem | Exactly-once | Temp file + atomic rename |
| XML | Filesystem | Exactly-once | Temp file + atomic rename |
| Excel (.xlsx) | Filesystem | Exactly-once | Temp file + atomic rename |
| Delta Lake | Filesystem / Directory | Exactly-once | Temp directory + atomic rename |
| Database (JDBC) | JDBC | Exactly-once | Staging table + swap |

Cloud storage paths (`s3://`, `gs://`, `abfss://`) use the same mechanism,
with `fsspec` `mv` for the atomic move.  Where a backend does not support
atomic `mv`, the framework falls back to a read-then-write copy, which is
not strictly atomic but is the best available guarantee for that backend.
