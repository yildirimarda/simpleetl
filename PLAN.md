# SimpleETL Framework Project Plan

## Project Name: SimpleETL

## Overview
A professional, production-grade ETL framework written in Python. Designed to run on local, AWS Glue, Databricks, and Azure Synapse platforms. Supports all major data formats with a focus on simplicity, readability, and production readiness.

## Goals
1. **Package Management**: Use uv for fast, reliable Python package management.
2. **Testing**: Achieve 95%+ test coverage with comprehensive unit and integration tests.
3. **Documentation**: All code comments in English; comprehensive user and developer documentation.
4. **Platform Support**: Local development, AWS Glue, Databricks, Azure Synapse.
5. **Format Support**: CSV, JSON, Parquet, Avro, ORC, XML, Excel, JDBC databases, etc.
6. **Production Ready**: Docker and Kubernetes configurations, CI/CD pipelines, logging, monitoring hooks.
7. **Clean Repository**: Proper .gitignore, licensing, and minimal, focused codebase.

---

## Phase 0: Project Setup — COMPLETE ✅

- [x] Initialize project with uv
- [x] Create base directory structure
- [x] Configure pyproject.toml with dependencies
- [x] Set up initial Git repository
- [x] Create CLAUDE.md with project guidelines
- [x] Write basic README.md

## Phase 1: Core ETL Framework — COMPLETE ✅

- [x] Design base ETL job interface/abstract class
- [x] Implement configuration loading (YAML/JSON)
- [x] Create reader/writer abstractions for different formats
- [x] Add basic transformation capabilities (filter, map, aggregate)
- [x] Implement job execution engine with logging
- [x] Add error handling and retry mechanisms
- [x] Write unit tests for core components

## Phase 2: Platform Adaptors — COMPLETE ✅

- [x] Create platform-specific runners (Local, Glue, Databricks, Synapse)
- [x] Implement platform detection and configuration
- [x] Write integration tests for each platform

## Phase 3: Format Support Expansion — COMPLETE ✅

- [x] Implement readers/writers for all major formats
- [x] Add format auto-detection based on file extension
- [x] Write format-specific tests

## Phase 4: Production Readiness (Initial) — COMPLETE ✅

- [x] Add Dockerfile and docker-compose.yml
- [x] Create Kubernetes manifests
- [x] Implement structured logging (JSON format)
- [x] Add metrics collection hooks (Prometheus compatible)
- [x] Create configuration templates for different environments
- [x] Set up pre-commit hooks
- [x] CLI entry point (argparse-based)
- [x] Health/Readiness HTTP endpoints
- [x] Data quality checks module
- [x] LICENSE file
- [x] Comprehensive documentation (docs/)

## Phase 5: CI/CD and Release — COMPLETE ✅

- [x] Configure GitHub Actions CI pipeline
- [x] Set up automated testing on push/pull request
- [x] Create release workflow (tagging, publishing to PyPI)
- [x] Add dependency vulnerability scanning
- [x] Performance benchmarking suite

## Phase 6: Production-Grade Features — COMPLETE ✅

### Current Status (2026-05-26)
- **Tests**: 1521 passed, 2 skipped ✅
- **Coverage**: 96% ✅
- **Linting**: ruff clean (0 errors) ✅
- **Type Checking**: mypy clean (0 errors) ✅
- **Dependencies**: Lightweight core (6 deps), optional extras for cloud/spark/db ✅

### 6.1 Streaming & Chunked Processing — COMPLETE ✅

- [x] Chunked/chunk_size parameter in base reader/writer
- [x] Chunked read/write for Parquet (iter_batches + ParquetWriter)
- [x] Chunked read/write for CSV
- [x] Chunked read/write for Database
- [x] Chunked read/write for JSON
- [x] Chunked read/write for Avro, ORC
- [x] Support for reading/writing compressed files (gzip, snappy)
- [x] Batch processing mode via transform_chain

### 6.2 Incremental / Delta Loading — COMPLETE ✅

- [x] Watermark-based incremental extraction
- [x] Checkpoint/resume support for long-running jobs
- [x] Merge/UPSERT operations in DatabaseWriter
- [x] State management between job runs (state store abstraction)
- [x] `incremental_key` and `high_watermark` in job config

### 6.3 Schema Management — COMPLETE ✅

- [x] Schema inference from data sources
- [x] Schema evolution support (add/remove/rename columns)
- [x] Schema registry interface (file-based)
- [x] DDL generation for database targets
- [x] Column mapping and renaming framework
- [x] High test coverage (96%+)
- [x] Support for nested/complex types (structs, arrays, maps)

### 6.4 Cloud Storage Support — COMPLETE ✅

- [x] S3 support (s3:// paths) via fsspec
- [x] GCS support (gs:// paths) via fsspec
- [x] Azure Blob/ADLS support (abfss:// paths) via fsspec
- [x] Unified filesystem abstraction layer
- [x] Cloud read/write tested for all 7 formats (CSV, JSON, Parquet, Avro, ORC, Excel, XML)

### 6.5 Connection Management — COMPLETE ✅

- [x] Connection pooling for database readers/writers
- [x] Integration with AWS Secrets Manager, Azure Key Vault, HashiCorp Vault
- [x] Environment variable interpolation in config files (${VAR} syntax)
- [x] SSL/TLS configuration for database connections
- [x] Connection timeout and retry configuration

### 6.6 Error Handling & Recovery — COMPLETE ✅

- [x] Dead letter queue (DLQ) for failed records
- [x] Partial failure handling (continue on bad records, collect errors)
- [x] Checkpointing for resumable jobs
- [x] Transaction management for database writes
- [x] Circuit breaker pattern for external service calls
- [x] Error classification (transient vs permanent)
- [x] Retry with jitter

### 6.7 Parallelism & Performance — COMPLETE ✅

- [x] Multi-threaded read/write for independent operations
- [x] Multi-process support for CPU-bound transformations
- [x] Parallel partition processing
- [x] Data partitioning strategy for writes (partition by column)

### 6.8 Job Orchestration & DAG — COMPLETE ✅

- [x] DAG-based job dependency definition
- [x] Fan-out/fan-in execution patterns
- [x] Conditional job execution
- [x] Integration hooks for Airflow, Prefect, Dagster
- [x] Job scheduling capability (cron expressions)
- [x] Multi-job runner with dependency resolution

### 6.9 Extensibility & Plugin System — COMPLETE ✅

- [x] Plugin registration system for custom formats
- [x] Hook/interceptor system (pre/post extract, transform, load)
- [x] Custom transformer registry
- [x] Event system / callbacks
- [x] Middleware pipeline for data processing
- [x] Entry_points-based external plugin discovery

### 6.10 Platform Integration (Real) — COMPLETE ✅

- [x] AWS Glue: boto3 integration, Glue context, DynamicFrame, S3
- [x] Dependencies reorganized: pyspark, boto3, db drivers are now optional extras
- [x] AWS Glue: bookmark API calls (stubs)
- [x] AWS Glue: pandas_to_dynamic_frame
- [x] Databricks: Spark session, DBFS, Delta Lake, Databricks Connect
- [x] Azure Synapse: Synapse Spark, ABFS, Synapse SDK
- [x] Unified Spark-based processing engine option

### 6.11 Data Lineage & Observability — COMPLETE ✅

- [x] Data lineage tracking (source → transform → destination)
- [x] LineageTracker with event recording and filtering
- [x] LineageHook for automatic lineage capture
- [x] Data freshness tracking (DataFreshnessTracker)
- [x] Audit trail of transformations
- [x] Integration with OpenLineage
- [x] Per-record provenance
- [x] Alerting integration hooks

### 6.12 Security — COMPLETE ✅

- [x] PII detection and masking (detect_pii_columns, detect_pii_values, mask_pii)
- [x] Column-level encryption support (ColumnEncryptor with Fernet)
- [x] Audit logging for data access (AuditLogger with file output)
- [x] Role-based access control hooks (RBACPolicy, apply_rbac_filter)
- [x] Secure credential handling throughout

### 6.13 Testing & Quality — COMPLETE ✅

- [x] Unit tests for all core modules (1521 tests)
- [x] Fix all failing tests (CLI import, DAG import, schema dup)
- [x] Integration tests with real databases (SQLite, PostgreSQL patterns)
- [x] End-to-end pipeline tests
- [x] conftest.py with shared fixtures
- [x] Improve coverage from 78% to 96% ✅
- [x] ruff linting clean (0 errors)
- [x] mypy type checking clean (0 errors)
- [x] Integration tests with real databases (PostgreSQL, MySQL)
- [x] Failure injection tests (network, disk, permissions)
- [x] Data volume tests (GB-scale)
- [x] Performance regression tests

### 6.14 Transformations — COMPLETE ✅

- [x] filter_data (column-based and function-based)
- [x] map_values (dict and callable mapping)
- [x] aggregate_data (groupby aggregation)
- [x] join_data (inner, left, right, outer joins)
- [x] union_data (concat with schema alignment)
- [x] deduplicate_data (distinct with subset columns)
- [x] with_column (add computed columns)
- [x] rename_columns / select_columns / drop_columns
- [x] fill_na / drop_na with strategies
- [x] sort_data / limit_rows / sample_data / distinct_data
- [x] cast_columns (safe type conversion with coerce mode)
- [x] when_otherwise (conditional column values)
- [x] add_computed_column (expression evaluation)
- [x] group_by_aggregate_data (enhanced groupby)
- [x] pivot_data / unpivot_data
- [x] transform_chain (sequential transformation pipeline)
- [x] window_functions (rank, dense_rank, lag, lead, row_number, percent_rank, cumsum, cume_dist)
- [x] string_operations (trim, upper, lower, replace, split, contains, regex_extract, length, substring, pad_left, pad_right)
- [x] date_operations (trunc, extract, diff, format, timezone, add, is_weekend, is_business_day)
- [x] TransformationChain (fluent chainable API)
- [x] chain() convenience function

### 6.15 Public API — COMPLETE ✅

- [x] Re-export key classes from package __init__.py
- [x] Top-level convenience functions
- [x] Version info and metadata

### 6.16 Quality Fixes — COMPLETE ✅

- [x] Fix 5 failing tests (tests/__init__.py, duplicate TestSchemaDiff class)
- [x] Fix 76 ruff linting errors (unused imports, unused variables, redefinition)
- [x] Fix all mypy type errors (config + targeted type: ignore)
- [x] Fix `read_partitioned` double-read performance bug
- [x] Fix `Schema.evolve()` dead code (unused dict comprehension)
- [x] Fix `LazyTransformation.optimize()` narrow `_apply_filter` heuristic
- [x] Fix `config.load_config()` ValidationError re-wrap
- [x] Fix `ETLJob.extract()` signature for incremental mode kwargs
- [x] Fix ORC reader/writer for PyArrow 24 API compatibility
- [x] Make dependencies optional: pyspark, boto3, db drivers, cloud SDKs
- [x] Make fastavro optional with pyarrow fallback for Avro reading
- [x] Add `tests/__init__.py` for importlib-based test discovery

---

## Phase 7: v1.0 Release — COMPLETE ✅ (2026-05-26)

- [x] All Phase 6 features implemented
- [x] Test coverage >= 95% — ACHIEVED: 96%
- [x] Documentation complete and reviewed
- [x] Docker builds for all target platforms
- [x] CI/CD pipeline passes on all branches
- [x] Code follows PEP 8 and passes linting (ruff, mypy)
- [x] Framework can handle datasets larger than memory (streaming/chunked)
- [x] Framework supports incremental/delta loading
- [x] Framework supports cloud platform integrations (Glue, Databricks, Synapse)
- [x] Framework supports S3, GCS, and ABFS paths
- [x] Dependencies reorganized as optional extras (lightweight core)
- [x] ruff + mypy clean (0 errors each)
- [x] All 1521 tests passing (0 failures)
- [x] Security audit completed (pip-audit blocking in CI, docs/security.md)
- [x] Performance benchmarks documented (benchmarks/, docs/performance.md)
- [x] Examples and docs reviewed
- [x] Lineage/audit/RBAC persistence added
- [x] Version bumped to 1.0.0

### 7.1 OpenLineage Integration — COMPLETE ✅

- [x] `OpenLineageConverter` class: converts `LineageEvent` → OpenLineage `RunEvent`
- [x] `LineageTracker.emit_openlineage()` method
- [x] HTTP emitter that POSTs to OpenLineage API endpoint
- [x] Configurable via `ETLJobConfig.openlineage_url`
- [x] Tests: `tests/test_lineage_openlineage.py` (38 tests)

### 7.2 Per-Record Provenance — COMPLETE ✅

- [x] `record_id` field on `LineageEvent`
- [x] `LineageTracker.record_provenance()` method
- [x] `ProvenanceHook` for automatic per-record tracking
- [x] `ProvenanceTracker` standalone class with O(1) lookups
- [x] Tests: `tests/test_lineage_provenance.py` (38 tests)

### 7.3 Alerting Integration Hooks — COMPLETE ✅

- [x] `AlertRule.evaluate()` with context
- [x] `AlertManager.check_and_dispatch()` → webhook/email/Slack stubs
- [x] Alert channels: `WebhookChannel`, `EmailChannel` (stub), `SlackChannel` (stub)
- [x] `AlertChannel` abstract base class
- [x] Tests: `tests/test_alerting.py` (33 tests)

### 7.4 Nested/Complex Type Schema Support — COMPLETE ✅

- [x] `StructType`, `ArrayType`, `MapType` classes in `schema.py`
- [x] `FieldDef` class for struct fields
- [x] `Schema.from_dataframe()` inference for nested types
- [x] DDL generation for nested types (PostgreSQL JSONB, MySQL JSON, etc.)
- [x] Tests: `tests/test_schema_nested.py` (58 tests)

### 7.5 Performance Benchmarks — COMPLETE ✅

- [x] `benchmarks/` directory with benchmark scripts
- [x] Read/write benchmarks for all formats (`benchmark_read_write.py`)
- [x] Transformation benchmarks — filter, join, aggregate, window (`benchmark_transformations.py`)
- [x] Streaming/chunked processing benchmarks (`benchmark_streaming.py`)
- [x] DAG operation benchmarks (`benchmark_dag.py`)
- [x] `docs/performance.md` with documented results

### 7.6 Security Audit — COMPLETE ✅

- [x] `pip-audit` blocking in CI
- [x] Review all error messages for information leakage
- [x] Verify no secrets in logs
- [x] Document security best practices in `docs/security.md`

### 7.7 Documentation Finalization — COMPLETE ✅

- [x] `docs/performance.md` — benchmark results
- [x] `docs/security.md` — security best practices
- [x] `docs/openlineage.md` — OpenLineage integration guide
- [x] `docs/provenance.md` — per-record provenance guide
- [x] `docs/alerting.md` — alerting integration guide
- [x] `docs/schema.md` — nested type support guide
- [x] Review and update `README.md` with v1.0 features
- [x] Final review of all docs for accuracy

---

## Definition of Done — COMPLETE ✅

- [x] All Phase 6 features implemented
- [x] Test coverage >= 95% — **ACHIEVED: 96%**
- [x] Documentation complete and accessible
- [x] Docker builds for all target platforms
- [x] CI/CD pipeline passes on all branches
- [x] Code follows PEP 8 and passes linting (ruff, mypy)
- [x] Framework can handle datasets larger than memory (streaming/chunked)
- [x] Framework supports incremental/delta loading
- [x] Framework supports cloud platform integrations (Glue, Databricks, Synapse)
- [x] Framework supports S3, GCS, and ABFS paths
- [x] Dependencies reorganized as optional extras (lightweight core)
- [x] ruff + mypy clean (0 errors each)
- [x] All 1521 tests passing (0 failures)
- [x] Security audit completed
- [x] Performance benchmarks documented
- [x] Examples and docs reviewed
- [x] Lineage/audit/RBAC persistence added
- [x] Version bumped to 1.0.0

## Success Metrics
- Framework can be instantiated and run a simple ETL job in <5 minutes of setup
- New contributors can understand and modify the codebase within 1 hour
- Framework handles production-scale data (GB+ volumes) efficiently
- Framework supports incremental processing for daily batch jobs
- Framework integrates with at least one major orchestrator (Airflow/Prefect/Dagster)
- Minimal dependencies outside of standard data engineering stack
