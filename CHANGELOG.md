# Changelog

## [0.4.0](https://github.com/yildirimarda/simpleetl/compare/v0.3.0...v0.4.0) (2026-09-02)


### Features

* add test skeleton to simpleetl --init scaffold ([#47](https://github.com/yildirimarda/simpleetl/issues/47)) ([e383fbc](https://github.com/yildirimarda/simpleetl/commit/e383fbc2035bdf9fd7fa7e383d90caa7176a3f71))
* backpressure and bounded memory for streaming reads ([#38](https://github.com/yildirimarda/simpleetl/issues/38)) ([2963c38](https://github.com/yildirimarda/simpleetl/commit/2963c38330f24d49a3183fc4ed969d5f94a26c15))
* CDC ingestion module with Debezium fixtures and integration tests ([#41](https://github.com/yildirimarda/simpleetl/issues/41)) ([dbafac9](https://github.com/yildirimarda/simpleetl/commit/dbafac9918a19eb2cc872aecd435191da59e1fc1))
* dry-run mode --dry-run N with pipeline execution, schema print, no writes ([#48](https://github.com/yildirimarda/simpleetl/issues/48)) ([5d6b136](https://github.com/yildirimarda/simpleetl/commit/5d6b136afbeee9f283652bab41dcb95d047f6c01))
* make benchmark scripts pass engine=pandas ([#50](https://github.com/yildirimarda/simpleetl/issues/50)) ([2646597](https://github.com/yildirimarda/simpleetl/commit/26465974171b97371b05af8fcba14ad5189b7867))
* predicate pushdown for JDBC sources - translate filter configs to SQL WHERE clauses ([#43](https://github.com/yildirimarda/simpleetl/issues/43)) ([8685ced](https://github.com/yildirimarda/simpleetl/commit/8685ced5f7779c16b68fbfbdae6489a519a9b9ac))
* publish benchmark doc with pandas-only 1M-row baseline results ([#49](https://github.com/yildirimarda/simpleetl/issues/49)) ([05044aa](https://github.com/yildirimarda/simpleetl/commit/05044aabaead828974f3cdfe725b550dd1c30a38))
* quality report artifact with HTML/JSON output and row samples ([#46](https://github.com/yildirimarda/simpleetl/issues/46)) ([eded4f6](https://github.com/yildirimarda/simpleetl/commit/eded4f61ff1af605f26abc0bba6a55ca61bd9528))
* real Unity Catalog integration on Databricks platform ([#42](https://github.com/yildirimarda/simpleetl/issues/42)) ([1087ec5](https://github.com/yildirimarda/simpleetl/commit/1087ec5b2af71abb1d6a52037be83bc70f616e5b))
* retry with exponential backoff + jitter and circuit breaker for rest_api and database sources ([#40](https://github.com/yildirimarda/simpleetl/issues/40)) ([b3ff739](https://github.com/yildirimarda/simpleetl/commit/b3ff7398ac7201c7995bf24b4e1052a07cc5043c))
* serve Prometheus metrics endpoint opt-in via config ([#45](https://github.com/yildirimarda/simpleetl/issues/45)) ([a757097](https://github.com/yildirimarda/simpleetl/commit/a757097a094bc7a56ab2f2e0fc9052612fee74c5))
* transactional sink contract for exactly-once file writes ([#39](https://github.com/yildirimarda/simpleetl/issues/39)) ([9c2ece3](https://github.com/yildirimarda/simpleetl/commit/9c2ece3245c2462619264a12a8fa7c31ea732ead))
* wire GlueCatalogWriter into ETL lifecycle behind config flag ([#44](https://github.com/yildirimarda/simpleetl/issues/44)) ([b37ecbd](https://github.com/yildirimarda/simpleetl/commit/b37ecbd6d8e06c56df3af992b857d3eb91370d6b))


### Bug Fixes

* **test:** compare min-of-runs, not mean, for microsecond-scale DAG benchmarks — CI noise only inflates upward ([d7c62fa](https://github.com/yildirimarda/simpleetl/commit/d7c62fad185bcb6d740c1eb3dddc515aa7f17f4c))

## [0.3.0](https://github.com/yildirimarda/simpleetl/compare/v0.2.0...v0.3.0) (2026-09-01)


### Features

* add batch_size config parameter to control chunk size in streaming mode ([#30](https://github.com/yildirimarda/simpleetl/issues/30)) ([fe4f7bf](https://github.com/yildirimarda/simpleetl/commit/fe4f7bf649b896940063b2676395364b23fb59e4))
* document and test format_options in ETLJobConfig ([#29](https://github.com/yildirimarda/simpleetl/issues/29)) ([e99bfa5](https://github.com/yildirimarda/simpleetl/commit/e99bfa51fbf3cefb69ed18283a2d38127ea96cb9))
* export job_timer and TimerContext from simpleetl.core ([#31](https://github.com/yildirimarda/simpleetl/issues/31)) ([1f7c979](https://github.com/yildirimarda/simpleetl/commit/1f7c979537020170eb25aa70aa197149749b6eba))
* fully integrate Table database abstraction ([#32](https://github.com/yildirimarda/simpleetl/issues/32)) ([43373e6](https://github.com/yildirimarda/simpleetl/commit/43373e66af18a46929c10773d0c5c561152a47c0))


### Bug Fixes

* align .python-version with CI (3.11) to avoid pyiceberg build failure (missing cc) ([#35](https://github.com/yildirimarda/simpleetl/issues/35)) ([5dce1e2](https://github.com/yildirimarda/simpleetl/commit/5dce1e2bf82ff2219838a2ff4504fff4fa8a414f))


### Documentation

* review examples and docs, reconcile version refs to 0.2.0 ([#27](https://github.com/yildirimarda/simpleetl/issues/27)) ([7bfc820](https://github.com/yildirimarda/simpleetl/commit/7bfc820c0a82bf44a09d147db9960a00e73bca0a))
* update performance benchmark docs (v0.2.0, Linux env); add verification test ([#25](https://github.com/yildirimarda/simpleetl/issues/25)) ([2ac0954](https://github.com/yildirimarda/simpleetl/commit/2ac0954f402a976a276b2cfb4727baccdb955186))
* update README quickstart to use top-level read()/write() functions ([#34](https://github.com/yildirimarda/simpleetl/issues/34)) ([f93513d](https://github.com/yildirimarda/simpleetl/commit/f93513d529f3a41d6b23c5e3a46d67b835966dfc))

## [0.2.0](https://github.com/yildirimarda/simpleetl/compare/v0.1.0...v0.2.0) (2026-09-01)


### Features

* deep refactor of polars engine abstraction ([#21](https://github.com/yildirimarda/simpleetl/issues/21)) ([3bab90e](https://github.com/yildirimarda/simpleetl/commit/3bab90e0ecae5656e21929b929cd8361fdb70cee))
* export new public classes from __init__ ([#20](https://github.com/yildirimarda/simpleetl/issues/20)) ([b22c3da](https://github.com/yildirimarda/simpleetl/commit/b22c3da28f67af59727515ed99c43843e33e46d0))
* wire new hooks (metrics, lineage, provenance, quality) into ETLJob lifecycle from config ([#19](https://github.com/yildirimarda/simpleetl/issues/19)) ([9b9c827](https://github.com/yildirimarda/simpleetl/commit/9b9c827cbdd1556386830f375b3edcfc33fad950))


### Bug Fixes

* derive __version__ from installed metadata so release bumps cannot drift; test compares against pyproject instead of a hardcoded string ([657cbab](https://github.com/yildirimarda/simpleetl/commit/657cbabf6c020b2f71817563bb177aea5eec4faf))
* reconcile version numbering to 0.1.0 across docs and source ([#10](https://github.com/yildirimarda/simpleetl/issues/10)) ([8167382](https://github.com/yildirimarda/simpleetl/commit/81673824f0ac66208c3c996dd571e1a980835c6f))


### Documentation

* complete documentation index links and add review test ([#18](https://github.com/yildirimarda/simpleetl/issues/18)) ([f3e4e9e](https://github.com/yildirimarda/simpleetl/commit/f3e4e9ea2c64a5bbc90519a38a6db2574ec9a4e2))
* document deferred Snowflake/BigQuery live-account validation; add integration scaffold ([#23](https://github.com/yildirimarda/simpleetl/issues/23)) ([a26cafc](https://github.com/yildirimarda/simpleetl/commit/a26cafc2a6627feaf3e052b0e4b3ba0d38dc4626))

## 0.1.0 (2026-09-01)


### ⚠ BREAKING CHANGES

* drop Python 3.9 (EOL; codebase uses 3.10+ union syntax throughout)

### Features

* **ci:** add release-please; its tag triggers the existing PyPI publish workflow ([c079b02](https://github.com/yildirimarda/simpleetl/commit/c079b024870b04969d2386a84238418a7d1f7de6))
* drop Python 3.9 (EOL; codebase uses 3.10+ union syntax throughout) ([5d8f3f4](https://github.com/yildirimarda/simpleetl/commit/5d8f3f4e50b73193e20949fc15ab75785ca8a07f))
* v1.1.0 — Jinja2 templates, DuckDB, REST API, Delta Lake, profiling ([19a3ad6](https://github.com/yildirimarda/simpleetl/commit/19a3ad659d2d27fce276aa5e4d272ac0fb2f28ef))


### Bug Fixes

* **ci:** pin matrix python over .python-version, install all extras, skip local package in pip-audit; format with ruff ([071cee9](https://github.com/yildirimarda/simpleetl/commit/071cee9685e3a711cc726832948e8a737a997035))
* **ci:** pre-create tmp_output mount so the non-root container user can write; assert e2e output exists ([8ae020e](https://github.com/yildirimarda/simpleetl/commit/8ae020e646bb73c9124aa2dfe1a5db578cf9fff7))
* **ci:** stop bare 'uv run' from re-syncing the venv (--no-sync), install extras+type stubs for mypy, drop pip-audit --strict ([04bc2fd](https://github.com/yildirimarda/simpleetl/commit/04bc2fdf70b6f8f1a27049113140877eaeca4ccc))
* **cli:** do not coerce template_vars None to {}, which forced jinja2 for plain configs ([4f7a310](https://github.com/yildirimarda/simpleetl/commit/4f7a3108f1d9de5a4d5d2a4181d611307d1c0df0))
* **cli:** put the working directory on sys.path before importing job classes ([29d3342](https://github.com/yildirimarda/simpleetl/commit/29d334241a58b66fd5ae490cca3ab5e70d728c14))
* derive __main__.py path from package in test, widen mypy ignore, bump aiohttp past PYSEC advisories ([e465930](https://github.com/yildirimarda/simpleetl/commit/e46593041a06f0ad0dde0f8a79dfefed60c4d8f7))
* **docker:** install monitoring extra and stop uv run re-syncing at container start ([0477451](https://github.com/yildirimarda/simpleetl/commit/04774511da190a951fcc7a592c820e4a699d6693))
* **docker:** install project into venv after copying source; copy README/LICENSE for build ([45b4a1f](https://github.com/yildirimarda/simpleetl/commit/45b4a1f160b9b25d49975a86a916c4852cd87bce))


### Documentation

* bump api-reference version to v1.1.0 ([10e19fd](https://github.com/yildirimarda/simpleetl/commit/10e19fd7e08b0c68e3a4b5b083b1cc5969337d23))
