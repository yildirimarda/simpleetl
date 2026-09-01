# Changelog

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
