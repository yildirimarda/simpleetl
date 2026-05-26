# SimpleETL Framework Guidelines

## Code Style
- Follow PEP 8 for Python code
- Use type hints for all function signatures
- Write docstrings for all public classes and methods
- Keep lines to a maximum of 88 characters
- Use ruff for linting and black for formatting

## Documentation
- All code comments in English
- User and developer documentation in the `docs/` directory
- API reference generated from docstrings

## Testing
- Write unit tests for all new functionality
- Aim for 95%+ test coverage
- Use pytest for testing
- Use pytest-cov for coverage reporting
- Use uv virtual environment when running tests, running ruff, mypy, syntax check, etc.

## Git Workflow
- Create descriptive branch names for features/fixes
- Write clear commit messages explaining the "why"
- Pull requests should include tests and documentation
- All CI checks must pass before merging

## Dependencies
- Manage dependencies with uv
- Keep dependencies minimal and up-to-date
- Add new dependencies only when necessary
- Update pyproject.toml when adding/removing dependencies

## Project Structure
- `src/`: Main source code
- `tests/`: Unit and integration tests
- `examples/`: Example ETL jobs and configurations
- `docs/`: User and developer documentation
- `configs/`: Example configuration files
- `docker/`: Docker-related files
- `k8s/`: Kubernetes manifests

## Implementation Principles
- Favor simplicity and readability over premature optimization
- Make the framework easy to extend and customize
- Provide clear error messages and logging
- Support multiple platforms (local, AWS Glue, Databricks, Azure Synapse)
- Support multiple data formats (CSV, JSON, Parquet, etc.)