#!/bin/bash

# Format code with black, ruff, and isort
echo "Formatting code with black..."
black src/ tests/ examples/

echo "Formatting imports with isort..."
isort src/ tests/ examples/

echo "Linting with ruff..."
ruff check --fix src/ tests/ examples/

echo "Code formatting complete!"