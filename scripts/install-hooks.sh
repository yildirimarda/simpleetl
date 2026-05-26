#!/bin/bash

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install

# Install pre-commit itself if not installed
if ! command -v pre-commit &> /dev/null; then
    echo "Installing pre-commit..."
    pip install pre-commit
fi

echo "Pre-commit hooks installed successfully!"
echo "Run 'pre-commit run --all-files' to check all files"