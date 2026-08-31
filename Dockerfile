# SimpleETL Framework Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY LICENSE .

# Install uv and dependencies
# Dependencies only (cache-friendly layer — the project itself is not
# copied yet, so don't try to install it)
RUN pip install uv && \
    uv sync --frozen --no-install-project

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/

# Copy configuration and example files
COPY configs/ ./configs/
COPY examples/ ./examples/

# Now install the project itself into the venv (deps are already cached);
# the ENTRYPOINT runs with --no-sync, so this is the final word
RUN uv sync --frozen

# Create non-root user
RUN useradd -m -u 1000 etluser && \
    chown -R etluser:etluser /app
USER etluser

# Expose port for metrics (if needed)
EXPOSE 8000

# Set default command
ENTRYPOINT ["uv", "run", "--no-sync", "python", "-m", "simpleetl"]
CMD ["--help"]