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

# Install uv and dependencies
RUN pip install uv && \
    uv sync --frozen

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/

# Copy configuration and example files
COPY configs/ ./configs/
COPY examples/ ./examples/

# Create non-root user
RUN useradd -m -u 1000 etluser && \
    chown -R etluser:etluser /app
USER etluser

# Expose port for metrics (if needed)
EXPOSE 8000

# Set default command
ENTRYPOINT ["uv", "run", "python", "-m", "simpleetl"]
CMD ["--help"]