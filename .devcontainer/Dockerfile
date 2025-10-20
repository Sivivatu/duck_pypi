# Stage 1: Base
# FROM python:3.12 AS base
# Use a Python image with uv pre-installed
# FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base
FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS base

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

# Install UV via pip
# Install UV via image copy
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# RUN pip install uv --no-cache-dir
RUN curl https://install.duckdb.org | sh

# Add DuckDB CLI to PATH
ENV PATH="/root/.duckdb/cli/latest:${PATH}"

# (Optional) confirm it's accessible
RUN duckdb --version || echo "DuckDB not yet installed"

# UV environment settings
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    # UV_PYTHON=/usr/local/bin/python3.12 \
    UV_PROJECT_ENVIRONMENT=/usr/local

# Stage 2: Development
FROM base AS development

# Set the working directory
WORKDIR /workspaces/duck_pypi
# Copy UV config files
COPY pyproject.toml uv.lock ./

# Install development dependencies
RUN --mount=type=cache,target=/root/.cache/ \
    uv sync --locked


# Stage 3: Prod
FROM base AS production

# Set the working directory
WORKDIR /workspaces/duck_pypi

# Copy UV configuration files and codebase
COPY Makefile pyproject.toml uv.lock ./
COPY ./ingestion ./ingestion
COPY ./transform ./transform

# Copy the project into the container
COPY .. /workspaces/duck_pypi

# Install only runtime dependencies
RUN --mount=type=cache,target=/root/.cache \
    uv sync --locked --no-dev --no-install-project

# Run additional setup (e.g., dbt dependencies)
RUN make dbt-deps

# Default command to keep container running for interactive `make` commands
CMD ["sleep", "infinity"]