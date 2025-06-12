# Stage 1: Base
FROM python:3.12 AS base

# Install UV via pip
# Install UV via image copy
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# RUN pip install uv --no-cache-dir

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

# Copy the project into the container
COPY .. /workspaces/duck_pypi

# Install the project in editable mode
RUN uv run pip install --no-cache-dir -e .

