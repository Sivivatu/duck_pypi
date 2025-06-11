
pypi-ingest:
	uv run python -m ingestion.pipeline

format:
	ruff format .

test:
	uv run pytest tests -v --cov=ingestion
