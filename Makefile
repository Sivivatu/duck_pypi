-include .env
export


pypi-ingest:
	uv run python -m ingestion.pipeline \
		--start_date $$START_DATE \
		--end_date $$END_DATE \
		--pypi_project $$PYPI_PROJECT \
		--table_name $$TABLE_NAME \
		--gcp_project $$GCP_PROJECT \
		--timestamp_column $$TIMESTAMP_COLUMN \
		--destination $$DESTINATION \
		--s3_path $$S3_PATH \
		--aws_profile $$AWS_PROFILE \

# optional s3-upload args

format:
	ruff format .

test:
	uv run pytest tests -v --cov=ingestion
