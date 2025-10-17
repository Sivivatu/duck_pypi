-include .env
export

DBT_FOLDER=transform/pypi_duckdb_stats
DBT_TARGET=dev

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

pypi-dbt:
	cd $$DBT_FOLDER && \
	dbt run \
# 		--vars '{"start_date": "'$$DBT_START_DATE'","end_date": "'$$DBT_END_DATE'"}' \
		--target $$DBT_TARGET

pypi-dbt-test:
	cd $$DBT_FOLDER && \
	dbt test --target $$DBT_TARGET \
		--vars '{"start_date": "2023-04-01","end_date": "2024-04-07"}' \


format:
	ruff format .

test:
	uv run pytest tests -v --cov=ingestion

aws-sso-creds:
# DuckDB aws creds doesn't support loading from sso, so this create temporary creds file
	aws configure export-credentials --profile $$AWS_PROFILE --format env-no-export | \
	grep -E 'AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN' | \
	sed -e 's/AWS_ACCESS_KEY_ID/aws_access_key_id/' \
		-e 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/' \
		-e 's/AWS_SESSION_TOKEN/aws_session_token/' \
		-e 's/^/ /' -e 's/=/ =/' | \
	awk -v profile="$$AWS_PROFILE" 'BEGIN {print "["profile"]"} {print}' > ~/.aws/credentials