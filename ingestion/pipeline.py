import os
import duckdb
import fire
from google.cloud.bigquery.client import Client
# from pandas import DataFrame

from ingestion.bigquery import (
    build_pypi_query,
    get_bigquery_client,
    get_bigquery_result,
)
from ingestion.models import PypiJobParameters, validate_table, FileDownloads
from ingestion.duck import (
    create_table_from_dataframe,
    load_aws_credentials,
    write_to_s3_from_duckdb,
    connect_to_md,
)
from loguru import logger

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ.pop("GRPC_TRACE", None)



def main(params: PypiJobParameters) -> None:
    raw_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    logger.info(F"Credentials path (raw): {raw_path}")
    # Strip surrounding quotes if a shell-quoted value was inserted into the
    # env file. Many editors or users accidentally include quotes — remove
    # them so the path is used correctly.
    if raw_path and ((raw_path.startswith('"') and raw_path.endswith('"')) or (raw_path.startswith("'") and raw_path.endswith("'"))):
        CREDENTIALS_PATH = raw_path[1:-1]
    else:
        CREDENTIALS_PATH = raw_path
    logger.info(f"Using Google credentials path: {CREDENTIALS_PATH}")
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    print("Ingestion pipeline started.")

    # Use the Pydantic model passed in (created from CLI args). Do not
    # unconditionally override it here — that prevents CLI / environment
    # values (including those provided by the Makefile via .env) from
    # being used.

    # Initialize BigQuery client
    bq_client: Client = get_bigquery_client(
        params=params, credentials_path=CREDENTIALS_PATH
    )

    # Build and execute the query
    query: str = build_pypi_query(params=params)

    pa_table = get_bigquery_result(query, bq_client)

    logger.info(f"Query executed successfully. Retrieved {len(pa_table)} rows.")
    logger.info(pa_table.slice(offset=0, length=5))
    logger.info("Validating table records...")
    validate_table(pa_table, FileDownloads)

    # # Process the results
    # for index, row in pa_table.iterrows():
    #     print(f"Package: {row['name']}, Version: {row['version']}")
    conn = duckdb.connect()
    create_table_from_dataframe(conn, params.table_name, "pa_table")
    logger.info(f"Created DuckDB table {params.table_name} from DataFrame.")

    logger.info(f"Destination(s) specified: {params.destination}")
    if "s3" in params.destination:
        logger.info("Preparing to write to S3.")
        load_aws_credentials(conn, params.aws_profile)
        write_to_s3_from_duckdb(
            conn, params.s3_path, params.table_name, params.timestamp_column
        )
        logger.info(f"Data written to S3 at {params.s3_path}/{params.table_name}.")
    if "motherduck" in params.destination:
        logger.info("Preparing to connect to MotherDuck.")
        motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
        if not motherduck_token:
            raise ValueError(
                "MOTHERDUCK_TOKEN environment variable is not set. Cannot connect to MotherDuck."
            )
        connect_to_md(conn, motherduck_token)
        logger.info("Connected to MotherDuck successfully.")
    if "local" in params.destination:
        logger.info("Preparing to write to local files.")
        logger.info("Writing file to parquet")
        conn.sql("COPY (SELECT * FROM pa_table) TO 'duckdb.parquet' (FORMAT 'parquet')")
        logger.info("Writing file to csv")
        conn.sql("COPY (SELECT * FROM pa_table) TO 'duckdb.csv' (FORMAT 'csv')")
        logger.info("Local files written successfully.")

    logger.info("Ingestion pipeline completed.")


if __name__ == "__main__":
    # Build PypiJobParameters from CLI kwargs or environment variables only.
    # Do not provide any in-code fallback defaults — require the caller
    # (Makefile/.env or the CLI) to supply values. If required values are
    # missing, raise an error instead of silently falling back.
    def _make_params(**kwargs):
        merged = {
            "start_date": kwargs.get("start_date") or os.getenv("START_DATE"),
            "end_date": kwargs.get("end_date") or os.getenv("END_DATE"),
            "pypi_project": kwargs.get("pypi_project") or os.getenv("PYPI_PROJECT"),
            "table_name": kwargs.get("table_name") or os.getenv("TABLE_NAME"),
            "gcp_project": kwargs.get("gcp_project") or os.getenv("GCP_PROJECT"),
            "timestamp_column": kwargs.get("timestamp_column")
            or os.getenv("TIMESTAMP_COLUMN"),
            "destination": kwargs.get("destination") or os.getenv("DESTINATION"),
            "s3_path": kwargs.get("s3_path") or os.getenv("S3_PATH"),
            "aws_profile": kwargs.get("aws_profile") or os.getenv("AWS_PROFILE"),
        }

        # Enforce required settings: treat empty strings as missing.
        required = [
            "start_date",
            "end_date",
            "pypi_project",
            "table_name",
            "gcp_project",
            "timestamp_column",
            "destination",
        ]
        missing = [k for k in required if not merged.get(k)]
        if missing:
            raise ValueError(
                f"Missing required configuration values: {', '.join(missing)}. "
                "Set them in .env or pass them as CLI flags when running the pipeline."
            )

        # Normalize destination into a list when provided as a comma-separated string
        dest = merged.get("destination")
        if isinstance(dest, str):
            merged["destination"] = [s.strip() for s in dest.split(",") if s.strip()]

        return PypiJobParameters(**merged)

    fire.Fire(lambda **kwargs: main(_make_params(**kwargs)))
