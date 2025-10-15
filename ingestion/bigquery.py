import os
import time

from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import pyarrow as pa

from ingestion.models import PypiJobParameters

# PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"


def get_bigquery_client(
    params: PypiJobParameters, credentials_path: str = None
) -> bigquery.Client:
    """
    Create a BigQuery client using the provided project name and optional credentials path.

    Args:
        project_name (str): The Google Cloud project name.
        credentials_path (str): Path to the service account key file. If None, uses default credentials.

    Returns:
        bigquery.Client: A BigQuery client instance.
    """
    try:
        # Read raw env var and sanitize surrounding quotes if present.
        raw_env_path: str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        def _strip_surrounding_quotes(v: str) -> str | None:
            if not v:
                return None
            if (v.startswith('"') and v.endswith('"')) or (
                v.startswith("'") and v.endswith("'")
            ):
                return v[1:-1]
            return v

        # Prefer an explicit credentials_path argument when provided; only
        # fall back to the environment variable if the argument is None.
        if credentials_path:
            service_account_path = credentials_path
            logger.info(f"Using credentials_path argument: {service_account_path}")
        else:
            service_account_path = _strip_surrounding_quotes(raw_env_path)
            logger.info(f"Raw GOOGLE_APPLICATION_CREDENTIALS env: {raw_env_path}")
            logger.info(f"Using service account path from env: {service_account_path}")

        if service_account_path and os.path.exists(service_account_path):
            credentials: service_account.Credentials = (
                service_account.Credentials.from_service_account_file(
                    service_account_path
                )
            )
            return bigquery.Client(project=params.gcp_project, credentials=credentials)

        raise EnvironmentError(
            """
            Service account credentials not found. 
            Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable or provide a valid credentials path.
            """
        )

    except DefaultCredentialsError as creds_error:
        raise creds_error


def get_bigquery_result(
    query_str: str, bigquery_client: bigquery.Client
) -> pa.Table:
    """
    Execute a BigQuery SQL query and yield the results as a pyarrow table.
    Args:
        query_str (str): The SQL query to execute.
        bigquery_client (bigquery.Client): The BigQuery client instance.
    Returns:
        pa.Table: The results of the query as a pyarrow table.
    """
    try:
        # start measuring time for query execution
        start_time: float = time.time()
        logger.info(f"Executing query: {query_str}")
        # execute the query and convert the result to a DataFrame
        pa_table: pa.Table = bigquery_client.query(query_str).to_arrow()
        # measure elapsed time for query execution
        elapsed_time: float = time.time() - start_time

        logger.info(f"Query executed successfully in {elapsed_time:.2f} seconds.")
        # iterate over the DataFrame in chunks if needed
        return pa_table
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


def build_pypi_query(
    params: PypiJobParameters
) -> str:
    """
    Build the SQL query to retrieve package information from the PyPI database.
    Returns:
        str: The SQL query string.
    """
    pypi_public_dataset = f"bigquery-public-data.pypi.{params.table_name}"
    return f"""
    SELECT
        *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND timestamp >= TIMESTAMP('{params.start_date}')
        AND timestamp < TIMESTAMP('{params.end_date}')
    """
