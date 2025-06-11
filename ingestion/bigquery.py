import os
import time

from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import pandas as pd

from ingestion.models import PypiJobParameters

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"

def get_bigquery_client(
    project_name: str, credentials_path: str = None
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
        service_account_path: str = os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS", credentials_path
        )
        print(f"Using service account path: {service_account_path}")
        if service_account_path and os.path.exists(service_account_path):
            credentials: service_account.Credentials = (
                service_account.Credentials.from_service_account_file(
                    service_account_path
                )
            )
            return bigquery.Client(project=project_name, credentials=credentials)
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
) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and yield the results as a pandas DataFrame.
    Args:
        query_str (str): The SQL query to execute.
        bigquery_client (bigquery.Client): The BigQuery client instance.
    Returns:
        pd.DataFrame: The results of the query as a pandas DataFrame.
    """
    try:
        # start measuring time for query execution
        start_time: float = time.time()
        logger.info(f"Executing query: {query_str}")
        # execute the query and convert the result to a DataFrame
        dataframe: pd.DataFrame = bigquery_client.query(query_str).to_dataframe()
        # measure elapsed time for query execution
        elapsed_time: float = time.time() - start_time

        logger.info(f"Query executed successfully in {elapsed_time:.2f} seconds.")
        # iterate over the DataFrame in chunks if needed
        return dataframe
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


def build_pypi_query(
        params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    """
    Build the SQL query to retrieve package information from the PyPI database.
    Returns:
        str: The SQL query string.
    """
    return f"""
    SELECT
        *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND timestamp >= '{params.start_date}'
        AND timestamp < '{params.end_date}'
    """
