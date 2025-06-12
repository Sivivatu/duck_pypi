from ingestion.models import PypiJobParameters
from ingestion.bigquery import build_pypi_query

def test_build_pypi_query():
    """
    Test the build_pypi_query function to ensure it constructs a valid SQL query.
    """
    params = PypiJobParameters(
        table_name="bigquery-public-data.pypi.file_downloads",
        gcp_project="duckdb-pypi",
        pypi_project="duckdb",
        s3_path=None,
        aws_profile=None,
        start_date="2023-01-01",
        end_date="2023-12-31",
        timestamp_column="timestamp"
    )

    query = build_pypi_query(params)
    expected_query = """
        SELECT
        *
    FROM
        `bigquery-public-data.pypi.file_downloads`
    WHERE
        project = 'duckdb'
        AND timestamp >= '2023-01-01'
        AND timestamp < '2023-12-31'

        """
    assert isinstance(query, str), "Query should be a string"
    # assert "SELECT" in query, "Query should contain a SELECT statement"
    # assert "FROM" in query, "Query should contain a FROM clause"
    # assert "WHERE" in query, "Query should contain a WHERE clause"
    assert query.strip() == expected_query.strip(), "Query does not match the expected format"
