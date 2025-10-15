from typing import List
from loguru import logger

def create_table_from_dataframe(duckdb_conn, table_name: str, dataframe: str) -> None:
    """
    Create a DuckDB table from a Pandas DataFrame.

    Args:
        duckdb_conn: An active DuckDB connection.
        table_name: The name of the DuckDB table to be created.
        dataframe: A Pandas DataFrame to be converted into a DuckDB table.
    """
    duckdb_conn.sql(
        f"""
        CREATE TABLE {table_name} AS 
            SELECT
                * 
            FROM {dataframe}
        """
    )

def load_aws_credentials(duckdb_conn, aws_profile: str = None) -> None:
    """
    Load AWS credentials from the specified profile into environment variables.

    Args:
        aws_profile: The name of the AWS profile to load credentials from. If None, uses the default profile.
    """
    duckdb_conn.sql(f"CALL load_aws_credentials('{aws_profile}');")

def write_to_s3_from_duckdb(
    duckdb_conn, s3_path: str, table_name: str, timestamp_column: str
) -> None:
    """
    Write a DuckDB table to an S3 path in the specified file format.

    Args:
        duckdb_conn: An active DuckDB connection.
        s3_path: The S3 path where the file will be written.
        table_name: The name of the DuckDB table to be written to S3.
        timestamp_column: The name of the timestamp column used for partitioning.
        """
    logger.info(f"Writing table {table_name} to {s3_path}/{table_name}.")
    duckdb_conn.sql(
        f"""
        COPY (
            SELECT 
                *,
                YEAR({timestamp_column}) AS year,,
                MONTH({timestamp_column}) AS month,
            FROM {table_name}) 
        TO '{s3_path}/{table_name}' 
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
        """
    )

def connect_to_md(duckdb_conn, motherduck_token: str) -> None:
    """
    Connect to MotherDuck using the provided token.

    Args:
        duckdb_conn: An active DuckDB connection.
        motherduck_token: The token used for authentication with MotherDuck.
    """
    duckdb_conn.sql("INSTALL md;")
    duckdb_conn.sql("LOAD md;")
    duckdb_conn.sql(f"SET motherduck_token='{motherduck_token}';")
    duckdb_conn.sql("ATTACH 'md:';")

def write_to_md_from_duckdb(
    duckdb_conn, table_name: str,
    local_database: str, 
    remote_database: str, 
    timestamp_column: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Write a DuckDB table to MotherDuck.

    Args:
        duckdb_conn: An active DuckDB connection.
        table_name: The name of the DuckDB table to be written to MotherDuck.
        local_database: The name of the local DuckDB database.
        remote_database: The name of the remote MotherDuck database.
        timestamp_column: The name of the timestamp column used for partitioning.
        start_date: The start date for filtering data.
        end_date: The end date for filtering data.
    """
    logger.info(f"Writing table {table_name} to MotherDuck at {remote_database}.")
    duckdb_conn.sql(f"CREATE DATABASE IF NOT EXISTS {remote_database};")
    duckdb_conn.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {remote_database}.{table_name} AS 
            SELECT * 
            FROM {local_database}.{table_name}
        """
        )
    duckdb_conn.sql(
        f"DELETE FROM {remote_database}.{table_name} WHERE {timestamp_column} BETWEEN '{start_date}' AND '{end_date}'"
    )
    duckdb_conn.sql(
        f"""
        INSERT INTO {remote_database}.{table_name}
        SELECT * 
        FROM {local_database}.{table_name}
        """
    )