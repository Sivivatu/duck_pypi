import os
from google.cloud.bigquery.client import Client
from pandas import DataFrame
from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)


def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        "/root/.config/gcloud/duck-pypi-db0ea102a482.json"
    )
    print("Ingestion pipeline started.")

    # Initialize BigQuery client
    bq_client: Client = get_bigquery_client(project_name="duck-pypi")

    # Build and execute the query
    query: str = build_pypi_query()
    df_results: DataFrame = get_bigquery_result(query, bq_client)

    print(f"Query executed successfully. Retrieved {len(df_results)} rows.")
    print(df_results.head())

    # # Process the results
    # for index, row in df_results.iterrows():
    #     print(f"Package: {row['name']}, Version: {row['version']}")

    print("Ingestion pipeline completed.")


if __name__ == "__main__":
    main()
