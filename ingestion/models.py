from pydantic import BaseModel, Field
from typing import Optional, List, Union, Annotated


class PypiJobParameters(BaseModel):
    start_date: str = "2023-01-01"
    end_date: str = "2023-12-31"
    pypi_project: str = "duckdb"
    table_name: str
    gcp_project: str
    timestamp_column: str = "timestamp"
    destination: Annotated[
        Union[List[str], str], Field(default=["local"])
    ]  # List of destinations to store the results, e.g., ["local", "gcs", "s3", "md"]
    s3_path: Optional[str]
    aws_profile: Optional[str]
