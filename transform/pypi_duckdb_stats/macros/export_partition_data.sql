{% macro export_partition_data(date_column, table) %}
{% set s3_path = env_var('TRANSFORM_S3_PATH_OUTPUT', 'my-bucket-path') %}
  COPY(
        SELECT *,
            year{{ date_column }} as year,
            month{{ date_column }} as month,
        COUNT(*) AS row_count
    FROM {{ table }}
    )
    TO '{{ s3_path }}/{{ table }}'
        (FORMAT PARQUET, PARTITION BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION ZSTD);
{% endmacro %}