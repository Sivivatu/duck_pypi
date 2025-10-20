
with pre_agg_data as (SELECT
    timestamp::date as date,
    details.system.name as system_name,
    details.system.release as system_release,
    file.version as version,
    project,
    country_code,
    details.cpu as cpu,
    case
        when details.python is null then null
        else concat(
            split_part(details.python, '.', 1),
            '.',
            split_part(details.python, '.', 2)
        ) 
        end as python
from {{ dbt_unit_testing.source('my_db', 'file_downloads') }}
)

select 
    MD5(concat(
        '|',
        date,
        system_name,
        system_release,
        version,
        project,
        country_code,
        cpu,
        python
    )) as id,
    date as downloaded_at,
    system_name,
    system_release,
    version as package_version,
    project,
    country_code,
    cpu,
    python as normalized_python_version,
    count(*) as daily_download_sum
from pre_agg_data
group by
    ALL