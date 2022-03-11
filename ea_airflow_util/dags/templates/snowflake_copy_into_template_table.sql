copy into {database}.{schema}.{table}
    (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, resource_name, v)
from (
    select
        split_part(metadata$filename, '/', 1) as tenant_code,
        split_part(metadata$filename, '/', 2) as api_year,
        to_date(split_part(metadata$filename, '/', 3), 'YYYYMMDD') as pull_date,
        to_timestamp(split_part(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') as pull_timestamp,
        metadata$file_row_number as file_row_number,
        metadata$filename as filename,
        '{resource}' as resource_name,
        t.$1 as v
    from @{database}.{schema}.{stage}/{s3_key} (file_format => 'json_default') t
)
force = true;