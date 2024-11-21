{{ config(materialized="view") }}

with
    src_budget as (select * from {{ source("google_sheets", "budget") }}),

    cleaned_data_budget as (
        select
            _row,
            quantity,
            month(month) as month,
            year(month) as year,
            product_id,
            convert_timezone('UTC', _fivetran_synced) as synced_utc  -- Convertir a UTC
        from src_budget
    )

select *
from cleaned_data_budget