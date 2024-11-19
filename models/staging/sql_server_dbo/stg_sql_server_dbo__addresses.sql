{{ config(materialized="view") }}

with
    src_addresses as (
        select
            address_id,
            try_cast(zipcode as int) as zipcode,
            lower(country),
            lower(state) as state,
            case when _fivetran_deleted is null then false else true end as is_deleted,
            convert_timezone('UTC', _fivetran_synced) as synced_utc
        from {{ source("sql_server_dbo", "addresses") }}
    )
select * from src_addresses