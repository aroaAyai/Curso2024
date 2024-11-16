with
    src_addresses as (
        select
            address_id,
            lower(country) as country,
            try_cast(zipcode as int) as zipcode,
            lower(state) as state,
            case when _fivetran_deleted is null then false else true end as is_deleted,
            _fivetran_synced
        from {{ source("sql_server_dbo", "addresses") }}
    )

select *
from src_addresses
