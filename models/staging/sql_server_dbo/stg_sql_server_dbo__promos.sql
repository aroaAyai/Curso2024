{{ config(materialized="view") }}

with
    src_promos as (select * from {{ source("sql_server_dbo", "promos") }}),
    new_row as (
        select
            md5('sin-promo') as promo_id,  -- Nueva ID
            lower('Sin Promoción') as desc_promo,  -- Convertir a minúsculas
            0 as discount,
            'active' as status,
            false as is_deleted,
            convert_timezone('UTC', current_timestamp()) as synced_at_utc  -- Fecha actual en UTC
    ),
    cleaned_data_promo as (
        select
            md5(lower(replace(promo_id, ' ', '_'))) as promo_id,  -- Hasheado de promo_id
            lower(promo_id) as desc_promo,  -- Convertir todo a minúsculas
            cast(discount as integer) as discount,
            lower(status) as status,
            case when _fivetran_deleted is null then false else true end as is_deleted,
            convert_timezone('UTC', _fivetran_synced) as synced_utc  -- Convertir a UTC
        from {{ source("sql_server_dbo", "promos") }}
    )

select *
from cleaned_data_promo
union all
select *
from new_row
