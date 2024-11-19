{{ config(materialized="view") }}

WITH src_orders AS (
    SELECT * 
    FROM {{ source("sql_server_dbo", "orders") }}
), 
clean_orders AS (
    SELECT
        order_id,
        case when shipping_service is null then 'in process' else shipping_service, 
        shipping_cost,
        inventory,
        CASE 
            WHEN _fivetran_deleted IS NULL THEN false 
            ELSE true 
        END AS is_deleted,
        convert_timezone('UTC', _fivetran_synced) AS synced_fivetran
    FROM src_orders
)

SELECT *
FROM clean_orders