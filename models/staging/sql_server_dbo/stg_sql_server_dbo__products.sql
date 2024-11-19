{{ config(materialized="view") }}

WITH src_products AS (
    SELECT * 
    FROM {{ source("sql_server_dbo", "products") }}
), 
clean_products AS (
    SELECT
        product_id,
        price,
        lower(name),
        inventory,
        CASE 
            WHEN _fivetran_deleted IS NULL THEN false 
            ELSE true 
        END AS is_deleted,
        convert_timezone('UTC', _fivetran_synced) AS synced_fivetran
    FROM src_products
)

SELECT *
FROM src_products