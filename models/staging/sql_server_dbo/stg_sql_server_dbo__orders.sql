{{ config(materialized="view") }}

WITH src_orders AS (
    SELECT * 
    FROM {{ source("sql_server_dbo", "orders") }}
), 
clean_orders AS (
    SELECT
        order_id,
        CASE 
            WHEN shipping_service IS NULL or TRIM(promo_id)= '' tHEN 'in process' 
            ELSE shipping_service 
        END AS shipping_service, 
        shipping_cost,
        address_id,
        created_at,
        CASE 
            WHEN promo_id IS NULL OR TRIM(promo_id) = '' THEN 'sin promocion' 
            ELSE promo_id 
        END AS promo_desc,
        CONVERT_TIMEZONE('UTC', estimated_delivery_at) AS estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        CONVERT_TIMEZONE('UTC', delivered_at) AS delivered_at,
        tracking_id,
        status,
        CASE 
            WHEN _fivetran_deleted IS NULL THEN FALSE 
            ELSE TRUE 
        END AS is_deleted,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS synced_fivetran
    FROM src_orders
)

SELECT *
FROM clean_orders
