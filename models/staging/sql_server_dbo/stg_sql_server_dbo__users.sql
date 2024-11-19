{{ config(materialized="view") }}

WITH src_users AS (
    SELECT * 
    FROM {{ source("sql_server_dbo", "users") }}
), 
clean_users AS (
    SELECT
        user_id,
        convert_timezone('UTC', updated_at) AS synced_updated_at,
        address_id,
        lower(last_name) AS last_name,
        convert_timezone('UTC', created_at) AS synced_created_at,
        first_name,
        md5(phone_number) AS phone_hash,
        CASE 
            WHEN total_orders IS NULL THEN 0 
            ELSE total_orders 
        END AS total_orders,
        lower(first_name) AS first_name_lower,
        email,
        CASE 
            WHEN _fivetran_deleted IS NULL THEN false 
            ELSE true 
        END AS is_deleted,
        convert_timezone('UTC', _fivetran_synced) AS synced_fivetran
    FROM src_users
)

SELECT *
FROM clean_users