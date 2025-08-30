{{
  config(
    materialized = 'table',
    )
}}

WITH src_playlists AS (
    SELECT * FROM {{ref("src_playlists")}}
)

SELECT 
    DISTINCT owner_id AS user_id,
    owner_name as user_name
FROM src_playlists