{{
  config(
    materialized = 'table',
    )
}}

WITH src_tracks AS (
    SELECT * FROM {{ref("src_tracks")}}
)

SELECT DISTINCT
  f2.key::string   AS artist_id,
  f2.value::string AS artist_name
FROM src_tracks s,
     LATERAL FLATTEN(input => PARSE_JSON(s.artists)) f1,  -- flatten the list
     LATERAL FLATTEN(input => f1.value) f2