{{
  config(
    materialized = 'table',
    )
}}

WITH src_tracks AS (
    SELECT * FROM {{ref("src_tracks")}}
)

SELECT 
    SPLIT_PART(f.value::string, ':', -1) AS playlist_id,
    track_id
FROM src_tracks s,
     LATERAL FLATTEN(input => PARSE_JSON(s.included_in)) f  -- flatten the list