{{
  config(
    materialized = 'table',
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('src_tracks') }}

),

artist_array AS (
    SELECT
        track_id,
        f1.value AS artist_obj
    FROM source,
         LATERAL FLATTEN(input => PARSE_JSON(artists)) f1  -- flatten the list of dicts
),

artist_pairs AS (
    SELECT
        track_id,
        f2.key::string   AS artist_id,
        f2.value::string AS artist_name
    FROM artist_array,
         LATERAL FLATTEN(input => artist_array.artist_obj) f2  -- flatten each dict inside the list
)

SELECT DISTINCT
    track_id,
    artist_id,
    artist_name
FROM artist_pairs