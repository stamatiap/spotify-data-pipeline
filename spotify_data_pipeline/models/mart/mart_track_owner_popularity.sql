{{
  config(
    materialized = 'table',
    )
}}

WITH playlist_tracks AS (

    SELECT * 
    FROM {{ ref('map_playlist_tracks') }}

),

playlist_owners AS (

    SELECT * 
    FROM {{ ref('dim_playlist_cleansed') }}

),

SELECT
    pt.track_id,
    COUNT(DISTINCT p.owner_id) AS owner_count
FROM playlist_tracks AS pt
LEFT JOIN playlist_owners AS p
    ON p.playlist_id = pt.playlist_id
GROUP BY pt.track_id
