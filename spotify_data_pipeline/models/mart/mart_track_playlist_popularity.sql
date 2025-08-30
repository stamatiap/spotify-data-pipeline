{{
  config(
    materialized = 'table',
    )
}}

WITH playlist_tracks AS (
    SELECT * FROM {{ref("map_playlist_tracks")}}
)

SELECT track_id, 
    COUNT( DISTINCT playlist_id) AS playlist_count
FROM playlist_tracks
GROUP BY track_id