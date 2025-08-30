{{
  config(
    materialized = 'view',
    )
}}

WITH src_playlists AS (
    SELECT * FROM {{ref("src_playlists")}}
)

SELECT
    playlist_id,
    playlist_name,
    playlist_uri,
    owner_name,
    owner_id,
    owner_uri,
    total_tracks
FROM src_playlists