WITH raw_playlists AS (
    SELECT * FROM {{source('spotify', 'playlists')}}
)

SELECT 
    id AS playlist_id,
    name AS playlist_name,
    uri AS playlist_uri,
    owner_display_name AS owner_name,
    owner_id,
    owner_uri,
    tracks_total AS total_tracks
FROM raw_playlists