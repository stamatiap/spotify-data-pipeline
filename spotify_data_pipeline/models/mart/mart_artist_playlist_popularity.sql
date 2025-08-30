WITH playlist_tracks AS (
    SELECT *
    FROM {{ ref('map_playlist_tracks') }}
),

track_artists AS (
    SELECT track_id, artist_id
    FROM {{ ref('map_track_artists') }}
)

SELECT
    pt.playlist_id,
    COUNT(DISTINCT ta.artist_id) AS artist_count
FROM track_artists AS ta
LEFT JOIN playlist_tracks AS pt
    ON pt.track_id = ta.track_id
GROUP BY pt.playlist_id