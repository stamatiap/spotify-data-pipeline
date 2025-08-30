WITH playlist_tracks AS (
    SELECT *
    FROM {{ref("map_playlist_tracks")}}
),

playlist_owners AS (
    SELECT *
    FROM {{ref("dim_playlist_cleansed")}}
),

track_artists AS (
    SELECT track_id, artist_id
    FROM {{ref("map_track_artists")}}
)

SELECT
    ta.artist_id,
    COUNT(DISTINCT po.owner_id) AS owner_count
FROM track_artists AS ta
LEFT JOIN playlist_tracks AS pt
    ON pt.track_id = ta.track_id
LEFT JOIN playlist_owners AS po
    ON po.playlist_id = pt.playlist_id
GROUP BY ta.artist_id

