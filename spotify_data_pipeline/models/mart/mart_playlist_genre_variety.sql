WITH playlist_tracks AS (
    SELECT *
    FROM {{ref("map_playlist_tracks")}}
),

track_artists AS (
    SELECT *
    FROM {{ref("map_track_artists")}}
),

artist_genre AS (
    SELECT *
    FROM {{ref("artist_genre")}}
)

SELECT playlist_id, 
    count(distinct(genre_1)) AS unique_main_genres
FROM playlist_tracks AS pt
LEFT JOIN track_artists AS ta
    ON pt.track_id = ta.track_id
LEFT JOIN  artist_genre AS ag
    ON ta.artist_id = ag.artist_id
WHERE genre_1 is not null
GROUP BY playlist_id