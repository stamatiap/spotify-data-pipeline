WITH playlist_tracks AS (
    SELECT *
    FROM {{ref("map_playlist_tracks")}}
),

tracks AS (
    SELECT *
    FROM {{ref("dim_tracks_cleansed")}}
)

SELECT pt.playlist_id, 
    sum(duration) as total_duration_sec,
    round(AVG(danceability), 2) AS avg_danceability, 
    round(AVG(energy), 2) AS avg_energy,
    round(AVG(tempo)) AS avg_tempo,
    round(AVG(speechiness), 2) as avg_speechiness,
    round(AVG(valence), 2) AS avg_valence,
    count(distinct(pt.track_id)) as unique_tracks,
    count(pt.track_id) as total_tracks
FROM playlist_tracks AS pt
INNER JOIN tracks AS ts
    ON pt.track_id = ts.track_id
GROUP BY playlist_id