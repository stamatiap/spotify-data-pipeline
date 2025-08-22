{{
  config(
    materialized = 'view',
    )
}}

WITH src_tracks AS (
    SELECT * FROM {{ref("src_tracks")}}
)

SELECT
    track_id,
    track_name,
    track_uri,
    popularity,
    duration,
    artists,
    included_in,
    reccobeats_id,
    ROUND(acousticness, 2) AS acousticness,
    ROUND(danceability, 2) AS danceability,
    ROUND(energy, 2) AS energy,
    ROUND(instrumentalness, 2) AS instrumentalness,
    key,
    ROUND(liveness, 2) AS liveness,
    ROUND(loudness, 2) AS loudness,
    mode,
    ROUND(speechiness, 2) AS speechiness,
    ROUND(tempo, 2) AS tempo,
    ROUND(valence, 2) AS valence
FROM src_tracks
WHERE reccobeats_id IS NOT NULL