WITH raw_tracks AS (
    SELECT * FROM {{ source('spotify', 'tracks')}}
)

SELECT
    id AS track_id,
    name AS track_name,
    uri AS track_uri,
    popularity,
    duration,
    artists,
    included_in,
    reccobeats_id,
    acousticness,
    danceability,
    energy,
    instrumentalness,
    key,
    liveness,
    loudness,
    mode,
    speechiness,
    tempo,
    valence
FROM raw_tracks
