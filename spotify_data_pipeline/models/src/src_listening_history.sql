WITH raw_listening_history AS (
    SELECT * FROM {{ source('spotify', 'listening_history')}}
)

SELECT
    played_at,
    track_id,
    track_name,
    track_uri
From raw_listening_history