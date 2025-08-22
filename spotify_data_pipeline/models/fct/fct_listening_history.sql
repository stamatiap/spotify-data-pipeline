
{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}

WITH src_listening_history AS (
    SELECT * FROM {{ref("src_listening_history")}}
)

SELECT
    played_at,
    track_id,
    track_name,
    track_uri
FROM src_listening_history
WHERE (played_at is not null) AND (track_id is not null)
{% if is_incremental() %}
    AND played_at > (select max(played_at) from {{ this }})
{% endif %}
