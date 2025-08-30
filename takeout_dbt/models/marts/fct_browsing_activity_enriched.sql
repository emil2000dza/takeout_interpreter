{{ config(materialized="table") }}

SELECT DISTINCT
    raw_history.id,
    raw_history.title,
    raw_history.datetime,
    sessions.seconds_since_last_visit AS seconds_since_last_visit,
    sessions.seconds_until_next_visit AS seconds_until_next_visit,
    sessions.minutes_since_last_visit AS minutes_since_last_visit,
    sessions.seconds_since_last_visit_categories AS seconds_since_last_visit_categories,
    sessions.attention_span AS attention_span,
    sessions.session_id_20_min AS session_id,
    times.day_of_week AS day_of_week,
    times.hour_of_day AS hour_of_day,
    times.time_of_day AS time_of_day,
    raw_history.url,
    raw_history.domain,
    {% for i in range(1, 6) %}
        urls.path_level_{{ i }} AS path_level_{{ i }},
    {% endfor %}
    {% for i in range(1, 3) %}
        urls.domain_depth_{{ i }} AS domain_depth_{{ i }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source('chrome_history','raw_history') }}
INNER JOIN {{ ref('stg_sessions') }} AS sessions USING (id)
INNER JOIN {{ ref('stg_time_enrichment') }} AS times USING (id)
INNER JOIN {{ ref('stg_url_parts') }} AS urls USING (url)