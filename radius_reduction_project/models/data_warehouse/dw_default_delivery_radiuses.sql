{{
  config(
    materialized = 'table',
    )
}}
with bASe AS (
    SELECT
        delivery_area_id,
        delivery_radius_meters,
        event_started_timestamp,
        --to get all the cutoff points of radius changes
        CASE
            WHEN
                delivery_radius_meters
                != LAG(delivery_radius_meters, 1)
                    OVER (
                        PARTITION BY delivery_area_id
                        ORDER BY event_started_timestamp ASC
                    )
                THEN 1
            ELSE 0
        END AS radius_change_binary
    FROM {{ref('dl_dbt_oms_delivery_radius_logs')}} 
    ORDER BY 
        1,
        3
),

interval_ordinals AS (
    SELECT
        delivery_area_id,
        delivery_radius_meters,
        event_started_timestamp,
        --to label the intervals OVER which delivery radiuses are consistent
        SUM(radius_change_binary)
            OVER (
                PARTITION BY delivery_area_id
                ORDER BY
                    event_started_timestamp ASC
                rows between unbounded preceding and current row
            )
        AS interval_ordinal
    FROM bASe
),

interval_starts AS (
    SELECT distinct
        delivery_area_id,
        delivery_radius_meters,
        interval_ordinal,
        --to get the start of the intervals, FROM which we will get interval duration
        MIN(event_started_timestamp)
            OVER (
                PARTITION BY delivery_area_id, interval_ordinal
                ORDER BY
                    interval_ordinal ASC
                rows between unbounded preceding and current row
            )
        AS interval_start
    FROM interval_ordinals
),

interval_durations AS (
    SELECT
        delivery_area_id,
        delivery_radius_meters,
        interval_ordinal,
        interval_start,
        --this for displaying purposes in the final result set
        LEAD(interval_start, 1)
            OVER (PARTITION BY delivery_area_id ORDER BY interval_ordinal ASC)
        AS interval_END,
        --to get interval duration 
        DATEDIFF(
            hour,
            interval_start,
            LEAD(interval_start, 1)
                OVER (
                    PARTITION BY delivery_area_id ORDER BY interval_ordinal ASC
                )
        ) AS interval_duration
    FROM interval_starts
),

default_radius_changes AS (
    SELECT
        delivery_area_id,
        interval_start,
        interval_END,
        --This displays the changes of default radius only
        CASE
            WHEN interval_duration > 24 THEN delivery_radius_meters
        END AS new_default_radius
    FROM interval_durations
),

main AS (
    SELECT
        delivery_area_id,
        interval_start AS event_started,
        interval_END AS event_ENDed,
        --Filling in all the time intervals OVER which there is no default radius change
        COALESCE(
            new_default_radius,
            LAST_VALUE(new_default_radius ignore nulls)
                OVER (
                    PARTITION BY delivery_area_id
                    ORDER BY
                        interval_start
                    rows between unbounded preceding and current row
                )
        ) AS default_delivery_radius
    FROM default_radius_changes
)

SELECT 
    delivery_area_id
    , default_delivery_radius
    , event_started
    , event_ended
FROM main
