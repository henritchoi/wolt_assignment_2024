{{
  config(
    materialized = 'table'
    )
}}

WITH 
    delivery_radius_logs AS (
        SELECT 
            delivery_area_id,
            delivery_radius_meters,
            event_started_timestamp,
            LEAD(event_started_timestamp,1) OVER (PARTITION BY delivery_area_id ORDER BY event_started_timestamp) AS event_ended_timestamp
        FROM {{ref('dw_delivery_radius_logs')}}
    ),

    default_delivery_radiuses AS (
        SELECT
            delivery_area_id,
            default_delivery_radius,
            event_started,
            event_ended
        FROM {{ref('dw_default_delivery_radiuses')}}
    ),

    main AS (
    SELECT 
        drl.delivery_area_id,
        dr.default_delivery_radius,
        drl.event_started_timestamp,
        drl.event_ended_timestamp,
        round(
            datediff(
                min,
                drl.event_started_timestamp,
                drl.event_ended_timestamp
            )
            /24) AS event_duration,
        CASE
            WHEN drl.delivery_radius_meters < dr.default_delivery_radius THEN 1
            WHEN drl.delivery_radius_meters >= dr.default_delivery_radius THEN 0
        END AS is_radius_reduction,
        row_number() over (PARTITION BY drl.delivery_area_id order by event_started desc nulls last) as rn
    FROM delivery_radius_logs drl
    LEFT JOIN default_delivery_radiuses dr
        ON drl.event_started_timestamp >= dr.event_started
        AND drl.event_ended_timestamp <= dr.event_ended
        AND drl.delivery_area_id = dr.delivery_area_id
    )
SELECT
    delivery_area_id,
    default_delivery_radius,
    event_started_timestamp,
    event_ended_timestamp,
    event_duration,
    is_radius_reduction,
    'FALSE' as is_frontfill
FROM main

UNION ALL

--frontfill

SELECT
    delivery_area_id,
    default_delivery_radius,
    event_ended_timestamp as event_started_timestamp,
    DATEADD(
        month
        ,1
        ,event_started_timestamp) 
    as event_ended,
    --this is a blank event
    0 AS event_duration,
    0 is_radius_reduction,
    'TRUE' as is_frontfill
FROM main
where rn=1
