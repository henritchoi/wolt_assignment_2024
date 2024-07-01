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
            LEAD(event_started_timestamp,1) OVER (PARTITION BY delivery_area_id order by event_started_timestamp) as event_ended_timestamp
        FROM {{ref('dw_delivery_radius_logs')}}
    ),

    default_radiuses as (
        SELECT
            delivery_area_id,
            default_delivery_radius,
            event_started,
            event_ended
        FROM {{ref('dw_default_delivery_radiuses')}}
    ),

    main as (
    SELECT 
        drl.delivery_area_id,
        datediff(
            hour,
            drl.event_started_timestamp,
            drl.event_ended_timestamp
        ) as event_duration,
        CASE
            WHEN drl.delivery_radius_meters < dr.default_delivery_radius THEN 'TRUE'
            WHEN drl.delivery_radius_meters >= dr.default_delivery_radius THEN 'FALSE'
        END AS is_radius_reduction
    FROM delivery_radius_logs drl
    LEFT JOIN default_radiuses dr
        ON drl.event_started_timestamp >= dr.event_started
        AND drl.event_ended_timestamp <= dr.event_ended
        AND drl.delivery_area_id = dr.delivery_area_id
    )
SELECT
    delivery_area_id,
    sum(event_duration) as radius_reduction_duration
FROM main
WHERE is_radius_reduction='TRUE'
GROUP BY 
    delivery_area_id
