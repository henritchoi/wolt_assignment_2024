{{
  config(
    materialized = 'incremental',
    unique_key = 'delivery_area_per_hour_id',
    on_schema_change = 'sync_all_columns'
    )
}}
WITH 
    hourly_table AS (
        SELECT
            period_start,
            period_end
        FROM {{ref('dl_dbt_ref_hours')}}
        WHERE
            1 = 1

        {% if is_incremental() %}
        
        AND period_start >= 
            LEAST({{var("backfill_variable", "2100-01-01")}}
            , dateadd (day, -1, (SELECT max(period_start)::date FROM {{this}})) 
            )
        
        {% endif %}
    ),

    delivery_radius_reductions AS (
        SELECT
            delivery_area_id,
            default_delivery_radius,
            event_duration,
            round_to_closest_hour(event_started_timestamp) AS event_started,
            round_to_closest_hour(event_ended_timestamp) AS event_ended,
            is_radius_reduction
        FROM {{ref('dm_delivery_radius_reductions')}}
        WHERE
            1 = 1
            
        {% if is_incremental() %}
        
        AND event_started_timestamp >= 
            LEAST({{var("backfill_variable", "2100-01-01")}}
            , dateadd (day, -1, (SELECT max(period_start)::date FROM {{this}})) 
            )
        
        {% endif %}
    ),

    purchases AS (
        SELECT
            purchase_id,
            time_received,
            time_delivered,
            end_amount_with_vat_eur,
            dropoff_distance_straight_line_metres,
            delivery_area_id
        FROM {{ref('dw_purchases')}}
        WHERE
            1 = 1
            
        {% if is_incremental() %}
        
        
        AND time_received >= 
            LEAST({{var("backfill_variable", "2100-01-01")}}
            , dateadd (day, -1, (SELECT max(period_start)::date FROM {{this}})) 
            )
        
        {% endif %}
    ),

    main AS (
        SELECT 
            h.period_start,
            drr.delivery_area_id,
            drr.default_delivery_radius,
            SUM(nvl(end_amount_with_vat_eur, 0)) AS revenue,
            COUNT(purchase_id) AS purchase_count,
            SUM(CASE WHEN is_radius_reduction = 1 then event_duration else 0 end) AS reduction_duration,
            SUM(is_radius_reduction) AS reduction_count
        FROM hourly_table h
        LEFT JOIN delivery_radius_reductions drr
            ON drr.event_started <= h.period_start
            AND drr.event_ended > h.period_start
        LEFT JOIN purchases p
            ON p.delivery_area_id = drr.delivery_area_id
            AND p.time_received BETWEEN h.period_start AND h.period_end
        WHERE
            1 = 1
            
        {% if is_incremental() %}
        
        AND period_start >= 
            LEAST({{var("backfill_variable", "2100-01-01")}}
            , dateadd (day, -1, (SELECT max(period_start)::date FROM {{this}})) 
            )
        
        {% endif %}

        {{dbt_utils.group_by(n=3)}}
    )
SELECT 
    {{dbt_utils.generate_surrogate_key(['m1.period_start', 'm1.delivery_area_id'])}} as delivery_area_per_hour_id,
    m1.period_start,
    m1.delivery_area_id,
    m1.default_delivery_radius,
    m1.revenue,
    m1.purchase_count,
    m1.reduction_duration,
    m1.reduction_count,
    m1.revenue - m2.revenue AS revenue_wow_variation,
    m1.purchase_count - m2.purchase_count AS purchase_count_wow_variation,
    m1.reduction_duration - m2.reduction_duration AS reduction_duration_wow_variation,
    m1.reduction_count - m2.reduction_count AS reduction_count_wow_variation
FROM main m1
LEFT JOIN main m2
    ON m1.period_start = m2.period_start + interval '7 days'
    and m1.delivery_area_id = m2.delivery_area_id
WHERE 
    1 = 1
        
    {% if is_incremental() %}
    
    AND m1.period_start >= 
        LEAST({{var("backfill_variable", "2100-01-01")}}
        , dateadd (day, -1, (SELECT max(period_start)::date FROM {{this}})) 
          )
    
    {% endif %}
