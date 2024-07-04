{{
  config(
    materialized = 'incremental',
    unique_key = 'delivery_radius_event_id',
    on_schema_change = 'sync_all_columns'
    )
}}
SELECT
    {{dbt_utils.generate_surrogate_key(['delivery_area_id','event_started_timestamp'])}} as delivery_radius_event_id,
    delivery_area_id,
    delivery_radius_meters,
    event_started_timestamp
FROM {{ref('dl_dbt_oms_delivery_radius_logs')}}
WHERE
    1 =1 

    {% if is_incremental() %}
    
      AND event_started_timestamp >= 
            LEAST({{var("backfill_variable", "2100-01-01")}}
            , dateadd (day, -1, (SELECT max(event_started_timestamp)::date FROM {{this}})) 
            )
    
    {% endif %}
