{{
  config(
    materialized = 'incremental',
    unique_key = 'purchase_id',
    on_schema_change = 'sync_all_columns'
    )
}}
SELECT
    purchase_id,
    time_received,
    time_delivered,
    end_amount_with_vat_eur,
    dropoff_distance_straight_line_metres,
    delivery_area_id
FROM {{ref('dl_dbt_oms_purchases')}}
WHERE
    1 =1 

    {% if is_incremental() %}
    
    AND time_received >= 
        LEAST({{var("backfill_variable", "2100-01-01")}}
        , dateadd (day, -1, (SELECT max(time_received)::date FROM {{this}})) 
        )
    
    {% endif %}
