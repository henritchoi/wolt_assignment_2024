select
    delivery_area_id,
    delivery_radius_meters,
    event_started_timestamp
from {{ source('dl_oms', 'dl_oms_delivery_radius_log') }}