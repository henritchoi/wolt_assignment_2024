select
    purchase_id,
    time_received,
    time_delivered,
    end_amount_with_vat_eur,
    dropoff_distance_straight_line_metres,
    delivery_area_id
from {{ source('dl_oms', 'dl_oms_purchases') }}
