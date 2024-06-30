select
    period_start,
    period_end
from {{ source('dl_reference', 'dl_ref_hours') }}
