select
  r_reason_sk
  , r_reason_id
  , r_reason_desc
from {{ source('sf100tcl', 'reason') }}