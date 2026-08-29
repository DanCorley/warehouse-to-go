select
  r_regionkey
  , r_name
  , r_comment
from {{ source('sf1', 'region') }}