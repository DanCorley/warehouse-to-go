select
  n_nationkey
  , n_name
  , n_regionkey
  , n_comment
from {{ source('sf100', 'nation') }}