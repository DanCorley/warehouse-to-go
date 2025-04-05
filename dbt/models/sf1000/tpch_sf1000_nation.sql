select
  n_nationkey
  , n_name
  , n_regionkey
  , n_comment
from {{ source('sf1000', 'nation') }}