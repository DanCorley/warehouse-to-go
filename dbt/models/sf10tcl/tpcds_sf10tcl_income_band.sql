select
  ib_income_band_sk
  , ib_lower_bound
  , ib_upper_bound
from {{ source('sf10tcl', 'income_band') }}