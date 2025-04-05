select
  hd_demo_sk
  , hd_income_band_sk
  , hd_buy_potential
  , hd_dep_count
  , hd_vehicle_count
from {{ source('sf100tcl', 'household_demographics') }}