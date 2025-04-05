select
  cd_demo_sk
  , cd_gender
  , cd_marital_status
  , cd_education_status
  , cd_purchase_estimate
  , cd_credit_rating
  , cd_dep_count
  , cd_dep_employed_count
  , cd_dep_college_count
from {{ source('sf10tcl', 'customer_demographics') }}