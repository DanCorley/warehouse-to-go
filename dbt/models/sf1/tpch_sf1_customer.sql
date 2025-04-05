select
  c_custkey
  , c_name
  , c_address
  , c_nationkey
  , c_phone
  , c_acctbal
  , c_mktsegment
  , c_comment
from {{ source('sf1', 'customer') }}