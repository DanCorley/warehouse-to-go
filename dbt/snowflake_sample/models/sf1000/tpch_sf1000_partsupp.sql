select
  ps_partkey
  , ps_suppkey
  , ps_availqty
  , ps_supplycost
  , ps_comment
from {{ source('sf1000', 'partsupp') }}