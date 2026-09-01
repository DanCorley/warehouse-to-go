select
    "MediaTypeId"
    , "Name"
from {{ source('chinook', 'MediaType') }}
