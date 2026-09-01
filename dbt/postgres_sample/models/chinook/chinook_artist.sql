select
    "ArtistId"
    , "Name"
from {{ source('chinook', 'Artist') }}
