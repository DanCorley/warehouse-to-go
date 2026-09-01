select
    "AlbumId"
    , "Title"
    , "ArtistId"
from {{ source('chinook', 'Album') }}
