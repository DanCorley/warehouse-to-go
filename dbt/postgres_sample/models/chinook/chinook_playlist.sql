select
    "PlaylistId"
    , "Name"
from {{ source('chinook', 'Playlist') }}
