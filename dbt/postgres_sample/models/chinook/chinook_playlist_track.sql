select
    "PlaylistId"
    , "TrackId"
from {{ source('chinook', 'PlaylistTrack') }}
