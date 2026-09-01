select
    "TrackId"
    , "Name"
    , "AlbumId"
    , "MediaTypeId"
    , "GenreId"
    , "Composer"
    , "Milliseconds"
    , "Bytes"
    , "UnitPrice"
from {{ source('chinook', 'Track') }}
