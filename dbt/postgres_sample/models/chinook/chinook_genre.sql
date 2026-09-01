select
    "GenreId"
    , "Name"
from {{ source('chinook', 'Genre') }}
