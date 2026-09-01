select
    show_id
    , type
    , title
    , director
    , cast_members
    , country
    , date_added
    , release_year
    , rating
    , duration
    , listed_in
    , description
from {{ source('netflix', 'netflix_shows') }}
