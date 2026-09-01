select
    film_id
    , category_id
    , last_update
from {{ source('dvdrental', 'film_category') }}
