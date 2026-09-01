select
    city_id
    , city
    , country_id
    , last_update
from {{ source('dvdrental', 'city') }}
