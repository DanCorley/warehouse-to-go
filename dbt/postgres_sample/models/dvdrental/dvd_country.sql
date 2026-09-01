select
    country_id
    , country
    , last_update
from {{ source('dvdrental', 'country') }}
