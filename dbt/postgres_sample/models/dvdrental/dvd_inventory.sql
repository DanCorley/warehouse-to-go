select
    inventory_id
    , film_id
    , store_id
    , last_update
from {{ source('dvdrental', 'inventory') }}
