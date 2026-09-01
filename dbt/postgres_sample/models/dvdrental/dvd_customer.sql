select
    customer_id
    , store_id
    , first_name
    , last_name
    , email
    , address_id
    , activebool
    , create_date
    , last_update
    , active
from {{ source('dvdrental', 'customer') }}
