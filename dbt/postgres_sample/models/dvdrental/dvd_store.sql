select
    store_id
    , manager_staff_id
    , address_id
    , last_update
from {{ source('dvdrental', 'store') }}
