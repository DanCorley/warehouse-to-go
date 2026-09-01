select
    category_id
    , name
    , last_update
from {{ source('dvdrental', 'category') }}
