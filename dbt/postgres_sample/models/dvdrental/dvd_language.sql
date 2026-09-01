select
    language_id
    , name
    , last_update
from {{ source('dvdrental', 'language') }}
