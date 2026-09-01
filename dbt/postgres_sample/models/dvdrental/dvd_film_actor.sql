select
    actor_id
    , film_id
    , last_update
from {{ source('dvdrental', 'film_actor') }}
