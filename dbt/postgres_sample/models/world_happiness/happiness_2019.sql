select
    overall_rank
    , country_or_region
    , score
    , gdp_per_capita
    , social_support
    , healthy_life_expectancy
    , freedom_to_make_life_choices
    , generosity
    , perceptions_of_corruption
from {{ source('world_happiness', '2019') }}
