select
    passengerid
    , survived
    , pclass
    , name
    , sex
    , age
    , sibsp
    , parch
    , ticket
    , fare
    , cabin
    , embarked
    , wikiid
    , name_wiki
    , age_wiki
    , hometown
    , boarded
    , destination
    , lifeboat
    , body
    , class
from {{ source('titanic', 'passenger') }}
