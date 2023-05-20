WITH 

source AS (
    SELECT * FROM {{ source('greenery', 'users') }}

)

, renamed_recasted AS (

    SELECT  
        user_id as user_guid
        , first_name
        , last_name
        , email
        , phone_number
        , created_at as created_at_utc
        , updated_at as updated_at_utc
        , address as address_guid

    FROM source

)

SELECT * FROM renamed_recasted
