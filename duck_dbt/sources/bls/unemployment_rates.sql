WITH source AS (
    SELECT * 
    FROM {{ source('bls', 'unemployment_rates')}}
)

SELECT * 
FROM source;