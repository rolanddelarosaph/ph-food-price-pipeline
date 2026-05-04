-- ============================================
-- MODEL: dim_market.sql
-- LAYER: Gold
-- PURPOSE: Market dimension table
-- ============================================

WITH markets AS (
    SELECT DISTINCT
        market_name,
        region_name,
        province_name,
        latitude,
        longitude
    FROM {{ ref('silver_food_prices') }}
    WHERE market_name IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['market_name', 'region_name']) }}  AS market_key,
    market_name,
    region_name,
    province_name,
    latitude,
    longitude,
    CURRENT_TIMESTAMP()                                                      AS created_at
FROM markets