-- ============================================
-- MODEL: dim_commodity.sql
-- LAYER: Gold
-- PURPOSE: Commodity dimension table
-- ============================================

WITH commodities AS (
    SELECT DISTINCT
        commodity_name,
        commodity_category,
        unit
    FROM {{ ref('silver_food_prices') }}
    WHERE commodity_name IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['commodity_name', 'unit']) }}  AS commodity_key,
    commodity_name,
    commodity_category,
    unit,
    CURRENT_TIMESTAMP()                                                  AS created_at
FROM commodities