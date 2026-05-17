-- ============================================
-- MODEL: fact_food_prices.sql
-- LAYER: Gold
-- PURPOSE: Central fact table joining food
--          prices with dimensions, CPI context
--          and exchange rate enrichment
-- ============================================

WITH food_prices AS (
    SELECT * FROM {{ ref('silver_food_prices') }}
),

cpi AS (
    SELECT * FROM {{ ref('silver_psa_cpi') }}
),

fx AS (
    SELECT * FROM {{ ref('silver_bsp_exchange_rate') }}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

dim_commodity AS (
    SELECT * FROM {{ ref('dim_commodity') }}
),

dim_market AS (
    SELECT * FROM {{ ref('dim_market') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

-- Get monthly exchange rate matched to price date
monthly_fx AS (
    SELECT
        rate_year,
        rate_month,
        usd_php_rate
    FROM fx
),

-- Join CPI to food prices on region + year + month
-- matching food commodity category to CPI food category
cpi_food AS (
    SELECT
        region_name,
        cpi_year,
        cpi_month_num,
        AVG(cpi_value) AS avg_food_cpi
    FROM cpi
    WHERE UPPER(commodity_description) LIKE '%FOOD%'
        AND cpi_month_num IS NOT NULL
    GROUP BY region_name, cpi_year, cpi_month_num
),

enriched AS (
    SELECT
        -- Keys
        dr.region_key,
        dc.commodity_key,
        dm.market_key,
        dd.date_key,

        -- Date attributes
        fp.price_date,
        fp.price_year,
        fp.price_month,

        -- Location
        fp.region_name,
        fp.island_group,
        fp.market_name,
        fp.province_name,

        -- Commodity
        fp.commodity_name,
        fp.commodity_category,
        fp.unit,
        fp.price_type,

        -- Prices
        fp.price_php,
        fp.price_usd,

        -- Exchange rate enrichment
        COALESCE(
            fp.price_php / NULLIF(lx.usd_php_rate, 0),
            fp.price_usd
        )                                           AS price_usd_current_rate,
        lx.usd_php_rate                             AS current_usd_php_rate,

        -- CPI enrichment
        cf.avg_food_cpi                             AS regional_food_cpi,

        -- Price affordability index
        -- Higher = less affordable relative to CPI baseline
        CASE
            WHEN cf.avg_food_cpi IS NOT NULL AND cf.avg_food_cpi > 0
            THEN ROUND(fp.price_php / cf.avg_food_cpi * 100, 4)
            ELSE NULL
        END                                         AS price_to_cpi_ratio,

        CURRENT_TIMESTAMP()                         AS created_at

    FROM food_prices fp
    LEFT JOIN dim_region dr
        ON fp.region_name = dr.region_name
    LEFT JOIN dim_commodity dc
        ON fp.commodity_name = dc.commodity_name
        AND fp.unit = dc.unit
    LEFT JOIN dim_market dm
        ON fp.market_name = dm.market_name
        AND fp.region_name = dm.region_name
    LEFT JOIN dim_date dd
        ON fp.price_year = dd.year
        AND fp.price_month = dd.month
    LEFT JOIN cpi_food cf
        ON fp.region_name = cf.region_name
        AND fp.price_year = cf.cpi_year
        AND fp.price_month = cf.cpi_month_num
    LEFT JOIN monthly_fx lx
    ON fp.price_year = lx.rate_year
    AND fp.price_month = lx.rate_month
)

SELECT * FROM enriched