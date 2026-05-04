-- ============================================
-- MODEL: silver_food_prices.sql
-- LAYER: Silver
-- SOURCE: bronze_wfp_food_prices
-- PURPOSE: Clean and standardize WFP food
--          price data for analytical use
-- ============================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_wfp_food_prices') }}
),

cleaned AS (
    SELECT
        -- Date
        TRY_CAST(DATE AS DATE)                          AS price_date,
        YEAR(TRY_CAST(DATE AS DATE))                    AS price_year,
        MONTH(TRY_CAST(DATE AS DATE))                   AS price_month,

        -- Region standardization
        CASE
            WHEN UPPER(TRIM("ADM1+NAME")) IN ('NATIONAL CAPITAL REGION', 'NATIONAL CAPITAL REGION (NCR)')
                THEN 'National Capital Region'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'CORDILLERA ADMINISTRATIVE REGION'
                THEN 'Cordillera Administrative Region'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'AUTONOMOUS REGION IN MUSLIM MINDANAO'
                THEN 'Bangsamoro Autonomous Region'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION I'   THEN 'Region I - Ilocos'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION II'  THEN 'Region II - Cagayan Valley'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION III' THEN 'Region III - Central Luzon'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION IV-A' THEN 'Region IV-A - CALABARZON'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION IV-B' THEN 'Region IV-B - MIMAROPA'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION V'   THEN 'Region V - Bicol'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION VI'  THEN 'Region VI - Western Visayas'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION VII' THEN 'Region VII - Central Visayas'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION VIII' THEN 'Region VIII - Eastern Visayas'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION IX'  THEN 'Region IX - Zamboanga Peninsula'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION X'   THEN 'Region X - Northern Mindanao'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION XI'  THEN 'Region XI - Davao'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION XII' THEN 'Region XII - SOCCSKSARGEN'
            WHEN UPPER(TRIM("ADM1+NAME")) = 'REGION XIII' THEN 'Region XIII - Caraga'
            ELSE TRIM("ADM1+NAME")
        END                                             AS region_name,

        -- Island group derived column
        CASE
            WHEN UPPER(TRIM("ADM1+NAME")) IN (
                'NATIONAL CAPITAL REGION', 'CORDILLERA ADMINISTRATIVE REGION',
                'REGION I', 'REGION II', 'REGION III',
                'REGION IV-A', 'REGION IV-B', 'REGION V'
            ) THEN 'Luzon'
            WHEN UPPER(TRIM("ADM1+NAME")) IN (
                'REGION VI', 'REGION VII', 'REGION VIII'
            ) THEN 'Visayas'
            ELSE 'Mindanao'
        END                                             AS island_group,

        -- Location
        TRIM("ADM2+NAME")                               AS province_name,
        TRIM("LOC+MARKET+NAME")                         AS market_name,
        TRY_CAST("GEO+LAT" AS FLOAT)                   AS latitude,
        TRY_CAST("GEO+LON" AS FLOAT)                   AS longitude,

        -- Commodity
        INITCAP(TRIM("ITEM+TYPE"))                      AS commodity_category,
        TRIM("ITEM+NAME")                               AS commodity_name,
        UPPER(TRIM("ITEM+UNIT"))                        AS unit,

        -- Price type
        INITCAP(TRIM("ITEM+PRICE+TYPE"))                AS price_type,

        -- Values
        TRY_CAST("VALUE" AS FLOAT)                      AS price_php,
        TRY_CAST("VALUE+USD" AS FLOAT)                  AS price_usd,

        CURRENT_TIMESTAMP()                             AS created_at

    FROM source
    WHERE
        "DATE" IS NOT NULL
        AND "VALUE" IS NOT NULL
        AND TRY_CAST("VALUE" AS FLOAT) > 0
)

SELECT * FROM cleaned
