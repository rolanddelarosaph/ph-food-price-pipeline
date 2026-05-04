-- ============================================
-- MODEL: silver_bsp_exchange_rate.sql
-- LAYER: Silver
-- SOURCE: bronze_bsp_exchange_rate
-- PURPOSE: Clean and type exchange rate data
-- ============================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_bsp_exchange_rate') }}
),

cleaned AS (
    SELECT
        TRY_CAST(FETCH_DATE AS DATE)        AS rate_date,
        YEAR(TRY_CAST(FETCH_DATE AS DATE))  AS rate_year,
        MONTH(TRY_CAST(FETCH_DATE AS DATE)) AS rate_month,
        TRIM(BASE_CURRENCY)                 AS base_currency,
        TRIM(TARGET_CURRENCY)               AS target_currency,
        TRY_CAST(USD_PHP_RATE AS FLOAT)     AS usd_php_rate,
        TRIM(SOURCE)                        AS data_source,
        CURRENT_TIMESTAMP()                 AS created_at
    FROM source
    WHERE USD_PHP_RATE IS NOT NULL
)

SELECT * FROM cleaned