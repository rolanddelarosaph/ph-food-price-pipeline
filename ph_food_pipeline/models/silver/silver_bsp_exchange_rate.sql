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
        TRY_CAST(MONTH_DATE AS DATE)        AS rate_month_date,
        TRY_CAST(YEAR AS INT)               AS rate_year,
        TRY_CAST(MONTH AS INT)              AS rate_month,
        TRIM(MONTH_NAME)                    AS rate_month_name,
        TRY_CAST(USD_PHP_RATE AS FLOAT)     AS usd_php_rate,
        TRY_CAST(NUM_TRADING_DAYS AS INT)   AS num_trading_days,
        TRIM(SOURCE)                        AS data_source,
        CURRENT_TIMESTAMP()                 AS created_at
    FROM source
    WHERE
        MONTH_DATE IS NOT NULL
        AND USD_PHP_RATE IS NOT NULL
)

SELECT * FROM cleaned
ORDER BY rate_month_date