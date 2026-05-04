-- ============================================
-- MODEL: dim_date.sql
-- LAYER: Gold
-- PURPOSE: Date dimension table (monthly grain)
-- ============================================

WITH date_spine AS (
    SELECT DISTINCT
        price_year  AS year,
        price_month AS month
    FROM {{ ref('silver_food_prices') }}
    WHERE price_year IS NOT NULL
        AND price_month IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }}    AS date_key,
    year,
    month,
    CASE month
        WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
        WHEN 9  THEN 'September' WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
    END                                                          AS month_name,
    CASE
        WHEN month IN (1,2,3)   THEN 'Q1'
        WHEN month IN (4,5,6)   THEN 'Q2'
        WHEN month IN (7,8,9)   THEN 'Q3'
        ELSE 'Q4'
    END                                                          AS quarter,
    CURRENT_TIMESTAMP()                                          AS created_at
FROM date_spine