-- ============================================
-- MODEL: silver_psa_cpi.sql
-- LAYER: Silver
-- SOURCE: bronze_psa_cpi
-- PURPOSE: Clean and standardize PSA CPI data
-- ============================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_psa_cpi') }}
),

cleaned AS (
    SELECT
        -- Region standardization
        CASE
            WHEN UPPER(TRIM(REGION)) IN ('PHILIPPINES')
                THEN 'Philippines (National)'
            WHEN UPPER(TRIM(REGION)) LIKE '%NATIONAL CAPITAL%'
                THEN 'National Capital Region'
            WHEN UPPER(TRIM(REGION)) LIKE '%CORDILLERA%'
                THEN 'Cordillera Administrative Region'
            WHEN UPPER(TRIM(REGION)) LIKE '%MUSLIM MINDANAO%'
                OR UPPER(TRIM(REGION)) LIKE '%BANGSAMORO%'
                OR UPPER(TRIM(REGION)) LIKE '%BARMM%'
                THEN 'Bangsamoro Autonomous Region'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION I%'
                AND UPPER(TRIM(REGION)) NOT LIKE '%REGION II%'
                AND UPPER(TRIM(REGION)) NOT LIKE '%REGION III%'
                THEN 'Region I - Ilocos'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION II%'
                AND UPPER(TRIM(REGION)) NOT LIKE '%REGION III%'
                THEN 'Region II - Cagayan Valley'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION III%'
                THEN 'Region III - Central Luzon'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION IV-A%'
                OR UPPER(TRIM(REGION)) LIKE '%CALABARZON%'
                THEN 'Region IV-A - CALABARZON'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION IV-B%'
                OR UPPER(TRIM(REGION)) LIKE '%MIMAROPA%'
                THEN 'Region IV-B - MIMAROPA'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION V%'
                THEN 'Region V - Bicol'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION VI%'
                THEN 'Region VI - Western Visayas'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION VII%'
                THEN 'Region VII - Central Visayas'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION VIII%'
                THEN 'Region VIII - Eastern Visayas'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION IX%'
                THEN 'Region IX - Zamboanga Peninsula'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION X%'
                THEN 'Region X - Northern Mindanao'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION XI%'
                THEN 'Region XI - Davao'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION XII%'
                THEN 'Region XII - SOCCSKSARGEN'
            WHEN UPPER(TRIM(REGION)) LIKE '%REGION XIII%'
                OR UPPER(TRIM(REGION)) LIKE '%CARAGA%'
                THEN 'Region XIII - Caraga'
            ELSE TRIM(REGION)
        END                                     AS region_name,

        -- Commodity
        TRIM(COMMODITY)                         AS commodity_description,

        -- Extract commodity code e.g. "01.1" from "01.1 - FOOD"
        CASE
            WHEN COMMODITY LIKE '%-%'
                THEN TRIM(SPLIT_PART(COMMODITY, '-', 1))
            ELSE NULL
        END                                     AS commodity_code,

        -- Time
        TRY_CAST(YEAR AS INT)                   AS cpi_year,
        TRIM(MONTH)                             AS cpi_month,

        -- Convert month name to number
        CASE TRIM(MONTH)
            WHEN 'Jan' THEN 1  WHEN 'Feb' THEN 2
            WHEN 'Mar' THEN 3  WHEN 'Apr' THEN 4
            WHEN 'May' THEN 5  WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7  WHEN 'Aug' THEN 8
            WHEN 'Sep' THEN 9  WHEN 'Oct' THEN 10
            WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
        END                                     AS cpi_month_num,

        -- CPI value
        TRY_CAST(CPI_VALUE AS FLOAT)            AS cpi_value,

        -- Source tracking
        TRIM(SOURCE_FILE)                       AS source_file,
        CURRENT_TIMESTAMP()                     AS created_at

    FROM source
    WHERE
        REGION IS NOT NULL
        AND COMMODITY IS NOT NULL
        AND CPI_VALUE IS NOT NULL
        AND TRIM(MONTH) != 'Ave'  -- exclude annual averages
)

SELECT * FROM cleaned
WHERE cpi_value IS NOT NULL