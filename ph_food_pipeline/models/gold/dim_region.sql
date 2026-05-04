-- ============================================
-- MODEL: dim_region.sql
-- LAYER: Gold
-- PURPOSE: Region dimension table with
--          standardized Philippine regions
-- ============================================

WITH regions AS (
    SELECT DISTINCT region_name
    FROM {{ ref('silver_food_prices') }}
    WHERE region_name IS NOT NULL

    UNION

    SELECT DISTINCT region_name
    FROM {{ ref('silver_psa_cpi') }}
    WHERE region_name IS NOT NULL
        AND region_name != 'Philippines (National)'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['region_name']) }}     AS region_key,
    region_name,
    CASE
        WHEN region_name IN (
            'National Capital Region',
            'Cordillera Administrative Region',
            'Region I - Ilocos',
            'Region II - Cagayan Valley',
            'Region III - Central Luzon',
            'Region IV-A - CALABARZON',
            'Region IV-B - MIMAROPA',
            'Region V - Bicol'
        ) THEN 'Luzon'
        WHEN region_name IN (
            'Region VI - Western Visayas',
            'Region VII - Central Visayas',
            'Region VIII - Eastern Visayas'
        ) THEN 'Visayas'
        ELSE 'Mindanao'
    END                                                         AS island_group,
    CURRENT_TIMESTAMP()                                         AS created_at
FROM regions