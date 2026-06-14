-- =============================================================================
-- dim_hospital.sql  |  Gold Layer — Dimension Table
-- =============================================================================
-- Purpose : Stable hospital dimension for the star schema.
--           One row per hospital. Used by fact_hospital_quality.
-- =============================================================================

WITH silver AS (
    SELECT * FROM {{ ref('stg_hospitals') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['facility_id']) }}  AS hospital_key,  -- surrogate PK
    facility_id,
    hospital_name,
    address,
    city,
    state,
    zip_code,
    county,
    phone,
    hospital_type,
    hospital_ownership,
    has_emergency_services,
    is_birthing_friendly,
    _ingestion_date                                          AS valid_from  -- SCD Type 1 marker
FROM silver
