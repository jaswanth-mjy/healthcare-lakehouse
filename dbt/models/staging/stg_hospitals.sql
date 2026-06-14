-- =============================================================================
-- stg_hospitals.sql  |  Silver Layer — Staging
-- =============================================================================
-- Purpose : Clean, rename, and type-cast raw CMS hospital data from Bronze.
--           No business logic here — just trusted, clean records.
-- Source  : bronze/hospitals (JSON partitioned by ingestion_date)
-- =============================================================================

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'hospitals') }}
),

cleaned AS (
    SELECT
        -- ── Identity ──────────────────────────────────────────────────────────
        LPAD(CAST(facility_id AS VARCHAR), 6, '0')   AS facility_id,    -- Preserve leading zeros
        INITCAP(TRIM(facility_name))                  AS hospital_name,
        INITCAP(TRIM(address))                        AS address,
        INITCAP(TRIM(city_town))                      AS city,
        UPPER(TRIM(state))                            AS state,
        LPAD(CAST(zip_code AS VARCHAR), 5, '0')       AS zip_code,
        INITCAP(TRIM(county_parish))                  AS county,
        TRIM(telephone_number)                        AS phone,

        -- ── Classification ───────────────────────────────────────────────────
        TRIM(hospital_type)                           AS hospital_type,
        TRIM(hospital_ownership)                      AS hospital_ownership,
        CASE UPPER(TRIM(emergency_services))
            WHEN 'YES' THEN TRUE
            WHEN 'NO'  THEN FALSE
            ELSE NULL
        END                                           AS has_emergency_services,
        CASE UPPER(TRIM(meets_criteria_for_birthing_friendly_designation))
            WHEN 'Y' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END                                           AS is_birthing_friendly,

        -- ── Overall Rating ───────────────────────────────────────────────────
        CASE
            WHEN TRIM(hospital_overall_rating) ~ '^[1-5]$'
            THEN CAST(hospital_overall_rating AS INTEGER)
            ELSE NULL
        END                                           AS overall_rating,

        -- ── Mortality (MORT) Measures ─────────────────────────────────────────
        CAST(count_of_facility_mort_measures AS INTEGER)  AS mort_measures_count,
        CAST(count_of_mort_measures_better   AS INTEGER)  AS mort_better,
        CAST(count_of_mort_measures_no_different AS INTEGER) AS mort_same,
        CAST(count_of_mort_measures_worse    AS INTEGER)  AS mort_worse,

        -- ── Safety Measures ───────────────────────────────────────────────────
        CAST(count_of_facility_safety_measures AS INTEGER) AS safety_measures_count,
        CAST(count_of_safety_measures_better   AS INTEGER) AS safety_better,
        CAST(count_of_safety_measures_no_different AS INTEGER) AS safety_same,
        CAST(count_of_safety_measures_worse    AS INTEGER) AS safety_worse,

        -- ── Readmission (READM) Measures ─────────────────────────────────────
        CAST(count_of_facility_readm_measures AS INTEGER) AS readm_measures_count,
        CAST(count_of_readm_measures_better   AS INTEGER) AS readm_better,
        CAST(count_of_readm_measures_no_different AS INTEGER) AS readm_same,
        CAST(count_of_readm_measures_worse    AS INTEGER) AS readm_worse,

        -- ── Patient Experience (Pt Exp) ───────────────────────────────────────
        CAST(count_of_facility_pt_exp_measures AS INTEGER) AS pt_exp_measures_count,

        -- ── Timeliness & Efficiency (TE) ──────────────────────────────────────
        CAST(count_of_facility_te_measures AS INTEGER)    AS te_measures_count,

        -- ── Derived Quality Score (0–100) ─────────────────────────────────────
        -- Simple score: % of measures rated "Better" across MORT + Safety + READM
        CASE
            WHEN (
                COALESCE(CAST(count_of_facility_mort_measures   AS INTEGER), 0) +
                COALESCE(CAST(count_of_facility_safety_measures AS INTEGER), 0) +
                COALESCE(CAST(count_of_facility_readm_measures  AS INTEGER), 0)
            ) = 0 THEN NULL
            ELSE ROUND(
                100.0 * (
                    COALESCE(CAST(count_of_mort_measures_better   AS INTEGER), 0) +
                    COALESCE(CAST(count_of_safety_measures_better AS INTEGER), 0) +
                    COALESCE(CAST(count_of_readm_measures_better  AS INTEGER), 0)
                ) / (
                    COALESCE(CAST(count_of_facility_mort_measures   AS INTEGER), 0) +
                    COALESCE(CAST(count_of_facility_safety_measures AS INTEGER), 0) +
                    COALESCE(CAST(count_of_facility_readm_measures  AS INTEGER), 0)
                ), 1
            )
        END                                            AS quality_score,

        -- ── Metadata passthrough ──────────────────────────────────────────────
        _source,
        _ingested_at,
        _ingestion_date

    FROM raw
    WHERE facility_id IS NOT NULL     -- drop header artifacts or blank rows
)

SELECT * FROM cleaned
