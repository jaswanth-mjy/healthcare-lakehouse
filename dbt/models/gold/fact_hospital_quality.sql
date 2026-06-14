-- =============================================================================
-- fact_hospital_quality.sql  |  Gold Layer — Fact Table
-- =============================================================================
-- Purpose : One row per hospital per ingestion snapshot.
--           Tracks quality metrics over time (slowly changing fact).
-- Grain   : facility_id + _ingestion_date
-- =============================================================================

WITH silver AS (
    SELECT * FROM {{ ref('stg_hospitals') }}
),

dim AS (
    SELECT hospital_key, facility_id FROM {{ ref('dim_hospital') }}
)

SELECT
    -- ── Keys ──────────────────────────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['s.facility_id', 's._ingestion_date']) }} AS quality_fact_key,
    d.hospital_key,
    s.facility_id,
    s._ingestion_date                             AS snapshot_date,

    -- ── Rating ───────────────────────────────────────────────────────────────
    s.overall_rating,
    s.quality_score,

    -- ── Mortality ─────────────────────────────────────────────────────────────
    s.mort_measures_count,
    s.mort_better,
    s.mort_same,
    s.mort_worse,
    ROUND(100.0 * s.mort_better / NULLIF(s.mort_measures_count, 0), 1) AS mort_pct_better,

    -- ── Safety ───────────────────────────────────────────────────────────────
    s.safety_measures_count,
    s.safety_better,
    s.safety_same,
    s.safety_worse,
    ROUND(100.0 * s.safety_better / NULLIF(s.safety_measures_count, 0), 1) AS safety_pct_better,

    -- ── Readmission ───────────────────────────────────────────────────────────
    s.readm_measures_count,
    s.readm_better,
    s.readm_same,
    s.readm_worse,
    ROUND(100.0 * s.readm_better / NULLIF(s.readm_measures_count, 0), 1) AS readm_pct_better,

    -- ── Patient Experience ────────────────────────────────────────────────────
    s.pt_exp_measures_count,

    -- ── Timeliness & Efficiency ───────────────────────────────────────────────
    s.te_measures_count,

    -- ── Metadata ─────────────────────────────────────────────────────────────
    s._source,
    s._ingested_at

FROM silver s
LEFT JOIN dim d ON s.facility_id = d.facility_id
