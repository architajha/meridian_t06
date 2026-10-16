-- =============================================================================
-- Project Meridian: Trainee 06 - Reconciliation & Validation Queries
-- Package 06: Meridian Direct
-- Contains:
-- 1. Metric View vs Direct Fact Table KPI Reconciliation
-- 2. Bronze vs Silver + Quarantine Count Reconciliation
-- 3. Advanced Challenge 1: Native TPC-DS History vs Pipeline SCD Type 2 Reconciliation
-- 4. Advanced Challenge 2: Shipping SLA On-Time Rate vs Target Parameter Table
-- 5. Advanced Challenge 3: Rolling & Period-over-Period Performance
-- =============================================================================

USE CATALOG meridian_t06;

-- =============================================================================
-- 1. KPI Reconciliation: Metric View vs Fact Table
-- Proves metric view produces mathematically identical results to raw fact queries
-- =============================================================================
WITH fact_direct AS (
    SELECT 
        'Direct Fact Query' AS calculation_source,
        ROUND(SUM(s.net_sales_amount), 2) AS net_sales,
        ROUND(SUM(s.cs_net_profit) / NULLIF(SUM(s.net_sales_amount), 0), 4) AS profit_margin,
        ROUND(SUM(s.quantity) / NULLIF(COUNT(DISTINCT s.order_number), 0), 2) AS items_per_order
    FROM gold.fct_catalog_sales s
),
metric_view_direct AS (
    SELECT 
        'Governed Metric View' AS calculation_source,
        ROUND(SUM(total_net_sales), 2) AS net_sales,
        ROUND(SUM(total_net_profit) / NULLIF(SUM(total_net_sales), 0), 4) AS profit_margin,
        ROUND(SUM(total_units_sold) / NULLIF(SUM(total_orders), 0), 2) AS items_per_order
    FROM gold.mv_catalog_sales_kpis
)
SELECT * FROM fact_direct
UNION ALL
SELECT * FROM metric_view_direct;

-- =============================================================================
-- 2. Bronze vs Silver + Quarantine Count Reconciliation
-- Proves that no records were dropped silently (Audit Requirement)
-- =============================================================================
SELECT 
    'catalog_sales' AS dataset,
    (SELECT COUNT(*) FROM bronze.raw_catalog_sales) AS bronze_total_rows,
    (SELECT COUNT(*) FROM silver.silver_catalog_sales) AS silver_clean_rows,
    (SELECT COUNT(*) FROM ops.quarantine_catalog_sales) AS quarantine_dropped_rows,
    ((SELECT COUNT(*) FROM silver.silver_catalog_sales) + (SELECT COUNT(*) FROM ops.quarantine_catalog_sales)) AS silver_plus_quarantine,
    CASE 
        WHEN (SELECT COUNT(*) FROM bronze.raw_catalog_sales) = 
             ((SELECT COUNT(*) FROM silver.silver_catalog_sales) + (SELECT COUNT(*) FROM ops.quarantine_catalog_sales))
        THEN 'RECONCILED_EXACT' 
        ELSE 'MISMATCH_DETECTED' 
    END AS reconciliation_status;

-- =============================================================================
-- 3. Advanced Challenge 1: Native TPC-DS History vs Pipeline SCD Type 2
-- Reconciles native rec_start_date/rec_end_date against Lakeflow __START_AT/__END_AT
-- =============================================================================
WITH native_tpcds AS (
    SELECT 
        cc_call_center_id,
        COUNT(*) AS native_versions_count,
        MIN(cc_rec_start_date) AS native_earliest_start,
        MAX(COALESCE(cc_rec_end_date, '9999-12-31')) AS native_latest_end,
        COUNT(CASE WHEN cc_rec_end_date IS NULL THEN 1 END) AS native_current_count
    FROM samples.tpcds_sf1.call_center
    GROUP BY cc_call_center_id
),
pipeline_scd2 AS (
    SELECT 
        call_center_id,
        COUNT(*) AS pipeline_versions_count,
        MIN(valid_from) AS pipeline_earliest_start,
        MAX(COALESCE(valid_to, TIMESTAMP '9999-12-31 00:00:00')) AS pipeline_latest_end,
        COUNT(CASE WHEN is_current = TRUE THEN 1 END) AS pipeline_current_count
    FROM gold.dim_call_center
    GROUP BY call_center_id
)
SELECT 
    n.cc_call_center_id,
    n.native_versions_count,
    p.pipeline_versions_count,
    n.native_current_count,
    p.pipeline_current_count,
    CASE 
        WHEN n.native_current_count = p.pipeline_current_count THEN 'CURRENT_FLAG_ALIGNED'
        ELSE 'CURRENT_FLAG_DIFFERS'
    END AS current_alignment,
    'TPC-DS native history uses static date grains; Pipeline Lakeflow AUTO CDC uses precise streaming commit timestamps with automated __START_AT and __END_AT tracking across ingestion batches.' AS structural_explanation
FROM native_tpcds n
LEFT JOIN pipeline_scd2 p ON n.cc_call_center_id = p.call_center_id;

-- =============================================================================
-- 4. Advanced Challenge 2: Shipping SLA Against Parameter Table Targets
-- Evaluates actual on-time performance against contractual SLAs held in gov schema
-- =============================================================================
SELECT 
    mv.ship_mode_type,
    sla.target_sla_days,
    sla.target_on_time_pct,
    ROUND(mv.on_time_shipping_rate * 100, 2) AS actual_on_time_pct,
    ROUND((mv.on_time_shipping_rate * 100) - sla.target_on_time_pct, 2) AS sla_variance_pct,
    CASE 
        WHEN (mv.on_time_shipping_rate * 100) >= sla.target_on_time_pct THEN 'SLA_MET'
        ELSE 'SLA_BREACH'
    END AS sla_compliance_status
FROM (
    SELECT 
        ship_mode_type,
        SUM(total_on_time_shipments) / NULLIF(SUM(total_shipment_lines), 0) AS on_time_shipping_rate
    FROM gold.mv_catalog_sales_kpis
    GROUP BY ship_mode_type
) mv
LEFT JOIN gov.shipping_sla_targets sla ON mv.ship_mode_type = sla.sm_type
ORDER BY sla_variance_pct ASC;

-- =============================================================================
-- 5. Advanced Challenge 3: Rolling and Period-over-Period Metric Slicing
-- =============================================================================
SELECT 
    call_center_name,
    calendar_year,
    calendar_month,
    total_net_sales,
    LAG(total_net_sales, 1) OVER (PARTITION BY call_center_id ORDER BY calendar_year, calendar_month) AS prior_month_sales,
    ROUND(
        (total_net_sales - LAG(total_net_sales, 1) OVER (PARTITION BY call_center_id ORDER BY calendar_year, calendar_month)) /
        NULLIF(LAG(total_net_sales, 1) OVER (PARTITION BY call_center_id ORDER BY calendar_year, calendar_month), 0.0) * 100,
        2
    ) AS mom_sales_growth_pct,
    return_rate,
    profit_margin
FROM gold.mv_catalog_sales_kpis
ORDER BY call_center_name, calendar_year, calendar_month;
