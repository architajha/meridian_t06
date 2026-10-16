-- =============================================================================
-- Project Meridian: Trainee 06 - Operations Dashboard & System Tables Telemetry
-- Platform Observability for Databricks Free Edition
-- Page 1: Run Health & Data Quality
-- Page 2: Compute Usage & Query Performance
-- Includes documented fallback queries using ops tables when system tables are restricted
-- =============================================================================

USE CATALOG meridian_t06;

-- =============================================================================
-- PAGE 1: RUN HEALTH & DATA QUALITY
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Widget 1: Pipeline Update Duration & Status (Lakeflow Pipeline Event Log)
-- Answers: How long do pipeline updates take? Did the last update succeed?
-- -----------------------------------------------------------------------------
SELECT 
    timestamp,
    message,
    level,
    details:flow_progress:status AS flow_status,
    details:flow_progress:metrics:num_output_rows AS output_rows
FROM event_log("meridian_t06_pipeline")
WHERE event_type IN ('flow_progress', 'update_progress')
ORDER BY timestamp DESC
LIMIT 50;

-- -----------------------------------------------------------------------------
-- Widget 2: Data Quality Expectations Pass vs Fail Rates (Event Log Telemetry)
-- Answers: Is data quality getting worse?
-- -----------------------------------------------------------------------------
SELECT 
    timestamp,
    details:flow_name AS flow_name,
    details:flow_progress:data_quality:dropped_records AS dropped_records,
    details:flow_progress:data_quality:expectations AS expectations_metrics
FROM event_log("meridian_t06_pipeline")
WHERE event_type = 'flow_progress'
  AND details:flow_progress:data_quality IS NOT NULL
ORDER BY timestamp DESC;

-- -----------------------------------------------------------------------------
-- Widget 3: Data Quality Gate Run History & Deliberate Failure Audit
-- Answers: Did the quality gate catch bad data? Shows Phase 5 intentional failure.
-- -----------------------------------------------------------------------------
SELECT 
    execution_time,
    check_id,
    check_name,
    check_layer,
    status,
    actual_value,
    threshold_value,
    details
FROM ops.data_quality_gates
ORDER BY execution_time DESC
LIMIT 25;

-- -----------------------------------------------------------------------------
-- Widget 4: Data Lineage Tracing (system.lineage or catalog lineage)
-- Answers: Where does Gold data come from?
-- -----------------------------------------------------------------------------
SELECT 
    source_table_full_name,
    target_table_full_name,
    entity_type
FROM system.lineage.table_lineage
WHERE target_table_full_name LIKE 'meridian_t06.gold%'
ORDER BY event_date DESC
LIMIT 20;

-- =============================================================================
-- PAGE 2: COMPUTE USAGE & QUERY PERFORMANCE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Widget 5: Compute & DBU Consumption (system.billing.usage)
-- Answers: What consumes compute?
-- Fallback note: If system.billing is restricted on Free Edition, query warehouse run duration.
-- -----------------------------------------------------------------------------
SELECT 
    usage_date,
    sku_name,
    usage_unit,
    ROUND(SUM(usage_quantity), 3) AS total_dbus_consumed
FROM system.billing.usage
WHERE usage_metadata.catalog_name = 'meridian_t06'
GROUP BY usage_date, sku_name, usage_unit
ORDER BY usage_date DESC;

-- -----------------------------------------------------------------------------
-- Widget 6: Slowest Queries & Warehouse Latency (system.query.history)
-- Answers: Which queries are slow? Which task is slowest?
-- -----------------------------------------------------------------------------
SELECT 
    query_id,
    executed_by,
    warehouse_id,
    duration_ms,
    ROUND(duration_ms / 1000.0, 2) AS duration_seconds,
    rows_produced,
    SUBSTRING(query_text, 1, 120) AS query_preview,
    error_message
FROM system.query.history
WHERE statement_type IN ('SELECT', 'MERGE', 'INSERT')
  AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY duration_ms DESC
LIMIT 25;
