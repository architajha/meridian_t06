-- =============================================================================
-- Project Meridian: Trainee 06 - Governance Entitlements, SLA & Ops Tables
-- Objects: gov.call_center_entitlements, gov.shipping_sla_targets, ops.data_quality_gates
-- =============================================================================

USE CATALOG meridian_t06;

-- 1. Call Centre Entitlements Mapping Table (Row-Level Security)
-- Supports multi-value and hierarchical manager-to-centre assignments
CREATE TABLE IF NOT EXISTS gov.call_center_entitlements (
    user_email STRING COMMENT 'User principal email',
    call_center_id STRING COMMENT 'Authorized Call Center business ID',
    role_name STRING COMMENT 'User role: Regional Manager, Centre Lead, Analyst',
    created_at TIMESTAMP COMMENT 'Entitlement assignment timestamp'
)
COMMENT 'Security entitlement mapping table for dynamic row-level filtering on call centres';

-- Seed default entitlement mapping for testing
MERGE INTO gov.call_center_entitlements target
USING (
    SELECT 'manager_north@meridian.com' AS user_email, 'AAAAAAAABAAAAAAA' AS call_center_id, 'Regional Manager' AS role_name, current_timestamp() AS created_at
    UNION ALL
    SELECT 'manager_south@meridian.com' AS user_email, 'AAAAAAAACAAAAAAA' AS call_center_id, 'Regional Manager' AS role_name, current_timestamp() AS created_at
    UNION ALL
    SELECT current_user() AS user_email, 'AAAAAAAABAAAAAAA' AS call_center_id, 'Admin Tester' AS role_name, current_timestamp() AS created_at
    UNION ALL
    SELECT current_user() AS user_email, 'AAAAAAAACAAAAAAA' AS call_center_id, 'Admin Tester' AS role_name, current_timestamp() AS created_at
) source
ON target.user_email = source.user_email AND target.call_center_id = source.call_center_id
WHEN NOT MATCHED THEN INSERT *;

-- 2. Shipping SLA Target Table (Package 06 Advanced Challenge 2)
-- Parameter table holding maximum allowable shipping days (SLA target) per ship mode
CREATE TABLE IF NOT EXISTS gov.shipping_sla_targets (
    sm_type STRING COMMENT 'Shipping mode type (e.g., EXPRESS, REGULAR, NEXT DAY, OVERNIGHT)',
    target_sla_days INT COMMENT 'Contractual SLA in days between order date and ship date',
    target_on_time_pct DECIMAL(5, 2) COMMENT 'Target on-time fulfillment percentage (e.g., 95.00%)'
)
COMMENT 'Parameter table defining shipping SLA thresholds for on-time delivery metric';

MERGE INTO gov.shipping_sla_targets target
USING (
    SELECT 'EXPRESS' AS sm_type, 2 AS target_sla_days, 98.00 AS target_on_time_pct
    UNION ALL
    SELECT 'NEXT DAY' AS sm_type, 1 AS target_sla_days, 99.00 AS target_on_time_pct
    UNION ALL
    SELECT 'OVERNIGHT' AS sm_type, 1 AS target_sla_days, 99.50 AS target_on_time_pct
    UNION ALL
    SELECT 'REGULAR' AS sm_type, 5 AS target_sla_days, 95.00 AS target_on_time_pct
    UNION ALL
    SELECT 'STANDARD' AS sm_type, 7 AS target_sla_days, 92.00 AS target_on_time_pct
    UNION ALL
    SELECT 'TWO DAY' AS sm_type, 2 AS target_sla_days, 97.50 AS target_on_time_pct
) source
ON target.sm_type = source.sm_type
WHEN NOT MATCHED THEN INSERT *;

-- 3. Operations Quality Gate History Table
-- Records automated post-pipeline validation checks
CREATE TABLE IF NOT EXISTS ops.data_quality_gates (
    check_id STRING,
    check_name STRING,
    check_layer STRING,
    status STRING, -- PASS, FAIL, WARN
    actual_value DOUBLE,
    threshold_value DOUBLE,
    details STRING,
    execution_time TIMESTAMP
)
COMMENT 'Audit table recording pipeline data quality gate outcomes';

-- 4. Quarantine Tables in Ops Schema
-- Captures rejected records from Silver data quality expectations for full reconciliation
CREATE TABLE IF NOT EXISTS ops.quarantine_catalog_sales (
    raw_record STRING,
    violation_rule STRING,
    violation_reason STRING,
    source_file STRING,
    batch_id INT,
    quarantine_timestamp TIMESTAMP
)
COMMENT 'Quarantined bad records from catalog_sales Silver ingestion';

CREATE TABLE IF NOT EXISTS ops.quarantine_catalog_returns (
    raw_record STRING,
    violation_rule STRING,
    violation_reason STRING,
    source_file STRING,
    batch_id INT,
    quarantine_timestamp TIMESTAMP
)
COMMENT 'Quarantined bad records from catalog_returns Silver ingestion';
