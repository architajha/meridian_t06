-- =============================================================================
-- Project Meridian: Trainee 06 - Governance & Security Audit
-- Verifies:
-- 1. Grant least-privilege permissions: Consumers have SELECT on Gold only
-- 2. Audit query: Proves every column tagged with sensitive PII has an active mask
-- 3. Row-filter audit: Validates row security bindings on target tables
-- =============================================================================

USE CATALOG meridian_t06;

-- -----------------------------------------------------------------------------
-- 1. Least-Privilege Access Configuration
-- Revoke raw and staging layer access; grant consumers access to Gold and Metric Views only
-- -----------------------------------------------------------------------------
-- Allow read access to Gold schema and objects
GRANT USAGE ON CATALOG meridian_t06 TO `account users`;
GRANT USAGE ON SCHEMA gold TO `account users`;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO `account users`;
GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO `account users`;

-- Ensure Landing, Bronze, Silver, Gov, and Ops remain restricted to authorized engineers/admins
REVOKE ALL PRIVILEGES ON SCHEMA landing FROM `account users`;
REVOKE ALL PRIVILEGES ON SCHEMA bronze FROM `account users`;
REVOKE ALL PRIVILEGES ON SCHEMA silver FROM `account users`;
REVOKE ALL PRIVILEGES ON SCHEMA gov FROM `account users`;
REVOKE ALL PRIVILEGES ON SCHEMA ops FROM `account users`;

-- -----------------------------------------------------------------------------
-- 2. Audit Query: Prove Every Sensitive Tagged Column Has an Active Mask
-- Uses system.information_schema
-- -----------------------------------------------------------------------------
WITH sensitive_columns AS (
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        column_name,
        tag_name,
        tag_value
    FROM system.information_schema.column_tags
    WHERE table_catalog = 'meridian_t06'
      AND tag_name IN ('pii_type', 'sensitivity_level')
      AND tag_value IN ('name', 'financial', 'confidential', 'restricted')
),
masked_columns AS (
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        column_name,
        mask_name
    FROM system.information_schema.columns
    WHERE table_catalog = 'meridian_t06'
      AND mask_name IS NOT NULL
)
SELECT 
    s.table_schema,
    s.table_name,
    s.column_name,
    s.tag_name,
    s.tag_value,
    COALESCE(m.mask_name, 'NO_MASK_FOUND') AS assigned_mask,
    CASE 
        WHEN m.mask_name IS NOT NULL THEN 'COMPLIANT_SECURED'
        ELSE 'GOVERNANCE_BREACH_UNMASKED'
    END AS compliance_status
FROM sensitive_columns s
LEFT JOIN masked_columns m 
  ON s.table_catalog = m.table_catalog 
 AND s.table_schema = m.table_schema 
 AND s.table_name = m.table_name 
 AND s.column_name = m.column_name;

-- -----------------------------------------------------------------------------
-- 3. Row Filter Verification Audit
-- -----------------------------------------------------------------------------
SELECT 
    table_catalog,
    table_schema,
    table_name,
    row_filter
FROM system.information_schema.tables
WHERE table_catalog = 'meridian_t06'
  AND table_schema = 'gold'
  AND row_filter IS NOT NULL;
