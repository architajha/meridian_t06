-- =============================================================================
-- Project Meridian: Trainee 06 - Row-Level Security (Row Filters)
-- Objects: gov.fn_call_center_row_filter, gov.fn_sales_call_center_filter
-- Rule:
-- - Workspace administrators (t06_admins) see all rows
-- - Business users see only rows for call centres mapped to their email in gov.call_center_entitlements
-- - Supports multi-centre and hierarchical regional assignments
-- =============================================================================

USE CATALOG meridian_t06;

-- 1. Row Filter Function for Call Center Dimension
CREATE OR REPLACE FUNCTION gov.fn_call_center_row_filter(cc_id STRING)
RETURNS BOOLEAN
RETURN
    -- Admin override
    IS_ACCOUNT_GROUP_MEMBER('t06_admins')
    OR
    -- Entitlement lookup against mapping table
    EXISTS (
        SELECT 1 
        FROM gov.call_center_entitlements e
        WHERE e.user_email = CURRENT_USER()
          AND e.call_center_id = cc_id
    );

-- 2. Row Filter Function for Fact Catalog Sales
CREATE OR REPLACE FUNCTION gov.fn_sales_call_center_filter(cc_sk BIGINT)
RETURNS BOOLEAN
RETURN
    IS_ACCOUNT_GROUP_MEMBER('t06_admins')
    OR
    EXISTS (
        SELECT 1 
        FROM gold.dim_call_center cc
        JOIN gov.call_center_entitlements e ON cc.call_center_id = e.call_center_id
        WHERE e.user_email = CURRENT_USER()
          AND cc.call_center_sk = cc_sk
    );

-- 3. Attach Row Filter to Dimension and Fact Tables
ALTER TABLE gold.dim_call_center 
SET ROW FILTER gov.fn_call_center_row_filter ON (call_center_id);

ALTER TABLE gold.fct_catalog_sales 
SET ROW FILTER gov.fn_sales_call_center_filter ON (call_center_sk);

-- 4. Verification queries for security testing
-- Run as t06_admins: should return all call centres
-- Run as test manager: should return only permitted call centres
SELECT call_center_id, call_center_name, city, state 
FROM gold.dim_call_center;
