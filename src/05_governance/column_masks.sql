-- =============================================================================
-- Project Meridian: Trainee 06 - Column-Level Security (Column Masks)
-- Objects: gov.mask_manager_name, gov.mask_net_profit
-- Targets:
-- 1. gold.dim_call_center.cc_manager (Personal name -> Partial masking)
-- 2. gold.fct_catalog_sales.cs_net_profit (Financial metric -> Full redaction)
-- Entitled Groups: t06_pii_readers, t06_admins
-- =============================================================================

USE CATALOG meridian_t06;

-- 1. Column Mask for Manager Name (Partial Masking)
-- Preserves first and last letter for diagnostic purposes while masking private name
CREATE OR REPLACE FUNCTION gov.mask_manager_name(val STRING)
RETURNS STRING
RETURN
    CASE 
        WHEN IS_ACCOUNT_GROUP_MEMBER('t06_pii_readers') OR IS_ACCOUNT_GROUP_MEMBER('t06_admins')
            THEN val
        WHEN val IS NULL
            THEN NULL
        WHEN LENGTH(val) <= 2
            THEN '**'
        ELSE CONCAT(SUBSTRING(val, 1, 1), '*****', SUBSTRING(val, LENGTH(val), 1))
    END;

-- 2. Column Mask for Net Profit (Full Redaction)
-- Strictly suppresses financial profit margins from non-privileged viewers
CREATE OR REPLACE FUNCTION gov.mask_net_profit(val DECIMAL(7,2))
RETURNS DECIMAL(7,2)
RETURN
    CASE 
        WHEN IS_ACCOUNT_GROUP_MEMBER('t06_pii_readers') OR IS_ACCOUNT_GROUP_MEMBER('t06_admins')
            THEN val
        ELSE CAST(NULL AS DECIMAL(7,2))
    END;

-- 3. Attach Column Masks to Gold Tables
ALTER TABLE gold.dim_call_center 
ALTER COLUMN cc_manager SET MASK gov.mask_manager_name;

ALTER TABLE gold.fct_catalog_sales 
ALTER COLUMN cs_net_profit SET MASK gov.mask_net_profit;

-- 4. Verification Test Query
SELECT 
    cc.call_center_name,
    cc.cc_manager AS masked_or_unmasked_manager,
    s.order_number,
    s.net_sales_amount,
    s.cs_net_profit AS masked_or_unmasked_profit
FROM gold.fct_catalog_sales s
JOIN gold.dim_call_center cc ON s.call_center_sk = cc.call_center_sk
LIMIT 10;
