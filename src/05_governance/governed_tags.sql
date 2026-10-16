-- =============================================================================
-- Project Meridian: Trainee 06 - Governed Tag Policy
-- Defines controlled classification vocabulary and applies tags to schemas & columns
-- =============================================================================

USE CATALOG meridian_t06;

-- 1. Create Governed Tag Definitions
-- Tag 1: medallion_layer (Allowed: bronze, silver, gold, gov, ops)
-- Tag 2: sensitivity_level (Allowed: public, internal, confidential, restricted)
-- Tag 3: pii_type (Allowed: none, name, financial, identifier)

-- In Databricks Unity Catalog, tags can be set directly via SQL ALTER statements:
-- Schemas tagging
ALTER SCHEMA landing SET TAGS ('medallion_layer' = 'landing', 'sensitivity_level' = 'internal');
ALTER SCHEMA bronze SET TAGS ('medallion_layer' = 'bronze', 'sensitivity_level' = 'internal');
ALTER SCHEMA silver SET TAGS ('medallion_layer' = 'silver', 'sensitivity_level' = 'internal');
ALTER SCHEMA gold SET TAGS ('medallion_layer' = 'gold', 'sensitivity_level' = 'internal');
ALTER SCHEMA gov SET TAGS ('medallion_layer' = 'gov', 'sensitivity_level' = 'restricted');
ALTER SCHEMA ops SET TAGS ('medallion_layer' = 'ops', 'sensitivity_level' = 'internal');

-- Tables tagging
ALTER TABLE gold.fct_catalog_sales SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'fact');
ALTER TABLE gold.fct_catalog_returns SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'fact');
ALTER TABLE gold.dim_call_center SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'dimension', 'scd_type' = '2');
ALTER TABLE gold.dim_ship_mode SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'dimension');
ALTER TABLE gold.dim_catalog_page SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'dimension');
ALTER TABLE gold.dim_date SET TAGS ('medallion_layer' = 'gold', 'data_type' = 'dimension');

-- 2. Sensitive Column Classification (Package 06 Mandate)
-- Column 1: cc_manager (Contact centre manager personal name)
ALTER TABLE gold.dim_call_center ALTER COLUMN cc_manager SET TAGS (
    'sensitivity_level' = 'confidential',
    'pii_type' = 'name'
);

-- Column 2: cs_net_profit (Financial profit performance)
ALTER TABLE gold.fct_catalog_sales ALTER COLUMN cs_net_profit SET TAGS (
    'sensitivity_level' = 'restricted',
    'pii_type' = 'financial'
);
