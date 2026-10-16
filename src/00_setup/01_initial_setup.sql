-- =============================================================================
-- Project Meridian: Trainee 06 (Meridian Direct) - Initial Setup
-- Schemas: landing, bronze, silver, gold, gov, ops
-- Managed Volume: landing.raw_files
-- =============================================================================

-- 1. Confirm identity and SQL environment
SELECT current_user() AS me, current_version() AS dbsql_version;
SHOW SCHEMAS IN samples;

-- 2. Create Catalog for Project Meridian Trainee 06
CREATE CATALOG IF NOT EXISTS meridian_t06
  COMMENT 'Project Meridian capstone, trainee 06: Meridian Direct';

USE CATALOG meridian_t06;

-- 3. Create Schemas across the Medallion architecture and support layers
CREATE SCHEMA IF NOT EXISTS landing 
  COMMENT 'Raw file drops landing zone';

CREATE SCHEMA IF NOT EXISTS bronze 
  COMMENT 'Append-only raw tables with technical ingestion metadata';

CREATE SCHEMA IF NOT EXISTS silver 
  COMMENT 'Validated, deduplicated, conformed tables with data quality expectations';

CREATE SCHEMA IF NOT EXISTS gold 
  COMMENT 'Business-ready star schema models and metric views';

CREATE SCHEMA IF NOT EXISTS gov 
  COMMENT 'Security objects: row filter functions, column mask functions, entitlement mapping tables';

CREATE SCHEMA IF NOT EXISTS ops 
  COMMENT 'Data quality results, quarantine tables, and pipeline monitoring';

-- 4. Create Unity Catalog Managed Volume for batch file drops
CREATE VOLUME IF NOT EXISTS landing.raw_files
  COMMENT 'Managed landing zone for batch file simulation drops (/Volumes/meridian_t06/landing/raw_files/)';

-- 5. Verification queries
SHOW SCHEMAS IN meridian_t06;
SHOW VOLUMES IN meridian_t06.landing;

-- 6. Note on Group Creation:
-- In Databricks Free Edition / Workspace Settings > Identity and access > Groups:
-- Create group: t06_admins (Full administrative access, unmasked and unfiltered)
-- Create group: t06_pii_readers (Authorized to view unmasked PII and financial metrics)
-- Add current user to t06_admins.
