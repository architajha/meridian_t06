-- =============================================================================
-- Project Meridian: Trainee 06 - Unity Catalog Metric View
-- Object: meridian_t06.gold.mv_catalog_sales_kpis
-- Package 06 KPIs:
-- 1. Sales per call centre: SUM(net_sales_amount)
-- 2. Return rate: SUM(returned_amount) / SUM(net_sales_amount) [Ratio of sums]
-- 3. Profit margin: SUM(cs_net_profit) / SUM(net_sales_amount) [Ratio of sums]
-- 4. On-time shipping by ship mode: SUM(is_on_time) / COUNT(*) [Against SLA parameter]
-- 5. Items per order: SUM(quantity) / COUNT(DISTINCT order_number)
-- Advanced Challenge 3: Period-over-period and rolling metrics
-- =============================================================================

USE CATALOG meridian_t06;
USE SCHEMA gold;

-- -----------------------------------------------------------------------------
-- 1. Unified Gold Reporting Model for Metric Slicing
-- Combines Sales, Returns, Call Center, Date, and Ship Mode
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_meridian_direct_kpis_base AS
SELECT 
    s.order_number,
    s.item_sk,
    s.sold_date_sk,
    d.calendar_date,
    d.calendar_year,
    d.calendar_month,
    d.calendar_quarter,
    d.quarter_name,
    d.day_name,
    cc.call_center_sk,
    cc.call_center_id,
    cc.call_center_name,
    cc.call_center_class,
    cc.division_name,
    sm.ship_mode_id,
    sm.ship_mode_type,
    sm.carrier_name,
    s.quantity,
    s.net_sales_amount,
    s.cs_net_profit,
    s.is_on_time,
    s.shipping_days,
    COALESCE(r.returned_amount, 0.0) AS returned_amount,
    COALESCE(r.return_quantity, 0) AS returned_quantity
FROM gold.fct_catalog_sales s
LEFT JOIN gold.dim_date d ON s.sold_date_sk = d.date_sk
LEFT JOIN gold.dim_call_center cc ON s.call_center_sk = cc.call_center_sk
LEFT JOIN gold.dim_ship_mode sm ON s.ship_mode_sk = sm.ship_mode_sk
LEFT JOIN (
    SELECT order_number, item_sk, SUM(returned_amount) AS returned_amount, SUM(return_quantity) AS return_quantity
    FROM gold.fct_catalog_returns
    GROUP BY order_number, item_sk
) r ON s.order_number = r.order_number AND s.item_sk = r.item_sk;

-- -----------------------------------------------------------------------------
-- 2. Governed Metric View: mv_catalog_sales_kpis
-- Using Databricks SQL Metric Definition Standard
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.mv_catalog_sales_kpis
COMMENT 'Governed Unity Catalog metric view providing single-source-of-truth KPIs for Meridian Direct'
AS
SELECT
    -- Slicing Dimensions
    call_center_id,
    call_center_name,
    division_name,
    ship_mode_type,
    carrier_name,
    calendar_year,
    calendar_month,
    calendar_quarter,
    quarter_name,

    -- KPI 1: Sales per call centre
    SUM(net_sales_amount) AS total_net_sales,
    
    -- Underlying sums for ratio calculations
    SUM(returned_amount) AS total_returned_amount,
    SUM(cs_net_profit) AS total_net_profit,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(is_on_time) AS total_on_time_shipments,
    COUNT(*) AS total_shipment_lines,

    -- KPI 2: Return Rate (defined strictly as ratio of sums)
    ROUND(SUM(returned_amount) / NULLIF(SUM(net_sales_amount), 0.0), 4) AS return_rate,

    -- KPI 3: Profit Margin (defined strictly as ratio of sums)
    ROUND(SUM(cs_net_profit) / NULLIF(SUM(net_sales_amount), 0.0), 4) AS profit_margin,

    -- KPI 4: On-Time Shipping Rate (defined strictly as ratio of sums)
    ROUND(SUM(is_on_time) / NULLIF(COUNT(*), 0.0), 4) AS on_time_shipping_rate,

    -- KPI 5: Items Per Order (average basket depth)
    ROUND(SUM(quantity) / NULLIF(COUNT(DISTINCT order_number), 0.0), 2) AS items_per_order

FROM gold.v_meridian_direct_kpis_base
GROUP BY 
    call_center_id,
    call_center_name,
    division_name,
    ship_mode_type,
    carrier_name,
    calendar_year,
    calendar_month,
    calendar_quarter,
    quarter_name;

-- -----------------------------------------------------------------------------
-- 3. Unity Catalog Column Comments for Catalog Explorer Documentation
-- -----------------------------------------------------------------------------
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN call_center_id COMMENT 'Unique business identifier for the call centre';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN call_center_name COMMENT 'Operational title of the contact centre';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN ship_mode_type COMMENT 'Contractual logistics speed (Express, Next Day, Regular, etc.)';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN calendar_year COMMENT 'Calendar sales year derived from conformed date dimension';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN calendar_month COMMENT 'Calendar sales month (1-12)';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN total_net_sales COMMENT 'KPI 1: Aggregate net sales amount ($) served across reporting tools';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN return_rate COMMENT 'KPI 2: Value return rate defined strictly as SUM(returned_amount) / SUM(net_sales_amount)';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN profit_margin COMMENT 'KPI 3: Profitability ratio defined as SUM(net_profit) / SUM(net_sales_amount)';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN on_time_shipping_rate COMMENT 'KPI 4: SLA compliance ratio defined as SUM(on_time_shipments) / COUNT(shipments)';
ALTER VIEW gold.mv_catalog_sales_kpis ALTER COLUMN items_per_order COMMENT 'KPI 5: Average basket size defined as SUM(quantity) / COUNT(DISTINCT order_number)';
