-- =============================================================================
-- Project Meridian: Trainee 06 - Reusable Parameterized Queries (SQL Editor)
-- Targets: Typed parameters in Databricks SQL (Text, Date, Number)
-- =============================================================================

USE CATALOG meridian_t06;

-- -----------------------------------------------------------------------------
-- Query 1: Governed Call Centre Performance Query (Text & Numeric Parameters)
-- Queries the Metric View: gold.mv_catalog_sales_kpis
-- Parameters:
--   :p_call_center (STRING) - Filter by Call Centre Name (e.g., 'Midwest Direct')
--   :p_min_sales (DOUBLE)   - Minimum net sales threshold (e.g., 50000.0)
-- -----------------------------------------------------------------------------
SELECT 
    call_center_name,
    division_name,
    calendar_year,
    calendar_quarter,
    total_net_sales,
    total_orders,
    items_per_order,
    return_rate,
    profit_margin
FROM gold.mv_catalog_sales_kpis
WHERE (:p_call_center = 'ALL' OR call_center_name = :p_call_center)
  AND total_net_sales >= :p_min_sales
ORDER BY total_net_sales DESC;

-- -----------------------------------------------------------------------------
-- Query 2: Shipping SLA Compliance & Delivery Duration (Date & Text Parameters)
-- Queries Gold Fact joined with Date Dimension & SLA Parameter Table
-- Parameters:
--   :p_start_date (DATE)   - Start of analysis window (e.g., '2001-01-01')
--   :p_end_date (DATE)     - End of analysis window (e.g., '2001-12-31')
--   :p_ship_mode (STRING)  - Logistics tier (e.g., 'EXPRESS', 'ALL')
-- -----------------------------------------------------------------------------
SELECT 
    sm.ship_mode_type,
    sm.carrier_name,
    COUNT(s.order_number) AS total_shipments,
    ROUND(AVG(s.shipping_days), 1) AS avg_shipping_days,
    SUM(s.is_on_time) AS on_time_shipments,
    ROUND(SUM(s.is_on_time) / NULLIF(COUNT(s.order_number), 0) * 100, 2) AS actual_on_time_pct,
    sla.target_on_time_pct,
    ROUND((SUM(s.is_on_time) / NULLIF(COUNT(s.order_number), 0) * 100) - sla.target_on_time_pct, 2) AS sla_gap
FROM gold.fct_catalog_sales s
JOIN gold.dim_date d ON s.sold_date_sk = d.date_sk
JOIN gold.dim_ship_mode sm ON s.ship_mode_sk = sm.ship_mode_sk
LEFT JOIN gov.shipping_sla_targets sla ON sm.ship_mode_type = sla.sm_type
WHERE d.calendar_date BETWEEN :p_start_date AND :p_end_date
  AND (:p_ship_mode = 'ALL' OR sm.ship_mode_type = :p_ship_mode)
GROUP BY sm.ship_mode_type, sm.carrier_name, sla.target_on_time_pct
ORDER BY total_shipments DESC;

-- -----------------------------------------------------------------------------
-- Query 3: Basket Size & High-Value Order Analysis (Numeric & Date Parameters)
-- Parameters:
--   :p_min_items (INT) - Minimum threshold of items per order (e.g., 5)
--   :p_target_year (INT) - Target calendar year (e.g., 2001)
-- -----------------------------------------------------------------------------
SELECT 
    call_center_name,
    calendar_year,
    calendar_month,
    total_orders,
    total_units_sold,
    items_per_order,
    total_net_sales,
    ROUND(total_net_sales / NULLIF(total_orders, 0), 2) AS avg_order_value
FROM gold.mv_catalog_sales_kpis
WHERE calendar_year = :p_target_year
  AND items_per_order >= :p_min_items
ORDER BY items_per_order DESC, total_net_sales DESC;
