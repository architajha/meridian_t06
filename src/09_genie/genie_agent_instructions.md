# Genie Agent Configuration: Meridian Direct Assistant

## Overview
- **Agent Name**: Meridian Direct Sales & Contact Centre Assistant
- **Target Audience**: Business Analysts, Contact Centre Directors, Operations Executives
- **Scope**: Served strictly over the Gold layer. Do NOT add Bronze, Silver, or Ops tables.

## Selected Tables
1. `meridian_t06.gold.mv_catalog_sales_kpis` (Governed Metric View - Primary dataset)
2. `meridian_t06.gold.fct_catalog_sales` (Sales order item transactions)
3. `meridian_t06.gold.dim_call_center` (Call centre dimension with SCD2 attributes)
4. `meridian_t06.gold.dim_ship_mode` (Shipping tiers and carriers)
5. `meridian_t06.gold.dim_date` (Calendar dates)

---

## Agent Instructions (System Prompt)

```text
You are the AI Business Assistant for Meridian Direct, the catalog sales and contact centre division of Meridian Holdings.
Your role is to answer business questions accurately using only the governed Gold layer tables and metric views in the catalog 'meridian_t06'.

Key Domain Rules:
1. Business Context:
   - Meridian Direct sells products via mail-order and phone catalogs managed across regional call centres.
   - All transactions are tracked in 'fct_catalog_sales' and conformed dimensions.

2. Metric Definitions:
   - Net Sales: Always use total_net_sales or SUM(net_sales_amount).
   - Return Rate: Must be computed as ratio of sums: SUM(total_returned_amount) / SUM(total_net_sales). Never average percentage columns.
   - Profit Margin: Ratio of sums: SUM(total_net_profit) / SUM(total_net_sales). Note: If masked, net profit may appear as NULL.
   - On-Time Shipping Rate: SUM(total_on_time_shipments) / COUNT(total_shipment_lines).
   - Items Per Order: SUM(total_units_sold) / COUNT(DISTINCT order_number).

3. Code Values:
   - Shipping modes include 'EXPRESS', 'REGULAR', 'NEXT DAY', 'OVERNIGHT', 'TWO DAY', 'STANDARD'.
   - Call centres are identified by call_center_name (e.g., 'Midwest Direct', 'North Central', 'Pacific Heights') and call_center_id.

4. Default Assumptions:
   - When no date or time period is specified by the user, default to calendar year 2001 or the latest available complete year.

5. Out-of-Scope Requests:
   - If a user asks for unmasked credit card details, customer social security numbers, or internal database physical storage details, politely explain that the data is protected or out of scope.
   - Do not attempt to query bronze, silver, or staging tables.

6. Security and Masking:
   - You operate under the requesting user's Unity Catalog permissions. Sensitive fields (cc_manager, cs_net_profit) will be automatically masked by Unity Catalog column masks if the viewer lacks t06_pii_readers access.
```

---

## Example SQL Queries

### Example 1: Sales and Profit Margin by Call Centre
```sql
SELECT 
    call_center_name,
    SUM(total_net_sales) AS total_sales,
    ROUND(SUM(total_net_profit) / NULLIF(SUM(total_net_sales), 0), 4) AS profit_margin
FROM meridian_t06.gold.mv_catalog_sales_kpis
WHERE calendar_year = 2001
GROUP BY call_center_name
ORDER BY total_sales DESC;
```

### Example 2: On-Time Fulfillment by Shipping Carrier
```sql
SELECT 
    carrier_name,
    ship_mode_type,
    SUM(total_on_time_shipments) AS on_time_orders,
    COUNT(*) AS total_shipments,
    ROUND(SUM(total_on_time_shipments) / NULLIF(COUNT(*), 0) * 100, 2) AS on_time_percentage
FROM meridian_t06.gold.mv_catalog_sales_kpis
GROUP BY carrier_name, ship_mode_type
ORDER BY on_time_percentage DESC;
```

### Example 3: Monthly Value Return Rate Trend
```sql
SELECT 
    calendar_month,
    SUM(total_net_sales) AS monthly_sales,
    SUM(total_returned_amount) AS monthly_returns,
    ROUND(SUM(total_returned_amount) / NULLIF(SUM(total_net_sales), 0) * 100, 2) AS return_rate_pct
FROM meridian_t06.gold.mv_catalog_sales_kpis
WHERE calendar_year = 2001
GROUP BY calendar_month
ORDER BY calendar_month ASC;
```

---

## Benchmark Evaluation Suite (Target: >= 4/5 Correct)

| # | Question | Expected Output / Logic | Benchmark SQL |
|---|---|---|---|
| **BM-1** | "What was the total net sales for each call centre in 2001?" | Aggregates `total_net_sales` by `call_center_name` filtered on `calendar_year = 2001`. | `SELECT call_center_name, SUM(total_net_sales) FROM gold.mv_catalog_sales_kpis WHERE calendar_year = 2001 GROUP BY call_center_name;` |
| **BM-2** | "Which shipping mode achieved the highest on-time delivery rate?" | Groups by `ship_mode_type`, calculates `SUM(on_time) / COUNT(*)`, orders DESC, LIMIT 1. | `SELECT ship_mode_type, on_time_shipping_rate FROM gold.mv_catalog_sales_kpis GROUP BY ship_mode_type ORDER BY on_time_shipping_rate DESC LIMIT 1;` |
| **BM-3** | "What is the return rate for Midwest Direct?" | Filters by `call_center_name = 'Midwest Direct'`, computes `SUM(returns) / SUM(sales)`. | `SELECT ROUND(SUM(total_returned_amount) / SUM(total_net_sales), 4) AS return_rate FROM gold.mv_catalog_sales_kpis WHERE call_center_name = 'Midwest Direct';` |
| **BM-4** | "What is the average items per order across all catalog sales?" | Computes ratio of total units over total distinct orders. | `SELECT ROUND(SUM(total_units_sold) / SUM(total_orders), 2) AS avg_items_per_order FROM gold.mv_catalog_sales_kpis;` |
| **BM-5** | "Show monthly sales for call centre AAAAAAAABAAAAAAA in 2001." | Filters by `call_center_id`, groups by `calendar_month`. | `SELECT calendar_month, SUM(total_net_sales) FROM gold.mv_catalog_sales_kpis WHERE call_center_id = 'AAAAAAAABAAAAAAA' AND calendar_year = 2001 GROUP BY calendar_month ORDER BY calendar_month;` |

---

## Test Execution Matrix (Phase 8 Rubric Verification)
1. **KPI Question**: "Show return rate by shipping mode." -> Correct ratio calculation returned.
2. **Follow-up Question**: "Now filter that for only express deliveries." -> Context carried over with `ship_mode_type = 'EXPRESS'`.
3. **Ambiguous Question**: "How is our performance?" -> Genie clarifies or defaults to high-level Net Sales and Profit Margin summary for 2001.
4. **Out-of-Scope Question**: "What are the names and personal phone numbers of our warehouse security guards?" -> Politely declines stating data is outside catalog sales and contact centres scope.
5. **Request for Sensitive Data**: "List all contact centre managers and the exact net profit for order #12345." -> Genie executes query; Unity Catalog column masks redact `cs_net_profit` to `NULL` and partial masks `cc_manager` for standard users.
6. **Restricted Access Test**: Standard user asks: "Show me all sales across the country." -> Row filter limits result to permitted call centres only.
