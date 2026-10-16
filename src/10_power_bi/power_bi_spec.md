# Power BI Integration Specification: Meridian Direct (Package 06)

## 1. Connection Architecture

- **Connector**: Native Databricks Connector in Power BI Desktop
- **Server Hostname**: `<workspace-url>` (e.g., `adb-7405605563443378.18.azuredatabricks.net`)
- **HTTP Path**: `/sql/1.0/warehouses/<warehouse-id>` (Available from SQL Warehouse > Connection details)
- **Catalog**: `meridian_t06`
- **Database/Schema**: `gold`
- **Target Table/View**: `mv_catalog_sales_kpis` and `dim_call_center`
- **Authentication**: Azure Active Directory (OAuth) / User Credentials.
  > [!IMPORTANT]
  > NEVER store personal access tokens or hardcoded passwords in `.pbix` files. All reports must authenticate via user single sign-on (SSO).

---

## 2. Architectural Decision: DirectQuery vs. Import

### Decision: DirectQuery Mode (Recommended for Governed Multi-Tenant Security)

| Criteria | Import Mode | DirectQuery Mode (Selected) |
|---|---|---|
| **Security Enforcement** | ❌ **High Risk**: Ingests all data into a local PBX cache using publisher's credentials. Bypasses Unity Catalog row filters and masks for downstream viewers. | ✅ **Compliant**: Pushes SQL queries down to the warehouse at run-time under the authenticated viewer's identity. Unity Catalog row filters and column masks are strictly enforced. |
| **Warehouse Impact** | Queries warehouse only during scheduled cache refreshes. | Pushes lightweight aggregate queries to warehouse. Because queries target `mv_catalog_sales_kpis` (pre-aggregated metric view), execution is sub-second even on a 2X-Small warehouse. |
| **Data Freshness** | Stale until next scheduled dataset refresh. | Real-time: reflecting the latest Lakeflow pipeline batch runs. |

**Summary Justification**: DirectQuery is mandatory because Project Meridian requires row-level security (regional call centre isolation) and column-level masking to persist for external reporting tools.

---

## 3. Governed DAX Metric Formulations

All DAX measures are strictly defined as ratios of sums, guaranteeing exact mathematical reconciliation with Databricks SQL Metric Views:

```dax
// 1. Total Net Sales
Total Net Sales = SUM('mv_catalog_sales_kpis'[total_net_sales])

// 2. Return Rate (Ratio of Sums)
Return Rate = 
DIVIDE(
    SUM('mv_catalog_sales_kpis'[total_returned_amount]), 
    SUM('mv_catalog_sales_kpis'[total_net_sales]), 
    0.0
)

// 3. Net Profit Margin (Ratio of Sums - Returns BLANK when column mask redacts net profit)
Profit Margin = 
DIVIDE(
    SUM('mv_catalog_sales_kpis'[total_net_profit]), 
    SUM('mv_catalog_sales_kpis'[total_net_sales]), 
    BLANK()
)

// 4. On-Time Shipping Rate (Ratio of Sums)
On-Time Shipping % = 
DIVIDE(
    SUM('mv_catalog_sales_kpis'[total_on_time_shipments]), 
    SUM('mv_catalog_sales_kpis'[total_shipment_lines]), 
    0.0
)

// 5. Items Per Order (Basket Depth)
Items Per Order = 
DIVIDE(
    SUM('mv_catalog_sales_kpis'[total_units_sold]), 
    SUM('mv_catalog_sales_kpis'[total_orders]), 
    0.0
)
```

---

## 4. Single-Page Report Layout

1. **Header & Context**:
   - Title: "Meridian Direct: Executive Performance & Fulfillment"
   - Date Slicer: `calendar_year`, `calendar_month`
2. **Top Row: 4 KPI Cards**:
   - Card 1: `[Total Net Sales]` ($ format)
   - Card 2: `[Return Rate]` (% format)
   - Card 3: `[Profit Margin]` (% format)
   - Card 4: `[Items Per Order]` (1 decimal format)
3. **Middle Section: Visualizations**:
   - Left Chart (Clustered Column): Monthly Sales Trend (`total_net_sales` by `calendar_month`) with line overlay for `[Return Rate]`.
   - Right Chart (Bar Chart): Net Sales by Call Centre (`call_center_name`), colored by On-Time Shipping SLA compliance.
4. **Bottom Section**:
   - Detailed Table: `call_center_name`, `carrier_name`, `ship_mode_type`, `Total Net Sales`, `Return Rate`, `On-Time Shipping %`.

---

## 5. Reconciliation & Security Verification Check

- **Reconciliation**: When filtered on Year `2001`, Total Net Sales and Return Rate in Power BI match the values on the AI/BI Dashboard within $0.01 tolerance.
- **Restricted Refresh Check**:
  - When refreshed by user in `t06_admins`: Sees all call centres and unmasked profit figures.
  - When refreshed by user assigned to `manager_north@meridian.com`: Power BI dataset filters to only call centre `AAAAAAAABAAAAAAA`, and Profit Margin card displays blank/zero due to active column masking.
