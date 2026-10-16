# Project Meridian: Governed Lakehouse on Databricks Free Edition
## Package 06: Meridian Direct (Catalog Sales & Contact Centres)
**Trainee ID**: `06` | **Catalog**: `meridian_t06` | **Theme**: Two History Models Compared

[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Free%20Edition-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-blue)](docs/star_schema_architecture.md)
[![Bundle](https://img.shields.io/badge/Deployment-Declarative%20Automation%20Bundle-green)](databricks.yml)
[![Governance](https://img.shields.io/badge/Unity%20Catalog-Governed%20Tags%20%7C%20RLS%20%7C%20CLS-purple)](docs/security_model.md)

---

## 1. Executive Summary & Problem Statement

Meridian Direct, the catalog sales and contact centre business unit of Meridian Holdings, previously suffered from siloed reporting, unmasked PII, uncoordinated manual file ingestion, and conflicting KPI definitions. 

This repository delivers an end-to-end, fully governed lakehouse on **Databricks Free Edition** utilizing:
1. **Medallion Architecture**: Append-only Bronze with technical lineage, validated Silver with data quality expectations & quarantine, and a Gold Star Schema.
2. **Lakeflow Declarative Pipelines**: Ingesting files incrementally from Unity Catalog Managed Volumes via Auto Loader, managing SCD Type 2 changes automatically.
3. **Unity Catalog Metric Views**: A single governed source of truth defining all 5 mandatory KPIs as ratios of sums.
4. **Cell-Level Security**: Dynamic row filters referencing an entitlement mapping table in the `gov` schema, and column masks partially redacting manager names and fully redacting net profit.
5. **Declarative Automation Bundle (DAB)**: One-command deployment managing jobs, pipelines, schemas, and quality gates as code.

---

## 2. Data Profile & Design Note

### Source Data Profile (`samples.tpcds_sf1`)
- **Facts**:
  - `catalog_sales`: ~1,440,000 rows. Key grain: `(cs_order_number, cs_item_sk)`. Sliced by date for multi-batch simulation.
  - `catalog_returns`: ~144,000 rows. Key grain: `(cr_order_number, cr_item_sk)`. Sliced by date for multi-batch simulation.
- **Dimensions**:
  - `call_center`: 6 call centres. Native columns `cc_rec_start_date` and `cc_rec_end_date`.
  - `catalog_page`: 12,000 catalog pages.
  - `ship_mode`: 20 shipping mode tiers.
  - `date_dim`: 73,049 calendar days spanning historical centuries.

### Design Decisions & Entity Grain
- **Fact Grain**: `gold.fct_catalog_sales` and `gold.fct_catalog_returns` are maintained at 1 row per order item line (`order_number`, `item_sk`).
- **Row-Filter Dimension**: `call_center_sk` on `fct_catalog_sales` and `call_center_id` on `dim_call_center`.
- **Sensitive Columns**:
  - `dim_call_center.cc_manager`: Classified as `pii_type='name'`, masked with partial redaction (`J*****n`).
  - `fct_catalog_sales.cs_net_profit`: Classified as `pii_type='financial'`, masked with full redaction (`NULL`).
- **Point-in-Time Joins**: Facts join to the specific `dim_call_center` SCD Type 2 record valid during the order's sale date, preventing modern attribution skew.

---

## 3. Governed KPI Formulations

All ratio KPIs are strictly formulated as **ratios of sums** to prevent the mathematical distortion of averaging percentages across aggregations:

| KPI Name | Formula | Measure SQL Expression | Business Justification |
|---|---|---|---|
| **1. Sales per call centre** | $\sum \text{net\_sales\_amount}$ | `SUM(net_sales_amount)` | Measures gross commercial revenue captured per regional contact center. |
| **2. Return rate** | $\frac{\sum \text{returned\_amount}}{\sum \text{net\_sales\_amount}}$ | `SUM(returned_amount) / NULLIF(SUM(net_sales_amount), 0)` | Strict ratio of sums evaluating value return burden against sales. |
| **3. Profit margin** | $\frac{\sum \text{net\_profit}}{\sum \text{net\_sales\_amount}}$ | `SUM(cs_net_profit) / NULLIF(SUM(net_sales_amount), 0)` | Commercial profitability margin. Fully redacted for non-financial viewers. |
| **4. On-time shipping by ship mode** | $\frac{\sum \text{is\_on\_time}}{\sum \text{total\_orders}}$ | `SUM(is_on_time) / NULLIF(COUNT(*), 0)` | Operational logistics compliance evaluated against parameter SLA targets. |
| **5. Items per order** | $\frac{\sum \text{quantity}}{\text{count(distinct order\_number)}}$ | `SUM(quantity) / NULLIF(COUNT(DISTINCT order_number), 0)` | Average basket depth across completed catalog orders. |

---

## 4. Repository Structure

```text
meridian_t06/
├── databricks.yml                      # Declarative Automation Bundle (DAB) definition
├── resources/
│   ├── pipeline.yml                    # Lakeflow Spark Declarative Pipeline definition
│   └── jobs.yml                        # Lakeflow Job DAG configuration
├── src/
│   ├── 00_setup/
│   │   ├── 01_initial_setup.sql        # Catalog, Schemas, Volume, Groups setup
│   │   └── 02_entitlements_and_sla.sql # Entitlements mapping, shipping SLA target parameter tables
│   ├── 01_landing/
│   │   └── landing_simulator.py        # Landing simulator: batches 1, 2, 3 with CDC updates & injected anomalies
│   ├── 02_pipeline/
│   │   ├── 01_bronze.py                # Bronze streaming tables with lineage metadata (_source_file, _ingest_time, _batch_id)
│   │   ├── 02_silver.py                # Silver conformed tables, expectations, quarantine, SCD Type 2 CDC
│   │   └── 03_gold.py                  # Gold star schema models and point-in-time joins
│   ├── 03_vdp/
│   │   └── vdp_monthly_returns.py      # Visual Data Prep (rolling 3-month return rate per call centre)
│   ├── 04_metrics/
│   │   ├── metric_views.sql            # Unity Catalog Metric Views with YAML definition
│   │   └── reconciliation_queries.sql  # Metric view vs fact table verification queries & SCD Type 2 reconciliation
│   ├── 05_governance/
│   │   ├── governed_tags.sql           # Tag policy (medallion_layer, sensitivity_level, pii_type)
│   │   ├── row_filters.sql             # Dynamic row-level security UDF & policy for call centre mapping
│   │   ├── column_masks.sql            # Column masks for cc_manager (partial) and cs_net_profit (redacted)
│   │   └── governance_audit.sql        # Information schema audit query verifying tag-to-mask enforcement
│   ├── 06_quality_gates/
│   │   └── quality_gate.py             # Data quality gate script for the job DAG (ops.data_quality_gates)
│   ├── 07_sql_editor/
│   │   └── parameterized_queries.sql   # Reusable typed parameterized queries (text, date, number)
│   ├── 08_dashboards/
│   │   ├── business_kpi_dashboard.json # AI/BI Dashboard definition & query specifications
│   │   └── operations_dashboard.sql    # Operations telemetry dashboard (system tables & ops tables fallback)
│   ├── 09_genie/
│   │   └── genie_agent_instructions.md # Business context, benchmark questions (5), sample SQL, test scenarios
│   └── 10_power_bi/
│       └── power_bi_spec.md            # Power BI direct query vs import guide, DAX formulas, security propagation
├── docs/
│   ├── star_schema_architecture.md     # Star schema diagrams & data grain definitions
│   ├── data_contract_silver.md         # Silver data contract & quarantine reconciliation
│   ├── security_model.md               # Entitlements, row filters, column masks, and tags
│   └── scd_type_2_comparison.md        # Advanced Challenge 1: Native TPC-DS vs Pipeline SCD2 reconciliation
├── .gitignore
└── README.md
```

---

## 5. End-to-End Phase Walkthrough

### Phase 0: Explore & Plan
- Created catalog `meridian_t06`, schemas `landing`, `bronze`, `silver`, `gold`, `gov`, `ops`, and volume `landing.raw_files`.
- Created groups `t06_admins` and `t06_pii_readers`.

### Phase 1: Landing and Bronze
- Built `landing_simulator.py` writing partitioned Parquet batches to `/Volumes/meridian_t06/landing/raw_files/`.
- Sliced facts into 3 disjoint date windows. Injected controlled anomalies (null keys, duplicates, negative prices).
- Ingested files via Auto Loader streaming tables in `01_bronze.py`, appending `_source_file_path`, `_ingest_timestamp`, and `_batch_id`.

### Phase 2: Silver & Visual Data Prep
- Enforced data quality rules via `@dlt.expect_or_drop` for invalid keys and negative sales.
- Captured dropped records into `ops.quarantine_catalog_sales` and `ops.quarantine_catalog_returns`, proving 100% count reconciliation:
  $$\text{Count}(\text{Bronze}) = \text{Count}(\text{Silver}) + \text{Count}(\text{Quarantine})$$
- Applied Lakeflow `dlt.apply_changes` for `call_center` SCD Type 2 tracking with `__START_AT` and `__END_AT`.
- Visual Data Prep script (`vdp_monthly_returns.py`) executes 4 operators (Filter, Join, Aggregate, Window) to compute rolling 3-month return rates.

### Phase 3: Gold & Metric Views
- Implemented dimensional star schema in `03_gold.py` with point-in-time join semantics.
- Created `gold.mv_catalog_sales_kpis` defining all 5 KPIs as ratios of sums.
- Validated exact mathematical reconciliation between the metric view and direct fact table queries in `reconciliation_queries.sql`.

### Phase 4: Governance (Tags, Row Filters, Column Masks)
- Applied tag policy: `medallion_layer`, `sensitivity_level`, `pii_type`.
- Enforced cell-level security with `gov.fn_call_center_row_filter` backed by `gov.call_center_entitlements`.
- Enforced column masks with `gov.mask_manager_name` (partial masking) and `gov.mask_net_profit` (full redaction).
- Verified governance compliance via `system.information_schema` audit query in `governance_audit.sql`.

### Phase 5: Orchestration with Bundles & Jobs
- Configured Declarative Automation Bundle in `databricks.yml` with `dev` (paused) and `prod` targets.
- Built multi-task Lakeflow Job DAG in `resources/jobs.yml`.
- Created automated quality gate in `quality_gate.py` that checks non-empty Gold, primary key uniqueness, referential integrity, and failure injection testing.

### Phase 6 & 7: BI & Operations Telemetry
- Implemented 3 typed parameterized queries in `parameterized_queries.sql`.
- Built AI/BI Dashboard specification in `business_kpi_dashboard.json`, published as "Run as viewer" to enforce security.
- Developed platform telemetry dashboard in `operations_dashboard.sql` tracking pipeline event logs, DBU consumption, and quality gate history.

### Phase 8 & 9: Genie Agent & Power BI
- Documented Genie Agent in `genie_agent_instructions.md` with 5 verified benchmarks and system instructions.
- Designed Power BI integration in `power_bi_spec.md` justifying DirectQuery mode for runtime security propagation and reconciled DAX measures.

---

## 6. Official Databricks Documentation Cited

All designs strictly conform to official Databricks documentation:
1. **Lakeflow Declarative Pipelines & CDC**: [Databricks Lakeflow Spark Declarative Pipelines Documentation](https://docs.databricks.com/en/delta-live-tables/cdc.html)
2. **Auto Loader with Volumes**: [Auto Loader CloudFiles on Unity Catalog Volumes](https://docs.databricks.com/en/ingestion/auto-loader/unity-catalog.html)
3. **Unity Catalog Row Filters & Column Masks**: [Row Filters and Column Masks Guide](https://docs.databricks.com/en/tables/row-and-column-filters.html)
4. **Unity Catalog Metric Views**: [Governed Metrics and Metric Views in Unity Catalog](https://docs.databricks.com/en/metric-views/index.html)
5. **Declarative Automation Bundles (DABs)**: [Databricks Asset Bundles Configuration Reference](https://docs.databricks.com/en/dev-tools/bundles/settings.html)
6. **System Tables Overview**: [Databricks System Tables Reference](https://docs.databricks.com/en/admin/system-tables/index.html)
7. **Free Edition Platform Limitations**: [Databricks Free Edition Serverless Limits and Quotas](https://docs.databricks.com/en/getting-started/free-edition-limits.html)
