# Advanced Challenge 1: Native TPC-DS History vs. Pipeline AUTO CDC SCD Type 2

## 1. Executive Summary

Package 06 requires implementing Slowly Changing Dimension Type 2 (SCD Type 2) on the `call_center` dimension and comparing the native TPC-DS history model against the pipeline-managed AUTO CDC model.

Both models achieve temporal historisation, but their architectural mechanisms, temporal resolution, out-of-order handling, and query patterns differ significantly.

---

## 2. Comparative Architecture Matrix

| Architectural Feature | Native TPC-DS Model (`samples.tpcds_sf1.call_center`) | Lakeflow Pipeline AUTO CDC (`silver_call_center_scd2`) |
|---|---|---|
| **Tracking Columns** | `cc_rec_start_date`, `cc_rec_end_date` | `__START_AT`, `__END_AT` (exposed as `valid_from`, `valid_to`) |
| **Data Types** | `DATE` (Day-level grain, e.g., `1998-01-01`) | `TIMESTAMP` (Microsecond precision, e.g., `2024-06-01 10:00:00.000`) |
| **Key Management** | Pre-assigned surrogate keys (`cc_call_center_sk`) per version | Natural key `cc_call_center_id` tracked over time; pipeline generates surrogate version windows |
| **Current Record Marker** | `cc_rec_end_date IS NULL` | `__END_AT IS NULL` (represented as boolean flag `is_current = TRUE`) |
| **Out-of-Order Ingestion** | Manual SQL merge required; vulnerable to duplicate interval collisions | Handled natively by Lakeflow via `sequence_by` sequencing column (`cdc_timestamp`) |
| **Delete Propagation** | Soft deletes require explicit custom ETL logic | Native `apply_as_deletes` parameter closes the active version window automatically |
| **Storage & Optimization** | Static table | Delta Lake with liquid clustering and Z-Order optimization over version keys |

---

## 3. Structural & Reconciliation Analysis

### Finding 1: Temporal Granularity Differences
- In native TPC-DS, `cc_rec_start_date` is a `DATE`. If an organization undergoes two departmental or manager transitions within the same day, the TPC-DS model suffers primary key collision or non-deterministic interval queries.
- In the Lakeflow Declarative Pipeline, `__START_AT` is timestamp-accurate, allowing sub-daily transitions without ambiguity.

### Finding 2: Window Boundary Inclusivity
- **TPC-DS Convention**: Typically closed intervals where `rec_start_date <= event_date AND event_date <= rec_end_date`.
- **Lakeflow AUTO CDC Convention**: Half-open intervals where `valid_from <= event_timestamp AND event_timestamp < valid_to`. This prevents double-counting transactions occurring exactly at midnight on boundary dates.

### Finding 3: Point-in-Time Join Precision
In `fct_catalog_sales`, joining against native TPC-DS uses:
```sql
ON sales.sold_date_sk = dim_date.date_sk
AND dim_date.calendar_date BETWEEN cc.cc_rec_start_date AND COALESCE(cc.cc_rec_end_date, DATE '9999-12-31')
```
In contrast, joining against Lakeflow AUTO CDC uses:
```sql
ON sales.call_center_sk = cc.call_center_sk
AND sales.sale_date >= CAST(cc.valid_from AS DATE)
AND sales.sale_date < COALESCE(CAST(cc.valid_to AS DATE), DATE '9999-12-31')
```

---

## 4. Reconciliation SQL Query

```sql
SELECT 
    n.cc_call_center_id,
    COUNT(n.cc_call_center_sk) AS tpcds_version_count,
    COUNT(p.call_center_sk) AS pipeline_version_count,
    MAX(CASE WHEN n.cc_rec_end_date IS NULL THEN n.cc_manager END) AS tpcds_current_manager,
    MAX(CASE WHEN p.is_current = TRUE THEN p.cc_manager END) AS pipeline_current_manager,
    CASE 
        WHEN COUNT(n.cc_call_center_sk) = COUNT(p.call_center_sk) THEN 'MATCH_VERSION_COUNT'
        ELSE 'MULTI_BATCH_EXPANDED'
    END AS reconciliation_status
FROM samples.tpcds_sf1.call_center n
LEFT JOIN meridian_t06.gold.dim_call_center p 
  ON n.cc_call_center_id = p.call_center_id
GROUP BY n.cc_call_center_id;
```
