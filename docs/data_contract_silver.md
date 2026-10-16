# Silver Data Contract & Quality Architecture

## 1. Silver Data Contract Overview

The Silver layer serves as the cleansed, validated, and conformed enterprise store. It transitions raw technical dumps into trustworthy business entities.

### Schema & Naming Rules
1. **Naming**: All uppercase/cryptic source codes (e.g., `cs_net_paid`, `cs_ext_discount_amt`) are renamed to clean snake_case business terms (e.g., `net_paid`, `ext_discount_amt`).
2. **Explicit Casting**: Every field is strictly cast to its canonical schema type (e.g., currency/cost to `DECIMAL(7,2)`, IDs to `LONG`, quantities to `INT`).
3. **No Silent Drops**: Any row failing critical validity rules must not be silently discarded; it is routed to quarantine streaming tables in the `ops` schema.
4. **Primary Key Guarantee**: Exactly one row per business key `(order_number, item_sk)` is permitted in Silver.

---

## 2. Data Quality Expectations Matrix

| Dataset | Expectation Name | SQL Expression | Action | Business Rationale & Justification |
|---|---|---|---|---|
| `silver_catalog_sales` | `valid_keys` | `order_number IS NOT NULL AND item_sk IS NOT NULL` | **DROP ROW** | Core primary composite key. Transactions missing key identity cannot be attributed or joined to dimensions. |
| `silver_catalog_sales` | `valid_metrics` | `quantity > 0 AND net_paid >= 0.0` | **DROP ROW** | Commercial validity. Non-positive order lines or corrupted negative net revenue distort revenue reporting. |
| `silver_catalog_sales` | `non_negative_discount` | `ext_discount_amt >= 0.0` | **KEEP & RECORD** | Observability warning. Extreme or negative discounts require investigation but should not halt the transaction flow. |
| `silver_catalog_returns` | `valid_return_keys` | `order_number IS NOT NULL AND item_sk IS NOT NULL` | **DROP ROW** | Key identity required to link return to original sales line item. |
| `silver_catalog_returns` | `valid_return_metrics` | `return_quantity > 0 AND return_amount >= 0.0` | **DROP ROW** | Return quantities and refund amounts must be positive values. |
| `silver_catalog_returns` | `valid_reason` | `reason_sk IS NOT NULL` | **KEEP & RECORD** | Return reason classification warning; unclassified returns are retained for customer credit reconciliation. |

---

## 3. Quarantine Architecture & Exact Count Reconciliation

Dropped rows are captured in real-time in the `ops` schema:
- `ops.quarantine_catalog_sales`
- `ops.quarantine_catalog_returns`

### Mathematical Reconciliation Proof
Every pipeline run must satisfy:
$$\text{Count}(\text{Bronze}_{\text{raw}}) = \text{Count}(\text{Silver}_{\text{clean}}) + \text{Count}(\text{Ops}_{\text{quarantined}})$$

### Verification Query
```sql
SELECT 
    'catalog_sales' AS dataset,
    (SELECT COUNT(*) FROM meridian_t06.bronze.raw_catalog_sales) AS bronze_count,
    (SELECT COUNT(*) FROM meridian_t06.silver.silver_catalog_sales) AS silver_count,
    (SELECT COUNT(*) FROM meridian_t06.ops.quarantine_catalog_sales) AS quarantine_count,
    ((SELECT COUNT(*) FROM meridian_t06.silver.silver_catalog_sales) + 
     (SELECT COUNT(*) FROM meridian_t06.ops.quarantine_catalog_sales)) AS silver_plus_quarantine,
    CASE 
        WHEN (SELECT COUNT(*) FROM meridian_t06.bronze.raw_catalog_sales) = 
             ((SELECT COUNT(*) FROM meridian_t06.silver.silver_catalog_sales) + 
              (SELECT COUNT(*) FROM meridian_t06.ops.quarantine_catalog_sales))
        THEN 'PASSED_RECONCILED'
        ELSE 'FAILED_DISCREPANCY'
    END AS status;
```
