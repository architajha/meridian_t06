# Unity Catalog Security & Governance Model

## 1. Security Entitlements & Access Matrix

Project Meridian enforces least-privilege access, cell-level security (row filters), and column-level masking across all reporting interfaces.

| Persona / Role | Databricks Group | Row-Level Access (Call Centres) | Column: `cc_manager` (Name) | Column: `cs_net_profit` (Financial) | Schema Grants |
|---|---|---|---|---|---|
| **Platform Administrator** | `t06_admins` | **All Rows** (Unrestricted) | **Unmasked** (Full name visible) | **Unmasked** (Full profit visible) | Full admin across all schemas |
| **Executive / Financial Auditor** | `t06_pii_readers` | Governed by Entitlement Table | **Unmasked** | **Unmasked** | `SELECT` on `gold` |
| **Regional Contact Centre Manager** | Standard User | **Restricted**: Only authorized call centres in `gov.call_center_entitlements` | **Masked**: Partial redaction (`A*****n`) | **Masked**: Full redaction (`NULL`) | `SELECT` on `gold` |
| **Business Analyst / BI Viewer** | `account users` | **Restricted**: Mapped call centres only | **Masked**: Partial redaction | **Masked**: Full redaction | `SELECT` on `gold` only |

---

## 2. Governed Tag Taxonomy

Classification vocabulary configured across catalog objects:

1. **`medallion_layer`**:
   - Allowed values: `landing`, `bronze`, `silver`, `gold`, `gov`, `ops`.
   - Applied to schemas and base tables.
2. **`sensitivity_level`**:
   - Allowed values: `public`, `internal`, `confidential`, `restricted`.
   - Applied to schemas and sensitive data assets.
3. **`pii_type`**:
   - Allowed values: `none`, `name`, `financial`, `identifier`.
   - Applied directly to columns requiring security masks.

---

## 3. Dynamic Row-Level Security Architecture

Row filters are implemented as SQL User-Defined Functions (UDFs) referencing a centralized mapping table in `gov.call_center_entitlements`:

```sql
CREATE OR REPLACE FUNCTION gov.fn_call_center_row_filter(cc_id STRING)
RETURNS BOOLEAN
RETURN
    IS_ACCOUNT_GROUP_MEMBER('t06_admins')
    OR EXISTS (
        SELECT 1 
        FROM gov.call_center_entitlements e
        WHERE e.user_email = CURRENT_USER()
          AND e.call_center_id = cc_id
    );
```

### Advantages of the Dynamic Mapping Table:
- **Zero Hardcoding**: No hardcoded email addresses in SQL logic.
- **Hierarchical & Multi-Centre Support**: A single regional manager can be assigned to 1, 5, or 10 centres by simply inserting rows into `gov.call_center_entitlements`.
- **Immediate Propagation**: Granting or revoking an entitlement takes effect instantly without redeploying code or modifying table DDL.

---

## 4. Column Masking Specification

### Mask 1: Contact Centre Manager Name (`cc_manager`)
- **Type**: Partial redaction (Preserves first character, obscures middle characters with asterisks, preserves last character).
- **Function**: `gov.mask_manager_name(val STRING)`.
- **Behavior**: Non-privileged users see `J*****n` instead of `Johnathan`.

### Mask 2: Net Profit (`cs_net_profit`)
- **Type**: Full cryptographic suppression (`NULL`).
- **Function**: `gov.mask_net_profit(val DECIMAL(7,2))`.
- **Behavior**: Non-privileged users receive `NULL`, protecting competitive trade margins while still allowing aggregation of top-line revenue.
