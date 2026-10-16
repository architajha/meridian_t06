# Dimensional Model Architecture: Meridian Direct (Package 06)

## 1. Star Schema Overview

Meridian Direct models catalog sales orders and contact centre operations into an enterprise star schema in the Gold layer of Unity Catalog.

```mermaid
erDiagram
    DIM_DATE ||--o{ FCT_CATALOG_SALES : "sold_date_sk / ship_date_sk"
    DIM_CALL_CENTER ||--o{ FCT_CATALOG_SALES : "call_center_sk (Point-in-Time)"
    DIM_SHIP_MODE ||--o{ FCT_CATALOG_SALES : "ship_mode_sk"
    DIM_CATALOG_PAGE ||--o{ FCT_CATALOG_SALES : "catalog_page_sk"

    DIM_DATE ||--o{ FCT_CATALOG_RETURNS : "returned_date_sk"
    DIM_CALL_CENTER ||--o{ FCT_CATALOG_RETURNS : "call_center_sk"
    DIM_SHIP_MODE ||--o{ FCT_CATALOG_RETURNS : "ship_mode_sk"
    DIM_CATALOG_PAGE ||--o{ FCT_CATALOG_RETURNS : "catalog_page_sk"

    FCT_CATALOG_SALES ||--o| FCT_CATALOG_RETURNS : "order_number + item_sk"

    FCT_CATALOG_SALES {
        bigint order_number PK
        bigint item_sk PK
        bigint sold_date_sk FK
        bigint ship_date_sk FK
        bigint call_center_sk FK
        bigint catalog_page_sk FK
        bigint ship_mode_sk FK
        int quantity
        decimal wholesale_cost
        decimal sales_price
        decimal net_sales_amount
        decimal cs_net_profit "MASKED"
        int is_on_time
    }

    FCT_CATALOG_RETURNS {
        bigint order_number PK
        bigint item_sk PK
        bigint returned_date_sk FK
        bigint call_center_sk FK
        bigint catalog_page_sk FK
        bigint ship_mode_sk FK
        int return_quantity
        decimal returned_amount
        decimal net_loss
    }

    DIM_CALL_CENTER {
        bigint call_center_sk PK
        string call_center_id
        string call_center_name
        string cc_manager "MASKED"
        string call_center_class
        int employees_count
        timestamp valid_from "SCD2"
        timestamp valid_to "SCD2"
        boolean is_current
    }

    DIM_DATE {
        bigint date_sk PK
        date calendar_date
        int calendar_year
        int calendar_month
        int calendar_quarter
        string day_name
    }

    DIM_SHIP_MODE {
        bigint ship_mode_sk PK
        string ship_mode_id
        string ship_mode_type
        string carrier_name
    }

    DIM_CATALOG_PAGE {
        bigint catalog_page_sk PK
        string catalog_page_id
        string department
        int catalog_number
    }
```

---

## 2. Declared Grains & Table Metadata

| Table Name | Layer | Declared Grain | Primary Key | Description |
|---|---|---|---|---|
| `gold.fct_catalog_sales` | Gold Fact | 1 row per order item line | `(order_number, item_sk)` | Records financial and logistical line-item fulfillment for all completed catalog orders. |
| `gold.fct_catalog_returns` | Gold Fact | 1 row per returned item line | `(order_number, item_sk)` | Records return reasons, restock logistics, and refunded financial amounts. |
| `gold.dim_call_center` | Gold Dimension | 1 row per call centre version window | `call_center_sk` (version surrogate) | Slowly Changing Dimension Type 2 tracking organizational changes (promotions, square footage, tier). |
| `gold.dim_date` | Gold Dimension | 1 row per calendar day | `date_sk` | Conformed date dimension providing calendar hierarchies (day, month, quarter, year). |
| `gold.dim_ship_mode` | Gold Dimension | 1 row per shipping mode code | `ship_mode_sk` | Logistics carrier and contract terms defining expected SLA delivery intervals. |
| `gold.dim_catalog_page` | Gold Dimension | 1 row per catalog page | `catalog_page_sk` | Marketing catalog layouts, page numbers, and print department references. |

---

## 3. SCD Type 2 Point-in-Time Join Logic

In classical dimensional modeling, joining a transactional fact to a dimension version must reflect the state of the entity **at the time of the sale**, rather than defaulting to the current version.

In our pipeline:
1. `fct_catalog_sales` contains transaction date `sale_date` (derived from `sold_date_sk`).
2. `dim_call_center` contains valid interval timestamps `valid_from` (`__START_AT`) and `valid_to` (`__END_AT`).
3. Point-in-time join condition:
   ```sql
   SELECT 
       s.order_number,
       s.item_sk,
       s.net_sales_amount,
       cc.call_center_name,
       cc.cc_manager,
       cc.call_center_class
   FROM gold.fct_catalog_sales s
   JOIN gold.dim_call_center cc
     ON s.call_center_sk = cc.call_center_sk
    AND s.sale_date >= CAST(cc.valid_from AS DATE)
    AND s.sale_date < COALESCE(CAST(cc.valid_to AS DATE), DATE '9999-12-31');
   ```
This ensures that historical financial reporting accurately attributes sales to the call centre manager and structure in effect on the order date.
