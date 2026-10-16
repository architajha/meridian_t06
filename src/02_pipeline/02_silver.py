# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Silver Conformed & Cleansed Layer
# Features:
# 1. Renames columns to standardized business naming and casts types
# 2. Enforces data quality rules via expectations (keep/record vs drop)
# 3. Routes rejected records to quarantine streaming tables for 100% reconciliation
# 4. Enforces uniqueness (one row per business key)
# 5. Lakeflow AUTO CDC SCD Type 2 on call_center dimension
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Silver Fact: catalog_sales (Cleansed & Deduplicated)
# Expectations:
# - valid_keys: Drops rows with null order_number or item_sk
# - valid_metrics: Drops rows with non-positive quantity or negative net_paid
# - non_negative_discount: Keep and record warning for unexpected discounts
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_catalog_sales",
    comment="Cleaned, conformed catalog sales transactions with enforced business expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_keys", "order_number IS NOT NULL AND item_sk IS NOT NULL")
@dlt.expect_or_drop("valid_metrics", "quantity > 0 AND net_paid >= 0.0")
@dlt.expect("non_negative_discount", "ext_discount_amt >= 0.0")
def silver_catalog_sales():
    raw_sales = dlt.read_stream("raw_catalog_sales")
    
    transformed = (
        raw_sales
        .select(
            F.col("cs_sold_date_sk").cast("long").alias("sold_date_sk"),
            F.col("cs_ship_date_sk").cast("long").alias("ship_date_sk"),
            F.col("cs_bill_customer_sk").cast("long").alias("bill_customer_sk"),
            F.col("cs_call_center_sk").cast("long").alias("call_center_sk"),
            F.col("cs_catalog_page_sk").cast("long").alias("catalog_page_sk"),
            F.col("cs_ship_mode_sk").cast("long").alias("ship_mode_sk"),
            F.col("cs_item_sk").cast("long").alias("item_sk"),
            F.col("cs_order_number").cast("long").alias("order_number"),
            F.col("cs_quantity").cast("int").alias("quantity"),
            F.col("cs_wholesale_cost").cast("decimal(7,2)").alias("wholesale_cost"),
            F.col("cs_list_price").cast("decimal(7,2)").alias("list_price"),
            F.col("cs_sales_price").cast("decimal(7,2)").alias("sales_price"),
            F.col("cs_ext_discount_amt").cast("decimal(7,2)").alias("ext_discount_amt"),
            F.col("cs_ext_sales_price").cast("decimal(7,2)").alias("ext_sales_price"),
            F.col("cs_ext_ship_cost").cast("decimal(7,2)").alias("ext_ship_cost"),
            F.col("cs_net_paid").cast("decimal(7,2)").alias("net_paid"),
            F.col("cs_net_profit").cast("decimal(7,2)").alias("net_profit"),
            F.col("_source_file_path"),
            F.col("_ingest_timestamp"),
            F.col("_batch_id")
        )
    )
    # Ensure deduplication on business key: (order_number, item_sk)
    return transformed.dropDuplicates(["order_number", "item_sk"])

# -----------------------------------------------------------------------------
# Ops Quarantine: quarantine_catalog_sales
# Captures dropped rows that fail valid_keys or valid_metrics for full reconciliation
# -----------------------------------------------------------------------------
@dlt.table(
    name="quarantine_catalog_sales",
    comment="Quarantined records from raw_catalog_sales failing Silver expectations",
    table_properties={"quality": "ops"}
)
def quarantine_catalog_sales():
    raw_sales = dlt.read_stream("raw_catalog_sales")
    return (
        raw_sales
        .filter(
            (F.col("cs_order_number").isNull()) |
            (F.col("cs_item_sk").isNull()) |
            (F.col("cs_quantity") <= 0) |
            (F.col("cs_net_paid") < 0.0)
        )
        .select(
            F.col("cs_order_number").alias("order_number"),
            F.col("cs_item_sk").alias("item_sk"),
            F.col("cs_quantity").alias("quantity"),
            F.col("cs_net_paid").alias("net_paid"),
            F.when(F.col("cs_order_number").isNull() | F.col("cs_item_sk").isNull(), "NULL_PRIMARY_KEY")
             .when(F.col("cs_quantity") <= 0, "INVALID_QUANTITY")
             .when(F.col("cs_net_paid") < 0.0, "NEGATIVE_NET_PAID")
             .otherwise("OTHER_VIOLATION").alias("quarantine_reason"),
            F.col("_source_file_path"),
            F.col("_batch_id"),
            F.current_timestamp().alias("quarantine_time")
        )
    )

# -----------------------------------------------------------------------------
# Silver Fact: catalog_returns (Cleansed & Deduplicated)
# Expectations:
# - valid_return_keys: Drops rows with null order_number or item_sk
# - valid_return_metrics: Drops rows with negative returned quantity or amount
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_catalog_returns",
    comment="Cleaned, conformed catalog return transactions with enforced quality expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_return_keys", "order_number IS NOT NULL AND item_sk IS NOT NULL")
@dlt.expect_or_drop("valid_return_metrics", "return_quantity > 0 AND return_amount >= 0.0")
@dlt.expect("valid_reason", "reason_sk IS NOT NULL")
def silver_catalog_returns():
    raw_returns = dlt.read_stream("raw_catalog_returns")
    
    transformed = (
        raw_returns
        .select(
            F.col("cr_returned_date_sk").cast("long").alias("returned_date_sk"),
            F.col("cr_item_sk").cast("long").alias("item_sk"),
            F.col("cr_refunded_customer_sk").cast("long").alias("refunded_customer_sk"),
            F.col("cr_call_center_sk").cast("long").alias("call_center_sk"),
            F.col("cr_catalog_page_sk").cast("long").alias("catalog_page_sk"),
            F.col("cr_ship_mode_sk").cast("long").alias("ship_mode_sk"),
            F.col("cr_reason_sk").cast("long").alias("reason_sk"),
            F.col("cr_order_number").cast("long").alias("order_number"),
            F.col("cr_return_quantity").cast("int").alias("return_quantity"),
            F.col("cr_return_amount").cast("decimal(7,2)").alias("return_amount"),
            F.col("cr_return_tax").cast("decimal(7,2)").alias("return_tax"),
            F.col("cr_return_amt_inc_tax").cast("decimal(7,2)").alias("return_amt_inc_tax"),
            F.col("cr_net_loss").cast("decimal(7,2)").alias("net_loss"),
            F.col("_source_file_path"),
            F.col("_ingest_timestamp"),
            F.col("_batch_id")
        )
    )
    return transformed.dropDuplicates(["order_number", "item_sk"])

# -----------------------------------------------------------------------------
# Ops Quarantine: quarantine_catalog_returns
# -----------------------------------------------------------------------------
@dlt.table(
    name="quarantine_catalog_returns",
    comment="Quarantined records from raw_catalog_returns failing Silver expectations",
    table_properties={"quality": "ops"}
)
def quarantine_catalog_returns():
    raw_returns = dlt.read_stream("raw_catalog_returns")
    return (
        raw_returns
        .filter(
            (F.col("cr_order_number").isNull()) |
            (F.col("cr_item_sk").isNull()) |
            (F.col("cr_return_quantity") <= 0) |
            (F.col("cr_return_amount") < 0.0)
        )
        .select(
            F.col("cr_order_number").alias("order_number"),
            F.col("cr_item_sk").alias("item_sk"),
            F.col("cr_return_quantity").alias("return_quantity"),
            F.col("cr_return_amount").alias("return_amount"),
            F.when(F.col("cr_order_number").isNull() | F.col("cr_item_sk").isNull(), "NULL_PRIMARY_KEY")
             .when(F.col("cr_return_quantity") <= 0, "INVALID_RETURN_QUANTITY")
             .when(F.col("cr_return_amount") < 0.0, "NEGATIVE_RETURN_AMOUNT")
             .otherwise("OTHER_VIOLATION").alias("quarantine_reason"),
            F.col("_source_file_path"),
            F.col("_batch_id"),
            F.current_timestamp().alias("quarantine_time")
        )
    )

# -----------------------------------------------------------------------------
# Silver Conformed Dimension: call_center SCD Type 2 with AUTO CDC (apply_changes)
# Tracks history over multi-batch changes with __START_AT and __END_AT
# -----------------------------------------------------------------------------
dlt.create_streaming_table(
    name="silver_call_center_scd2",
    comment="Slowly Changing Dimension Type 2 for call_center using Lakeflow AUTO CDC",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_call_center_scd2",
    source="raw_call_center",
    keys=["cc_call_center_id"],
    sequence_by=F.col("cdc_timestamp"),
    apply_as_deletes=F.col("cdc_op") == "DELETE",
    except_column_list=["_source_file_path", "_ingest_timestamp", "_batch_id", "cdc_op"],
    stored_as_scd_type="2"
)

# -----------------------------------------------------------------------------
# Silver Conformed Dimension: catalog_page
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_catalog_page",
    comment="Conformed catalog page dimension",
    table_properties={"quality": "silver"}
)
def silver_catalog_page():
    return (
        dlt.read("raw_catalog_page")
        .select(
            F.col("cp_catalog_page_sk").cast("long").alias("catalog_page_sk"),
            F.col("cp_catalog_page_id").cast("string").alias("catalog_page_id"),
            F.col("cp_department").cast("string").alias("department"),
            F.col("cp_catalog_number").cast("int").alias("catalog_number"),
            F.col("cp_catalog_page_number").cast("int").alias("catalog_page_number"),
            F.col("cp_description").cast("string").alias("page_description"),
            F.col("cp_type").cast("string").alias("page_type")
        )
        .dropDuplicates(["catalog_page_sk"])
    )

# -----------------------------------------------------------------------------
# Silver Conformed Dimension: ship_mode
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_ship_mode",
    comment="Conformed shipping mode dimension",
    table_properties={"quality": "silver"}
)
def silver_ship_mode():
    return (
        dlt.read("raw_ship_mode")
        .select(
            F.col("sm_ship_mode_sk").cast("long").alias("ship_mode_sk"),
            F.col("sm_ship_mode_id").cast("string").alias("ship_mode_id"),
            F.col("sm_type").cast("string").alias("ship_mode_type"),
            F.col("sm_code").cast("string").alias("ship_mode_code"),
            F.col("sm_carrier").cast("string").alias("carrier_name"),
            F.col("sm_contract").cast("string").alias("contract_id")
        )
        .dropDuplicates(["ship_mode_sk"])
    )

# -----------------------------------------------------------------------------
# Silver Conformed Dimension: date_dim
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_date_dim",
    comment="Conformed calendar date dimension",
    table_properties={"quality": "silver"}
)
def silver_date_dim():
    return (
        dlt.read("raw_date_dim")
        .select(
            F.col("d_date_sk").cast("long").alias("date_sk"),
            F.col("d_date_id").cast("string").alias("date_id"),
            F.col("d_date").cast("date").alias("calendar_date"),
            F.col("d_year").cast("int").alias("calendar_year"),
            F.col("d_moy").cast("int").alias("calendar_month"),
            F.col("d_dom").cast("int").alias("calendar_day"),
            F.col("d_qoy").cast("int").alias("calendar_quarter"),
            F.col("d_day_name").cast("string").alias("day_name"),
            F.col("d_quarter_name").cast("string").alias("quarter_name"),
            F.col("d_weekend").cast("string").alias("is_weekend")
        )
        .dropDuplicates(["date_sk"])
    )
