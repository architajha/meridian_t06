# Databricks notebook source
import dlt
from pyspark.sql.functions import col

# Silver Cleansed Sales
@dlt.table(name="silver.silver_catalog_sales", comment="Validated catalog sales")
@dlt.expect_or_drop("valid_item", "cs_item_sk IS NOT NULL")
@dlt.expect_or_drop("valid_order", "cs_order_number IS NOT NULL")
def silver_catalog_sales():
    return dlt.read_stream("bronze.bronze_catalog_sales")

# Quarantine Table
@dlt.table(name="silver.quarantine_sales", comment="Quarantined records failing silver quality rules")
def quarantine_sales():
    return (
        dlt.read_stream("bronze.bronze_catalog_sales")
        .filter("cs_item_sk IS NULL OR cs_order_number IS NULL")
    )

# Call Center SCD Type 2 History Tracker
dlt.create_target_table(name="silver.silver_call_center")
dlt.apply_changes(
    target="meridian_t06.silver.silver_call_center",
    source="bronze.bronze_call_center",
    keys=["cc_call_center_sk"],
    sequence_by=col("_sq_rcvd_at"),
    stored_as_scd_type="2"
)