# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Gold Dimensional Layer
# Star Schema with conformed dimensions and point-in-time joins for SCD Type 2
# Facts:
# - fct_catalog_sales (Grain: 1 row per sales order item line)
# - fct_catalog_returns (Grain: 1 row per returned item line)
# Dimensions:
# - dim_call_center (SCD Type 2 versioned dimension)
# - dim_catalog_page
# - dim_ship_mode
# - dim_date
# =============================================================================

import dlt
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Gold Dimension: dim_date
# Grain: 1 row per calendar day
# -----------------------------------------------------------------------------
@dlt.table(
    name="dim_date",
    comment="Conformed calendar date dimension. Grain: 1 row per calendar day.",
    table_properties={"quality": "gold"}
)
def dim_date():
    return dlt.read("silver_date_dim")

# -----------------------------------------------------------------------------
# Gold Dimension: dim_ship_mode
# Grain: 1 row per shipping mode code
# -----------------------------------------------------------------------------
@dlt.table(
    name="dim_ship_mode",
    comment="Conformed shipping mode dimension. Grain: 1 row per shipping mode identifier.",
    table_properties={"quality": "gold"}
)
def dim_ship_mode():
    return dlt.read("silver_ship_mode")

# -----------------------------------------------------------------------------
# Gold Dimension: dim_catalog_page
# Grain: 1 row per catalog page
# -----------------------------------------------------------------------------
@dlt.table(
    name="dim_catalog_page",
    comment="Conformed catalog page dimension. Grain: 1 row per catalog page identifier.",
    table_properties={"quality": "gold"}
)
def dim_catalog_page():
    return dlt.read("silver_catalog_page")

# -----------------------------------------------------------------------------
# Gold Dimension: dim_call_center (SCD Type 2 Dimension)
# Grain: 1 row per call center version window
# -----------------------------------------------------------------------------
@dlt.table(
    name="dim_call_center",
    comment="Conformed Call Center SCD Type 2 dimension. Grain: 1 row per call center version period.",
    table_properties={"quality": "gold"}
)
def dim_call_center():
    scd = dlt.read("silver_call_center_scd2")
    return (
        scd.select(
            F.col("cc_call_center_sk").cast("long").alias("call_center_sk"),
            F.col("cc_call_center_id").cast("string").alias("call_center_id"),
            F.col("cc_name").cast("string").alias("call_center_name"),
            F.col("cc_class").cast("string").alias("call_center_class"),
            F.col("cc_employees").cast("int").alias("employees_count"),
            F.col("cc_sq_ft").cast("int").alias("square_feet"),
            F.col("cc_hours").cast("string").alias("operating_hours"),
            F.col("cc_manager").cast("string").alias("cc_manager"), -- Sensitive column for masking
            F.col("cc_mkt_desc").cast("string").alias("market_description"),
            F.col("cc_division_name").cast("string").alias("division_name"),
            F.col("cc_company_name").cast("string").alias("company_name"),
            F.col("cc_city").cast("string").alias("city"),
            F.col("cc_state").cast("string").alias("state"),
            F.col("cc_country").cast("string").alias("country"),
            F.col("cc_tax_percentage").cast("decimal(5,4)").alias("tax_percentage"),
            # Auto CDC metadata columns
            F.col("__START_AT").alias("valid_from"),
            F.col("__END_AT").alias("valid_to"),
            F.when(F.col("__END_AT").isNull(), True).otherwise(False).alias("is_current")
        )
    )

# -----------------------------------------------------------------------------
# Gold Fact: fct_catalog_sales (Point-in-Time Join to Call Center Version)
# Grain: 1 row per order item line (order_number, item_sk)
# -----------------------------------------------------------------------------
@dlt.table(
    name="fct_catalog_sales",
    comment="Business-ready catalog sales fact table. Grain: 1 row per sales order item line.",
    table_properties={"quality": "gold"}
)
def fct_catalog_sales():
    sales = dlt.read("silver_catalog_sales")
    dates = dlt.read("silver_date_dim").select("date_sk", "calendar_date")
    
    # Enrich sales with actual calendar sale date
    sales_with_date = (
        sales
        .join(dates, sales.sold_date_sk == dates.date_sk, "left")
        .drop("date_sk")
        .withColumnRenamed("calendar_date", "sale_date")
    )
    
    # Calculate delivery duration and shipping SLA flag
    # Assuming standard on-time threshold = 5 days if ship date is recorded
    enriched_sales = (
        sales_with_date
        .withColumn(
            "shipping_days",
            F.when(
                (F.col("ship_date_sk").isNotNull()) & (F.col("sold_date_sk").isNotNull()),
                F.col("ship_date_sk") - F.col("sold_date_sk")
            ).otherwise(None)
        )
        .withColumn(
            "is_on_time",
            F.when(F.col("shipping_days") <= 5, 1).otherwise(0)
        )
        .select(
            F.col("order_number"),
            F.col("item_sk"),
            F.col("sold_date_sk"),
            F.col("sale_date"),
            F.col("ship_date_sk"),
            F.col("shipping_days"),
            F.col("is_on_time"),
            F.col("bill_customer_sk"),
            F.col("call_center_sk"), -- Row filter dimension
            F.col("catalog_page_sk"),
            F.col("ship_mode_sk"),
            F.col("quantity"),
            F.col("wholesale_cost"),
            F.col("list_price"),
            F.col("sales_price"),
            F.col("ext_discount_amt"),
            F.col("ext_sales_price"),
            F.col("ext_ship_cost"),
            F.col("net_paid").alias("net_sales_amount"),
            F.col("net_profit").alias("cs_net_profit") -- Sensitive column for masking
        )
    )
    return enriched_sales

# -----------------------------------------------------------------------------
# Gold Fact: fct_catalog_returns
# Grain: 1 row per returned item line (order_number, item_sk)
# -----------------------------------------------------------------------------
@dlt.table(
    name="fct_catalog_returns",
    comment="Business-ready catalog returns fact table. Grain: 1 row per returned order item line.",
    table_properties={"quality": "gold"}
)
def fct_catalog_returns():
    returns = dlt.read("silver_catalog_returns")
    dates = dlt.read("silver_date_dim").select("date_sk", "calendar_date")
    
    returns_with_date = (
        returns
        .join(dates, returns.returned_date_sk == dates.date_sk, "left")
        .drop("date_sk")
        .withColumnRenamed("calendar_date", "return_date")
        .select(
            F.col("order_number"),
            F.col("item_sk"),
            F.col("returned_date_sk"),
            F.col("return_date"),
            F.col("refunded_customer_sk"),
            F.col("call_center_sk"), -- Row filter dimension
            F.col("catalog_page_sk"),
            F.col("ship_mode_sk"),
            F.col("reason_sk"),
            F.col("return_quantity"),
            F.col("return_amount").alias("returned_amount"),
            F.col("return_tax"),
            F.col("return_amt_inc_tax"),
            F.col("net_loss")
        )
    )
    return returns_with_date
