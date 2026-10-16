# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Visual Data Prep (Lakeflow Designer Code Excerpt)
# Target Object: gold.vdp_call_center_monthly_returns
# Question: Monthly sales and returns per call centre with a rolling three-month return rate
# Operators:
# 1. Filter (Filter dates by parameter target_year)
# 2. Join (Join sales, returns, call_center, and date_dim)
# 3. Aggregate (Monthly summation of sales and returns per call centre)
# 4. Window / Derive (Compute rolling 3-month return rate as ratio of rolling sums)
# Parameter: target_year
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# 1. Parameter configuration
try:
    dbutils.widgets.text("target_year", "2001", "Analysis Year")
    target_year = int(dbutils.widgets.get("target_year"))
except Exception:
    target_year = 2001

print(f"Executing Visual Data Prep for Target Year: {target_year}")

# 2. Operator 1 & Operator 2: Read Silver sources and Join
sales_df = spark.table("meridian_t06.silver.silver_catalog_sales")
returns_df = spark.table("meridian_t06.silver.silver_catalog_returns")
dates_df = spark.table("meridian_t06.silver.silver_date_dim")
cc_df = spark.table("meridian_t06.gold.dim_call_center").filter(F.col("is_current") == True)

# Enrich Sales with calendar month and call centre
sales_monthly = (
    sales_df
    .join(dates_df, sales_df.sold_date_sk == dates_df.date_sk, "inner")
    .filter(dates_df.calendar_year == target_year)
    .join(cc_df, sales_df.call_center_sk == cc_df.call_center_sk, "inner")
    .groupBy(
        cc_df.call_center_id,
        cc_df.call_center_name,
        dates_df.calendar_year,
        dates_df.calendar_month
    )
    .agg(
        F.sum("net_paid").alias("monthly_net_sales"),
        F.countDistinct("order_number").alias("monthly_orders")
    )
)

# Enrich Returns with calendar month and call centre
returns_monthly = (
    returns_df
    .join(dates_df, returns_df.returned_date_sk == dates_df.date_sk, "inner")
    .filter(dates_df.calendar_year == target_year)
    .join(cc_df, returns_df.call_center_sk == cc_df.call_center_sk, "inner")
    .groupBy(
        cc_df.call_center_id,
        dates_df.calendar_year,
        dates_df.calendar_month
    )
    .agg(
        F.sum("return_amount").alias("monthly_returned_amount"),
        F.sum("return_quantity").alias("monthly_returned_quantity")
    )
)

# Operator 3: Join monthly sales and returns
monthly_combined = (
    sales_monthly
    .join(
        returns_monthly,
        on=["call_center_id", "calendar_year", "calendar_month"],
        how="left"
    )
    .fillna(0, subset=["monthly_returned_amount", "monthly_returned_quantity"])
)

# Operator 4: Window Derivation - Rolling 3-Month Window
# Note: Ratios must always be computed as ratio of rolling sums, not average of monthly ratios
window_3m = (
    Window
    .partitionBy("call_center_id")
    .orderBy("calendar_month")
    .rowsBetween(-2, 0)
)

vdp_result = (
    monthly_combined
    .withColumn(
        "rolling_3m_sales",
        F.sum("monthly_net_sales").over(window_3m)
    )
    .withColumn(
        "rolling_3m_returns",
        F.sum("monthly_returned_amount").over(window_3m)
    )
    .withColumn(
        "monthly_return_rate",
        F.round(
            F.when(F.col("monthly_net_sales") > 0, F.col("monthly_returned_amount") / F.col("monthly_net_sales")).otherwise(0.0),
            4
        )
    )
    .withColumn(
        "rolling_3m_return_rate",
        F.round(
            F.when(F.col("rolling_3m_sales") > 0, F.col("rolling_3m_returns") / F.col("rolling_3m_sales")).otherwise(0.0),
            4
        )
    )
    .select(
        "call_center_id",
        "call_center_name",
        "calendar_year",
        "calendar_month",
        "monthly_net_sales",
        "monthly_returned_amount",
        "monthly_return_rate",
        "rolling_3m_sales",
        "rolling_3m_returns",
        "rolling_3m_return_rate",
        "monthly_orders",
        F.current_timestamp().alias("vdp_computed_at")
    )
)

# Write output to Gold table: gold.vdp_call_center_monthly_returns
vdp_result.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.vdp_call_center_monthly_returns")

print("Visual Data Prep (vdp_call_center_monthly_returns) completed and written to Gold.")
