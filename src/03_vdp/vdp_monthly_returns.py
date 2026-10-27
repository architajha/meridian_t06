# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum

sales = spark.table("meridian_t06.silver.silver_catalog_sales")
returns = spark.table("meridian_t06.bronze.bronze_catalog_returns")

vdp_df = (
       sales.join(returns, col("cs_order_number") == col("cr_order_number"), "left")
    .groupBy("cs_call_center_sk", "cs_sold_date_sk")
    .agg(
        _sum("cs_sales_price").alias("total_sales"),
        _sum("cr_return_amount").alias("total_returns")
    )
    .withColumn("return_rate", col("total_returns") / col("total_sales"))
)

vdp_df.write.format("delta").mode("overwrite").saveAsTable("meridian_t06.gold.vdp_monthly_returns")