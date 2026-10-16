# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Landing Simulator
# Package 06: Meridian Direct (samples.tpcds_sf1)
# Writes partitioned Parquet files to Unity Catalog Managed Volume:
# /Volumes/meridian_t06/landing/raw_files/<entity>/batch_<batch_number>/
# Parameter-driven: catalog, batch_number, rows_per_batch
# =============================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# 1. Parameter retrieval (Widget-driven or environment fallback)
try:
    dbutils.widgets.text("catalog", "meridian_t06", "Target Catalog")
    dbutils.widgets.text("batch_number", "1", "Batch Number (1, 2, 3)")
    dbutils.widgets.text("rows_per_batch", "2500", "Max Rows per Batch")
    
    catalog_name = dbutils.widgets.get("catalog")
    batch_num = int(dbutils.widgets.get("batch_number"))
    max_rows = int(dbutils.widgets.get("rows_per_batch"))
except Exception:
    catalog_name = "meridian_t06"
    batch_num = 1
    max_rows = 2500

volume_base_path = f"/Volumes/{catalog_name}/landing/raw_files"
print(f"Executing Landing Simulator: Catalog={catalog_name}, Batch={batch_num}, RowsPerBatch={max_rows}")
print(f"Target Volume Path: {volume_base_path}")

# Date range windows to ensure disjoint fact batches
date_windows = {
    1: (2450815, 2451100),  # Batch 1
    2: (2451101, 2451350),  # Batch 2
    3: (2451351, 2451600)   # Batch 3
}
min_date, max_date = date_windows.get(batch_num, (2450815 + (batch_num - 1) * 200, 2451000 + batch_num * 200))

# =============================================================================
# Helper function: inject controlled bad data into fact datasets
# - Injects null primary/foreign keys
# - Injects duplicate records
# - Injects invalid values (negative quantity / negative price)
# =============================================================================
def inject_anomalies(df, entity_name, batch_id):
    if df.count() == 0:
        return df
    
    # Take a small sample (e.g., 5 rows) to duplicate
    duplicates = df.limit(5)
    
    if entity_name == "catalog_sales":
        # Introduce null keys (cs_item_sk is null)
        null_keys = df.limit(3).withColumn("cs_item_sk", F.lit(None).cast("long"))
        # Introduce invalid domain values (negative quantity and negative price)
        invalid_values = df.limit(3).withColumn("cs_quantity", F.lit(-99).cast("int")) \
                                   .withColumn("cs_net_paid", F.lit(-500.0).cast("decimal(7,2)"))
        return df.unionByName(duplicates).unionByName(null_keys).unionByName(invalid_values)
    
    elif entity_name == "catalog_returns":
        # Introduce null keys (cr_item_sk is null)
        null_keys = df.limit(3).withColumn("cr_item_sk", F.lit(None).cast("long"))
        # Introduce invalid values (negative return amount)
        invalid_values = df.limit(3).withColumn("cr_return_quantity", F.lit(-5).cast("int")) \
                                   .withColumn("cr_return_amount", F.lit(-120.0).cast("decimal(7,2)"))
        return df.unionByName(duplicates).unionByName(null_keys).unionByName(invalid_values)
    
    return df

# =============================================================================
# 2. Land Fact Datasets: catalog_sales and catalog_returns
# =============================================================================
# Catalog Sales (Disjoint by cs_sold_date_sk)
cs_source = spark.table("samples.tpcds_sf1.catalog_sales") \
    .filter((F.col("cs_sold_date_sk") >= min_date) & (F.col("cs_sold_date_sk") <= max_date)) \
    .limit(max_rows)

cs_with_anomalies = inject_anomalies(cs_source, "catalog_sales", batch_num)
cs_target_path = f"{volume_base_path}/catalog_sales/batch_{batch_num}"
cs_with_anomalies.write.mode("overwrite").format("parquet").save(cs_target_path)
print(f"Landed catalog_sales Batch {batch_num}: {cs_with_anomalies.count()} rows -> {cs_target_path}")

# Catalog Returns (Disjoint by cr_returned_date_sk)
cr_source = spark.table("samples.tpcds_sf1.catalog_returns") \
    .filter((F.col("cr_returned_date_sk") >= min_date) & (F.col("cr_returned_date_sk") <= max_date)) \
    .limit(max_rows // 5)

cr_with_anomalies = inject_anomalies(cr_source, "catalog_returns", batch_num)
cr_target_path = f"{volume_base_path}/catalog_returns/batch_{batch_num}"
cr_with_anomalies.write.mode("overwrite").format("parquet").save(cr_target_path)
print(f"Landed catalog_returns Batch {batch_num}: {cr_with_anomalies.count()} rows -> {cr_target_path}")

# =============================================================================
# 3. Land Conformed Dimensions: catalog_page, ship_mode, date_dim (Landed once)
# =============================================================================
if batch_num == 1:
    # Catalog Page
    cp_df = spark.table("samples.tpcds_sf1.catalog_page")
    cp_path = f"{volume_base_path}/catalog_page/batch_1"
    cp_df.write.mode("overwrite").format("parquet").save(cp_path)
    print(f"Landed catalog_page Dimension: {cp_df.count()} rows -> {cp_path}")
    
    # Ship Mode
    sm_df = spark.table("samples.tpcds_sf1.ship_mode")
    sm_path = f"{volume_base_path}/ship_mode/batch_1"
    sm_df.write.mode("overwrite").format("parquet").save(sm_path)
    print(f"Landed ship_mode Dimension: {sm_df.count()} rows -> {sm_path}")
    
    # Date Dim
    dd_df = spark.table("samples.tpcds_sf1.date_dim")
    dd_path = f"{volume_base_path}/date_dim/batch_1"
    dd_df.write.mode("overwrite").format("parquet").save(dd_path)
    print(f"Landed date_dim Dimension: {dd_df.count()} rows -> {dd_path}")

# =============================================================================
# 4. Land call_center Dimension with Multi-Batch Updates for SCD Type 2 CDC Drill
# Batch 1: Baseline call center snapshot
# Batch 2: Attribute updates (manager changes and employee growth)
# Batch 3: Further modifications (tier changes and tax adjustments)
# =============================================================================
cc_base = spark.table("samples.tpcds_sf1.call_center")

if batch_num == 1:
    cc_batch_df = cc_base.withColumn("cdc_op", F.lit("INSERT")) \
                         .withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-01-01 08:00:00")))
    cc_path = f"{volume_base_path}/call_center/batch_1"
    cc_batch_df.write.mode("overwrite").format("parquet").save(cc_path)
    print(f"Landed call_center Batch 1 (Baseline): {cc_batch_df.count()} rows -> {cc_path}")

elif batch_num == 2:
    # Mutate attributes for call centres to create version 2 in SCD Type 2
    cc_batch2 = cc_base.filter(F.col("cc_call_center_sk").isin([1, 2, 3])) \
        .withColumn("cc_manager", F.concat(F.col("cc_manager"), F.lit(" (Promoted)"))) \
        .withColumn("cc_employees", F.col("cc_employees") + 25) \
        .withColumn("cdc_op", F.lit("UPDATE")) \
        .withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-06-01 10:00:00")))
    
    cc_path = f"{volume_base_path}/call_center/batch_2"
    cc_batch2.write.mode("overwrite").format("parquet").save(cc_path)
    print(f"Landed call_center Batch 2 (SCD2 Updates): {cc_batch2.count()} rows -> {cc_path}")

elif batch_num == 3:
    # Mutate additional attributes for version 3
    cc_batch3 = cc_base.filter(F.col("cc_call_center_sk").isin([1, 4])) \
        .withColumn("cc_manager", F.lit("Sarah Jenkins")) \
        .withColumn("cc_class", F.lit("Tier 1 Mega Center")) \
        .withColumn("cc_tax_percentage", F.lit(0.085)) \
        .withColumn("cdc_op", F.lit("UPDATE")) \
        .withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-11-15 14:30:00")))
    
    cc_path = f"{volume_base_path}/call_center/batch_3"
    cc_batch3.write.mode("overwrite").format("parquet").save(cc_path)
    print(f"Landed call_center Batch 3 (SCD2 Updates): {cc_batch3.count()} rows -> {cc_path}")

print("Landing Simulator completed successfully.")
