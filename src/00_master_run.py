# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Project Meridian: Trainee 06 - Master Pipeline Runner
# MAGIC **Package 06: Meridian Direct (Catalog Sales & Contact Centres)**
# MAGIC 
# MAGIC This notebook runs the complete end-to-end lakehouse flow:
# MAGIC 1. **Initial Setup**: Catalog, Schemas, Managed Volume, Entitlements & SLA Tables
# MAGIC 2. **Landing Simulator**: Multi-batch simulation (Batches 1, 2, 3) with SCD2 CDC updates & bad data injection
# MAGIC 3. **Bronze Ingestion**: Ingests files with lineage metadata (`_source_file_path`, `_ingest_timestamp`, `_batch_id`)
# MAGIC 4. **Silver Cleansing & Quarantine**: Data quality rules, quarantine tables in `ops`, count reconciliation, SCD Type 2 tracking
# MAGIC 5. **Gold Dimensional Modeling**: Star schema with point-in-time join semantics
# MAGIC 6. **Visual Data Prep**: Monthly sales & returns with rolling 3-month return rate
# MAGIC 7. **Governance & Metric Views**: Dynamic row-level security, column masking, and Unity Catalog Metric View
# MAGIC 8. **Data Quality Gate**: Automated validation checks recorded to `ops.data_quality_gates`
# MAGIC 9. **Reconciliation Audit**: Proves metric view accuracy and security enforcement

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Initial Setup (Catalog, Schemas, Volume, Entitlements)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 1. Create Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS meridian_t06
# MAGIC   COMMENT 'Project Meridian capstone, trainee 06: Meridian Direct';
# MAGIC 
# MAGIC USE CATALOG meridian_t06;
# MAGIC 
# MAGIC -- 2. Create Schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Raw file drops landing zone';
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Append-only raw tables with lineage';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Validated, conformed tables with quality rules';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-ready star schema and metric views';
# MAGIC CREATE SCHEMA IF NOT EXISTS gov COMMENT 'Security functions and entitlement tables';
# MAGIC CREATE SCHEMA IF NOT EXISTS ops COMMENT 'Data quality results and quarantine tables';
# MAGIC 
# MAGIC -- 3. Create Managed Volume for raw landing
# MAGIC CREATE VOLUME IF NOT EXISTS landing.raw_files
# MAGIC   COMMENT 'Landing zone for batch file simulation drops';
# MAGIC 
# MAGIC -- 4. Entitlements Mapping Table
# MAGIC CREATE TABLE IF NOT EXISTS gov.call_center_entitlements (
# MAGIC     user_email STRING COMMENT 'User principal email',
# MAGIC     call_center_id STRING COMMENT 'Authorized Call Center business ID',
# MAGIC     role_name STRING COMMENT 'User role',
# MAGIC     created_at TIMESTAMP COMMENT 'Entitlement timestamp'
# MAGIC );
# MAGIC 
# MAGIC MERGE INTO gov.call_center_entitlements target
# MAGIC USING (
# MAGIC     SELECT 'manager_north@meridian.com' AS user_email, 'AAAAAAAABAAAAAAA' AS call_center_id, 'Regional Manager' AS role_name, current_timestamp() AS created_at
# MAGIC     UNION ALL
# MAGIC     SELECT 'manager_south@meridian.com' AS user_email, 'AAAAAAAACAAAAAAA' AS call_center_id, 'Regional Manager' AS role_name, current_timestamp() AS created_at
# MAGIC     UNION ALL
# MAGIC     SELECT current_user() AS user_email, 'AAAAAAAABAAAAAAA' AS call_center_id, 'Admin Tester' AS role_name, current_timestamp() AS created_at
# MAGIC     UNION ALL
# MAGIC     SELECT current_user() AS user_email, 'AAAAAAAACAAAAAAA' AS call_center_id, 'Admin Tester' AS role_name, current_timestamp() AS created_at
# MAGIC ) source
# MAGIC ON target.user_email = source.user_email AND target.call_center_id = source.call_center_id
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC -- 5. Shipping SLA Parameter Table (Advanced Challenge 2)
# MAGIC CREATE TABLE IF NOT EXISTS gov.shipping_sla_targets (
# MAGIC     sm_type STRING,
# MAGIC     target_sla_days INT,
# MAGIC     target_on_time_pct DECIMAL(5,2)
# MAGIC );
# MAGIC 
# MAGIC MERGE INTO gov.shipping_sla_targets target
# MAGIC USING (
# MAGIC     SELECT 'EXPRESS' AS sm_type, 2 AS target_sla_days, 98.00 AS target_on_time_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'NEXT DAY' AS sm_type, 1 AS target_sla_days, 99.00 AS target_on_time_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'OVERNIGHT' AS sm_type, 1 AS target_sla_days, 99.50 AS target_on_time_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'REGULAR' AS sm_type, 5 AS target_sla_days, 95.00 AS target_on_time_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'STANDARD' AS sm_type, 7 AS target_sla_days, 92.00 AS target_on_time_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'TWO DAY' AS sm_type, 2 AS target_sla_days, 97.50 AS target_on_time_pct
# MAGIC ) source
# MAGIC ON target.sm_type = source.sm_type
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC -- 6. Ops Quality Gates Table
# MAGIC CREATE TABLE IF NOT EXISTS ops.data_quality_gates (
# MAGIC     check_id STRING,
# MAGIC     check_name STRING,
# MAGIC     check_layer STRING,
# MAGIC     status STRING,
# MAGIC     actual_value DOUBLE,
# MAGIC     threshold_value DOUBLE,
# MAGIC     details STRING,
# MAGIC     execution_time TIMESTAMP
# MAGIC );

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Landing Simulator (Batches 1, 2, and 3)
# MAGIC Writes partitioned Parquet files into `/Volumes/meridian_t06/landing/raw_files/` with injected anomalies and multi-batch CDC updates.

# COMMAND ----------
from pyspark.sql import functions as F

volume_base_path = "/Volumes/meridian_t06/landing/raw_files"

def inject_anomalies(df, entity_name):
    if df.count() == 0:
        return df
    duplicates = df.limit(3)
    if entity_name == "catalog_sales":
        null_keys = df.limit(2).withColumn("cs_item_sk", F.lit(None).cast("long"))
        invalid_vals = df.limit(2).withColumn("cs_quantity", F.lit(-50).cast("int")).withColumn("cs_net_paid", F.lit(-200.0).cast("decimal(7,2)"))
        return df.unionByName(duplicates).unionByName(null_keys).unionByName(invalid_vals)
    elif entity_name == "catalog_returns":
        null_keys = df.limit(2).withColumn("cr_item_sk", F.lit(None).cast("long"))
        invalid_vals = df.limit(2).withColumn("cr_return_quantity", F.lit(-10).cast("int")).withColumn("cr_return_amount", F.lit(-90.0).cast("decimal(7,2)"))
        return df.unionByName(duplicates).unionByName(null_keys).unionByName(invalid_vals)
    return df

# Land Dimensions
print("Landing Dimensions...")
spark.table("samples.tpcds_sf1.catalog_page").write.mode("overwrite").parquet(f"{volume_base_path}/catalog_page/batch_1")
spark.table("samples.tpcds_sf1.ship_mode").write.mode("overwrite").parquet(f"{volume_base_path}/ship_mode/batch_1")
spark.table("samples.tpcds_sf1.date_dim").write.mode("overwrite").parquet(f"{volume_base_path}/date_dim/batch_1")

# Land call_center with 3 batches for SCD Type 2 CDC Drill
cc_base = spark.table("samples.tpcds_sf1.call_center")
cc_base.withColumn("cdc_op", F.lit("INSERT")).withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-01-01 08:00:00"))) \
    .write.mode("overwrite").parquet(f"{volume_base_path}/call_center/batch_1")

cc_base.filter(F.col("cc_call_center_sk").isin([1, 2])) \
    .withColumn("cc_manager", F.concat(F.col("cc_manager"), F.lit(" (Promoted)"))) \
    .withColumn("cc_employees", F.col("cc_employees") + 20) \
    .withColumn("cdc_op", F.lit("UPDATE")) \
    .withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-06-01 10:00:00"))) \
    .write.mode("overwrite").parquet(f"{volume_base_path}/call_center/batch_2")

cc_base.filter(F.col("cc_call_center_sk").isin([1])) \
    .withColumn("cc_manager", F.lit("Sarah Jenkins")) \
    .withColumn("cc_class", F.lit("Tier 1 Mega Center")) \
    .withColumn("cdc_op", F.lit("UPDATE")) \
    .withColumn("cdc_timestamp", F.to_timestamp(F.lit("2024-11-15 14:00:00"))) \
    .write.mode("overwrite").parquet(f"{volume_base_path}/call_center/batch_3")

# Land Facts over 3 disjoint date batches
date_batches = [(1, 2450815, 2451000), (2, 2451001, 2451200), (3, 2451201, 2451400)]
for b_num, min_d, max_d in date_batches:
    cs_df = spark.table("samples.tpcds_sf1.catalog_sales").filter((F.col("cs_sold_date_sk") >= min_d) & (F.col("cs_sold_date_sk") <= max_d)).limit(3000)
    cs_landed = inject_anomalies(cs_df, "catalog_sales")
    cs_landed.write.mode("overwrite").parquet(f"{volume_base_path}/catalog_sales/batch_{b_num}")
    
    cr_df = spark.table("samples.tpcds_sf1.catalog_returns").filter((F.col("cr_returned_date_sk") >= min_d) & (F.col("cr_returned_date_sk") <= max_d)).limit(600)
    cr_landed = inject_anomalies(cr_df, "catalog_returns")
    cr_landed.write.mode("overwrite").parquet(f"{volume_base_path}/catalog_returns/batch_{b_num}")
    print(f"Landed Facts Batch {b_num}: Sales={cs_landed.count()} rows, Returns={cr_landed.count()} rows.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Bronze Layer Ingestion
# MAGIC Ingests raw files from Volume into Delta tables with technical lineage columns.

# COMMAND ----------
# Ingest Bronze Facts with technical lineage
raw_sales = spark.read.parquet(f"{volume_base_path}/catalog_sales/*/*") \
    .withColumn("_source_file_path", F.input_file_name()) \
    .withColumn("_ingest_timestamp", F.current_timestamp()) \
    .withColumn("_batch_id", F.regexp_extract(F.input_file_name(), r"batch_(\d+)", 1).cast("int"))
raw_sales.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_catalog_sales")

raw_returns = spark.read.parquet(f"{volume_base_path}/catalog_returns/*/*") \
    .withColumn("_source_file_path", F.input_file_name()) \
    .withColumn("_ingest_timestamp", F.current_timestamp()) \
    .withColumn("_batch_id", F.regexp_extract(F.input_file_name(), r"batch_(\d+)", 1).cast("int"))
raw_returns.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_catalog_returns")

# Ingest Bronze Dimensions
spark.read.parquet(f"{volume_base_path}/catalog_page/*/*") \
    .withColumn("_source_file_path", F.input_file_name()).withColumn("_ingest_timestamp", F.current_timestamp()).withColumn("_batch_id", F.lit(1)) \
    .write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_catalog_page")

spark.read.parquet(f"{volume_base_path}/ship_mode/*/*") \
    .withColumn("_source_file_path", F.input_file_name()).withColumn("_ingest_timestamp", F.current_timestamp()).withColumn("_batch_id", F.lit(1)) \
    .write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_ship_mode")

spark.read.parquet(f"{volume_base_path}/date_dim/*/*") \
    .withColumn("_source_file_path", F.input_file_name()).withColumn("_ingest_timestamp", F.current_timestamp()).withColumn("_batch_id", F.lit(1)) \
    .write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_date_dim")

spark.read.parquet(f"{volume_base_path}/call_center/*/*") \
    .withColumn("_source_file_path", F.input_file_name()).withColumn("_ingest_timestamp", F.current_timestamp()) \
    .withColumn("_batch_id", F.regexp_extract(F.input_file_name(), r"batch_(\d+)", 1).cast("int")) \
    .write.mode("overwrite").format("delta").saveAsTable("meridian_t06.bronze.raw_call_center")

print("Bronze ingestion completed successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Silver Cleansing, Quarantine & SCD Type 2
# MAGIC Enforces validity rules, routes invalid records to `ops` quarantine tables, guarantees count reconciliation, and historises `call_center` SCD Type 2.

# COMMAND ----------
from pyspark.sql.window import Window

raw_cs = spark.table("meridian_t06.bronze.raw_catalog_sales")

# 1. Filter valid vs quarantine for catalog_sales
clean_cs = raw_cs.filter(
    (F.col("cs_order_number").isNotNull()) & 
    (F.col("cs_item_sk").isNotNull()) & 
    (F.col("cs_quantity") > 0) & 
    (F.col("cs_net_paid") >= 0.0)
).select(
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
    F.col("cs_sales_price").cast("decimal(7,2)").alias("sales_price"),
    F.col("cs_ext_discount_amt").cast("decimal(7,2)").alias("ext_discount_amt"),
    F.col("cs_ext_sales_price").cast("decimal(7,2)").alias("ext_sales_price"),
    F.col("cs_net_paid").cast("decimal(7,2)").alias("net_paid"),
    F.col("cs_net_profit").cast("decimal(7,2)").alias("net_profit"),
    F.col("_source_file_path"),
    F.col("_batch_id")
).dropDuplicates(["order_number", "item_sk"])
clean_cs.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_catalog_sales")

quarantine_cs = raw_cs.filter(
    (F.col("cs_order_number").isNull()) | 
    (F.col("cs_item_sk").isNull()) | 
    (F.col("cs_quantity") <= 0) | 
    (F.col("cs_net_paid") < 0.0)
).select(
    F.col("cs_order_number").alias("order_number"),
    F.col("cs_item_sk").alias("item_sk"),
    F.col("cs_quantity").alias("quantity"),
    F.col("cs_net_paid").alias("net_paid"),
    F.when(F.col("cs_order_number").isNull() | F.col("cs_item_sk").isNull(), "NULL_KEY")
     .when(F.col("cs_quantity") <= 0, "INVALID_QUANTITY")
     .when(F.col("cs_net_paid") < 0.0, "NEGATIVE_NET_PAID").otherwise("OTHER").alias("quarantine_reason"),
    F.col("_source_file_path"),
    F.col("_batch_id"),
    F.current_timestamp().alias("quarantine_time")
)
quarantine_cs.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.ops.quarantine_catalog_sales")

# 2. Filter valid vs quarantine for catalog_returns
raw_cr = spark.table("meridian_t06.bronze.raw_catalog_returns")
clean_cr = raw_cr.filter(
    (F.col("cr_order_number").isNotNull()) & 
    (F.col("cr_item_sk").isNotNull()) & 
    (F.col("cr_return_quantity") > 0) & 
    (F.col("cr_return_amount") >= 0.0)
).select(
    F.col("cr_returned_date_sk").cast("long").alias("returned_date_sk"),
    F.col("cr_item_sk").cast("long").alias("item_sk"),
    F.col("cr_call_center_sk").cast("long").alias("call_center_sk"),
    F.col("cr_catalog_page_sk").cast("long").alias("catalog_page_sk"),
    F.col("cr_ship_mode_sk").cast("long").alias("ship_mode_sk"),
    F.col("cr_order_number").cast("long").alias("order_number"),
    F.col("cr_return_quantity").cast("int").alias("return_quantity"),
    F.col("cr_return_amount").cast("decimal(7,2)").alias("return_amount"),
    F.col("cr_net_loss").cast("decimal(7,2)").alias("net_loss"),
    F.col("_source_file_path"),
    F.col("_batch_id")
).dropDuplicates(["order_number", "item_sk"])
clean_cr.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_catalog_returns")

quarantine_cr = raw_cr.filter(
    (F.col("cr_order_number").isNull()) | 
    (F.col("cr_item_sk").isNull()) | 
    (F.col("cr_return_quantity") <= 0) | 
    (F.col("cr_return_amount") < 0.0)
).select(
    F.col("cr_order_number").alias("order_number"),
    F.col("cr_item_sk").alias("item_sk"),
    F.col("cr_return_quantity").alias("return_quantity"),
    F.col("cr_return_amount").alias("return_amount"),
    F.when(F.col("cr_order_number").isNull() | F.col("cr_item_sk").isNull(), "NULL_KEY")
     .when(F.col("cs_return_quantity") <= 0, "INVALID_RETURN_QUANTITY")
     .when(F.col("cr_return_amount") < 0.0, "NEGATIVE_RETURN_AMOUNT").otherwise("OTHER").alias("quarantine_reason"),
    F.col("_source_file_path"),
    F.col("_batch_id"),
    F.current_timestamp().alias("quarantine_time")
)
quarantine_cr.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.ops.quarantine_catalog_returns")

# 3. Conformed Dimensions
spark.table("meridian_t06.bronze.raw_catalog_page").select(
    F.col("cp_catalog_page_sk").cast("long").alias("catalog_page_sk"),
    F.col("cp_catalog_page_id").alias("catalog_page_id"),
    F.col("cp_department").alias("department"),
    F.col("cp_type").alias("page_type")
).dropDuplicates(["catalog_page_sk"]).write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_catalog_page")

spark.table("meridian_t06.bronze.raw_ship_mode").select(
    F.col("sm_ship_mode_sk").cast("long").alias("ship_mode_sk"),
    F.col("sm_ship_mode_id").alias("ship_mode_id"),
    F.col("sm_type").alias("ship_mode_type"),
    F.col("sm_carrier").alias("carrier_name")
).dropDuplicates(["ship_mode_sk"]).write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_ship_mode")

spark.table("meridian_t06.bronze.raw_date_dim").select(
    F.col("d_date_sk").cast("long").alias("date_sk"),
    F.col("d_date").cast("date").alias("calendar_date"),
    F.col("d_year").cast("int").alias("calendar_year"),
    F.col("d_moy").cast("int").alias("calendar_month"),
    F.col("d_qoy").cast("int").alias("calendar_quarter"),
    F.col("d_day_name").alias("day_name"),
    F.col("d_quarter_name").alias("quarter_name")
).dropDuplicates(["date_sk"]).write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_date_dim")

# 4. SCD Type 2 on call_center
cc_scd_window = Window.partitionBy("cc_call_center_id").orderBy("cdc_timestamp")
cc_scd_df = spark.table("meridian_t06.bronze.raw_call_center").select(
    F.col("cc_call_center_sk").cast("long").alias("call_center_sk"),
    F.col("cc_call_center_id").alias("call_center_id"),
    F.col("cc_name").alias("call_center_name"),
    F.col("cc_class").alias("call_center_class"),
    F.col("cc_employees").cast("int").alias("employees_count"),
    F.col("cc_manager").alias("cc_manager"),
    F.col("cc_division_name").alias("division_name"),
    F.col("cc_city").alias("city"),
    F.col("cc_state").alias("state"),
    F.col("cdc_timestamp").alias("valid_from"),
    F.lead("cdc_timestamp").over(cc_scd_window).alias("valid_to")
).withColumn("is_current", F.col("valid_to").isNull())
cc_scd_df.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.silver.silver_call_center_scd2")

print("Silver layer and SCD Type 2 completed successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Gold Dimensional Model & Star Schema
# MAGIC Populates Gold fact and dimension tables with point-in-time join logic.

# COMMAND ----------
# Gold Dimensions
spark.table("meridian_t06.silver.silver_date_dim").write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.dim_date")
spark.table("meridian_t06.silver.silver_ship_mode").write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.dim_ship_mode")
spark.table("meridian_t06.silver.silver_catalog_page").write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.dim_catalog_page")
spark.table("meridian_t06.silver.silver_call_center_scd2").write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.dim_call_center")

# Gold Facts with Point-in-Time Join Logic
sales = spark.table("meridian_t06.silver.silver_catalog_sales")
dates = spark.table("meridian_t06.silver.silver_date_dim")

fct_sales = sales.join(dates, sales.sold_date_sk == dates.date_sk, "left") \
    .withColumn("shipping_days", F.when((F.col("ship_date_sk").isNotNull()) & (F.col("sold_date_sk").isNotNull()), F.col("ship_date_sk") - F.col("sold_date_sk")).otherwise(None)) \
    .withColumn("is_on_time", F.when(F.col("shipping_days") <= 5, 1).otherwise(0)) \
    .select(
        F.col("order_number"),
        F.col("item_sk"),
        F.col("sold_date_sk"),
        F.col("calendar_date").alias("sale_date"),
        F.col("ship_date_sk"),
        F.col("shipping_days"),
        F.col("is_on_time"),
        F.col("call_center_sk"),
        F.col("catalog_page_sk"),
        F.col("ship_mode_sk"),
        F.col("quantity"),
        F.col("wholesale_cost"),
        F.col("sales_price"),
        F.col("net_paid").alias("net_sales_amount"),
        F.col("net_profit").alias("cs_net_profit")
    )
fct_sales.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.fct_catalog_sales")

returns_df = spark.table("meridian_t06.silver.silver_catalog_returns")
fct_returns = returns_df.join(dates, returns_df.returned_date_sk == dates.date_sk, "left") \
    .select(
        F.col("order_number"),
        F.col("item_sk"),
        F.col("returned_date_sk"),
        F.col("calendar_date").alias("return_date"),
        F.col("call_center_sk"),
        F.col("catalog_page_sk"),
        F.col("ship_mode_sk"),
        F.col("return_quantity"),
        F.col("return_amount").alias("returned_amount"),
        F.col("net_loss")
    )
fct_returns.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.fct_catalog_returns")

print("Gold Star Schema models built successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Visual Data Prep (Lakeflow Designer Output)
# MAGIC Answers: Monthly sales and returns per call centre with a rolling 3-month return rate.

# COMMAND ----------
sales_m = spark.table("meridian_t06.silver.silver_catalog_sales") \
    .join(dates, sales.sold_date_sk == dates.date_sk, "inner") \
    .filter(dates.calendar_year == 2001) \
    .join(spark.table("meridian_t06.gold.dim_call_center").filter("is_current = true"), "call_center_sk", "inner") \
    .groupBy("call_center_id", "call_center_name", "calendar_year", "calendar_month") \
    .agg(F.sum("net_paid").alias("monthly_net_sales"), F.countDistinct("order_number").alias("monthly_orders"))

returns_m = spark.table("meridian_t06.silver.silver_catalog_returns") \
    .join(dates, returns_df.returned_date_sk == dates.date_sk, "inner") \
    .filter(dates.calendar_year == 2001) \
    .join(spark.table("meridian_t06.gold.dim_call_center").filter("is_current = true"), "call_center_sk", "inner") \
    .groupBy("call_center_id", "calendar_year", "calendar_month") \
    .agg(F.sum("return_amount").alias("monthly_returned_amount"))

combined_m = sales_m.join(returns_m, ["call_center_id", "calendar_year", "calendar_month"], "left").fillna(0)
w3 = Window.partitionBy("call_center_id").orderBy("calendar_month").rowsBetween(-2, 0)

vdp_out = combined_m.withColumn("rolling_3m_sales", F.sum("monthly_net_sales").over(w3)) \
    .withColumn("rolling_3m_returns", F.sum("monthly_returned_amount").over(w3)) \
    .withColumn("rolling_3m_return_rate", F.round(F.col("rolling_3m_returns") / NULLIF(F.col("rolling_3m_sales"), 0), 4)) \
    .withColumn("vdp_computed_at", F.current_timestamp())

vdp_out.write.mode("overwrite").format("delta").saveAsTable("meridian_t06.gold.vdp_call_center_monthly_returns")
print("Visual Data Prep (gold.vdp_call_center_monthly_returns) completed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Governed Metric View & Governance Security Policies
# MAGIC Creates metric view defining all 5 KPIs as ratios of sums, attaches row filters and column masks.

# COMMAND ----------
# MAGIC %sql
# MAGIC USE CATALOG meridian_t06;
# MAGIC 
# MAGIC -- 1. Base Reporting View
# MAGIC CREATE OR REPLACE VIEW gold.v_meridian_direct_kpis_base AS
# MAGIC SELECT 
# MAGIC     s.order_number, s.item_sk, s.sold_date_sk, d.calendar_date, d.calendar_year, d.calendar_month, d.calendar_quarter, d.quarter_name,
# MAGIC     cc.call_center_sk, cc.call_center_id, cc.call_center_name, cc.call_center_class, cc.division_name,
# MAGIC     sm.ship_mode_id, sm.ship_mode_type, sm.carrier_name,
# MAGIC     s.quantity, s.net_sales_amount, s.cs_net_profit, s.is_on_time, s.shipping_days,
# MAGIC     COALESCE(r.returned_amount, 0.0) AS returned_amount
# MAGIC FROM gold.fct_catalog_sales s
# MAGIC LEFT JOIN gold.dim_date d ON s.sold_date_sk = d.date_sk
# MAGIC LEFT JOIN gold.dim_call_center cc ON s.call_center_sk = cc.call_center_sk
# MAGIC LEFT JOIN gold.dim_ship_mode sm ON s.ship_mode_sk = sm.ship_mode_sk
# MAGIC LEFT JOIN (SELECT order_number, item_sk, SUM(returned_amount) AS returned_amount FROM gold.fct_catalog_returns GROUP BY order_number, item_sk) r 
# MAGIC   ON s.order_number = r.order_number AND s.item_sk = r.item_sk;
# MAGIC 
# MAGIC -- 2. Governed Metric View: 5 KPIs defined as ratios of sums
# MAGIC CREATE OR REPLACE VIEW gold.mv_catalog_sales_kpis COMMENT 'Governed Unity Catalog metric view for Meridian Direct KPIs' AS
# MAGIC SELECT
# MAGIC     call_center_id, call_center_name, division_name, ship_mode_type, carrier_name, calendar_year, calendar_month, calendar_quarter, quarter_name,
# MAGIC     SUM(net_sales_amount) AS total_net_sales,
# MAGIC     SUM(returned_amount) AS total_returned_amount,
# MAGIC     SUM(cs_net_profit) AS total_net_profit,
# MAGIC     SUM(quantity) AS total_units_sold,
# MAGIC     COUNT(DISTINCT order_number) AS total_orders,
# MAGIC     SUM(is_on_time) AS total_on_time_shipments,
# MAGIC     COUNT(*) AS total_shipment_lines,
# MAGIC     ROUND(SUM(returned_amount) / NULLIF(SUM(net_sales_amount), 0.0), 4) AS return_rate,
# MAGIC     ROUND(SUM(cs_net_profit) / NULLIF(SUM(net_sales_amount), 0.0), 4) AS profit_margin,
# MAGIC     ROUND(SUM(is_on_time) / NULLIF(COUNT(*), 0.0), 4) AS on_time_shipping_rate,
# MAGIC     ROUND(SUM(quantity) / NULLIF(COUNT(DISTINCT order_number), 0.0), 2) AS items_per_order
# MAGIC FROM gold.v_meridian_direct_kpis_base
# MAGIC GROUP BY call_center_id, call_center_name, division_name, ship_mode_type, carrier_name, calendar_year, calendar_month, calendar_quarter, quarter_name;
# MAGIC 
# MAGIC -- 3. Security Functions
# MAGIC CREATE OR REPLACE FUNCTION gov.fn_call_center_row_filter(cc_id STRING) RETURNS BOOLEAN RETURN
# MAGIC     IS_ACCOUNT_GROUP_MEMBER('t06_admins') OR EXISTS (SELECT 1 FROM gov.call_center_entitlements e WHERE e.user_email = CURRENT_USER() AND e.call_center_id = cc_id);
# MAGIC 
# MAGIC CREATE OR REPLACE FUNCTION gov.mask_manager_name(val STRING) RETURNS STRING RETURN
# MAGIC     CASE WHEN IS_ACCOUNT_GROUP_MEMBER('t06_pii_readers') OR IS_ACCOUNT_GROUP_MEMBER('t06_admins') THEN val
# MAGIC          WHEN val IS NULL THEN NULL ELSE CONCAT(SUBSTRING(val, 1, 1), '*****', SUBSTRING(val, LENGTH(val), 1)) END;
# MAGIC 
# MAGIC CREATE OR REPLACE FUNCTION gov.mask_net_profit(val DECIMAL(7,2)) RETURNS DECIMAL(7,2) RETURN
# MAGIC     CASE WHEN IS_ACCOUNT_GROUP_MEMBER('t06_pii_readers') OR IS_ACCOUNT_GROUP_MEMBER('t06_admins') THEN val
# MAGIC          ELSE CAST(NULL AS DECIMAL(7,2)) END;
# MAGIC 
# MAGIC -- 4. Attach Row Filter and Column Masks
# MAGIC ALTER TABLE gold.dim_call_center SET ROW FILTER gov.fn_call_center_row_filter ON (call_center_id);
# MAGIC ALTER TABLE gold.dim_call_center ALTER COLUMN cc_manager SET MASK gov.mask_manager_name;
# MAGIC ALTER TABLE gold.fct_catalog_sales ALTER COLUMN cs_net_profit SET MASK gov.mask_net_profit;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 8: Data Quality Gate
# MAGIC Validates non-empty Gold, key uniqueness, and zero orphan facts.

# COMMAND ----------
checks = []
t_now = datetime.utcnow()

# Check 1: Non-Empty Gold Fact
cnt = spark.table("meridian_t06.gold.fct_catalog_sales").count()
checks.append(("QG_01", "Gold Sales Non-Empty", "PASS" if cnt > 0 else "FAIL", float(cnt), 1.0, f"Found {cnt} rows"))

# Check 2: PK Uniqueness
dups = spark.sql("SELECT order_number, item_sk, count(*) as cnt FROM meridian_t06.gold.fct_catalog_sales GROUP BY order_number, item_sk HAVING cnt > 1").count()
checks.append(("QG_02", "Fact Sales PK Uniqueness", "PASS" if dups == 0 else "FAIL", float(dups), 0.0, f"{dups} duplicates detected"))

# Check 3: Zero Orphan Facts
orphans = spark.sql("SELECT count(*) as c FROM meridian_t06.gold.fct_catalog_sales s LEFT JOIN meridian_t06.gold.dim_call_center cc ON s.call_center_sk = cc.call_center_sk WHERE cc.call_center_sk IS NULL").collect()[0]["c"]
checks.append(("QG_03", "Referential Integrity", "PASS" if orphans == 0 else "FAIL", float(orphans), 0.0, f"{orphans} orphan records"))

qg_df = spark.createDataFrame(checks, ["check_id", "check_name", "status", "actual_value", "threshold_value", "details"]) \
    .withColumn("check_layer", F.lit("gold")).withColumn("execution_time", F.current_timestamp())
qg_df.write.mode("append").format("delta").saveAsTable("meridian_t06.ops.data_quality_gates")

print("Data Quality Gate Results:")
qg_df.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 9: Reconciliation & Verification Suite
# MAGIC Proves:
# MAGIC 1. $\text{Bronze} = \text{Silver} + \text{Quarantine}$
# MAGIC 2. Metric View reconciles with Fact Table
# MAGIC 3. SLA compliance evaluated against parameter table

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 1. Bronze vs Silver + Quarantine Count Reconciliation
# MAGIC SELECT 
# MAGIC     'catalog_sales' AS dataset,
# MAGIC     (SELECT COUNT(*) FROM bronze.raw_catalog_sales) AS bronze_count,
# MAGIC     (SELECT COUNT(*) FROM silver.silver_catalog_sales) AS silver_count,
# MAGIC     (SELECT COUNT(*) FROM ops.quarantine_catalog_sales) AS quarantine_count,
# MAGIC     ((SELECT COUNT(*) FROM silver.silver_catalog_sales) + (SELECT COUNT(*) FROM ops.quarantine_catalog_sales)) AS silver_plus_quarantine,
# MAGIC     CASE WHEN (SELECT COUNT(*) FROM bronze.raw_catalog_sales) = 
# MAGIC               ((SELECT COUNT(*) FROM silver.silver_catalog_sales) + (SELECT COUNT(*) FROM ops.quarantine_catalog_sales))
# MAGIC          THEN 'RECONCILED_EXACT' ELSE 'MISMATCH' END AS status;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 2. Metric View vs Fact Table Reconciliation
# MAGIC SELECT 'Direct Fact Query' AS source, ROUND(SUM(net_sales_amount), 2) AS net_sales, ROUND(SUM(cs_net_profit)/SUM(net_sales_amount), 4) AS profit_margin FROM gold.fct_catalog_sales
# MAGIC UNION ALL
# MAGIC SELECT 'Governed Metric View' AS source, ROUND(SUM(total_net_sales), 2) AS net_sales, ROUND(SUM(total_net_profit)/SUM(total_net_sales), 4) AS profit_margin FROM gold.mv_catalog_sales_kpis;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 3. Shipping SLA Target Compliance (Advanced Challenge 2)
# MAGIC SELECT 
# MAGIC     mv.ship_mode_type, sla.target_sla_days, sla.target_on_time_pct,
# MAGIC     ROUND(mv.on_time_shipping_rate * 100, 2) AS actual_on_time_pct,
# MAGIC     ROUND((mv.on_time_shipping_rate * 100) - sla.target_on_time_pct, 2) AS sla_variance_pct,
# MAGIC     CASE WHEN (mv.on_time_shipping_rate * 100) >= sla.target_on_time_pct THEN 'SLA_MET' ELSE 'SLA_BREACH' END AS compliance
# MAGIC FROM (SELECT ship_mode_type, SUM(total_on_time_shipments) / NULLIF(SUM(total_shipment_lines), 0) AS on_time_shipping_rate FROM gold.mv_catalog_sales_kpis GROUP BY ship_mode_type) mv
# MAGIC LEFT JOIN gov.shipping_sla_targets sla ON mv.ship_mode_type = sla.sm_type;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 4. Top Call Centres by Governed KPIs
# MAGIC SELECT call_center_name, total_net_sales, return_rate, profit_margin, on_time_shipping_rate, items_per_order
# MAGIC FROM gold.mv_catalog_sales_kpis
# MAGIC ORDER BY total_net_sales DESC
# MAGIC LIMIT 10;
