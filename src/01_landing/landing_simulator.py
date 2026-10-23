# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("batch_num", "1", "Batch Number")
batch_num = dbutils.widgets.get("batch_num")
base_vol = f"/Volumes/meridian_t06/landing/raw_files"

def write_batch(source_table, path, limit=None):
    df = spark.table(source_table)
    if limit:
        df = df.limit(limit)
    df.write.mode("overwrite").json(path)

# Land Fact Batches
write_batch("samples.tpcds_sf1.catalog_sales", f"{base_vol}/batch_{batch_num}/catalog_sales", limit=5000)
write_batch("samples.tpcds_sf1.catalog_returns", f"{base_vol}/batch_{batch_num}/catalog_returns", limit=1000)

# Land Dimensions Once (Batch 1)
if batch_num == "1":
    write_batch("samples.tpcds_sf1.call_center", f"{base_vol}/dims/call_center")
    write_batch("samples.tpcds_sf1.ship_mode", f"{base_vol}/dims/ship_mode")

print(f"Batch {batch_num} landed successfully at {base_vol}")