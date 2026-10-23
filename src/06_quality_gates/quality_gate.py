# Databricks notebook source
# Databricks notebook source
bronze = spark.table("meridian_t06.bronze.bronze_catalog_sales").count()
silver = spark.table("meridian_t06.bronze.silver_catalog_sales").count()
quarantine = spark.table("meridian_t06.ops.quarantine_sales").count()

print(f"Reconciliation: Bronze ({bronze}) == Silver ({silver}) + Quarantine ({quarantine})")
assert bronze == (silver + quarantine), "Data Reconciliation Failed!"
print("QUALITY GATES PASSED")