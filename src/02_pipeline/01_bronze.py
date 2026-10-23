# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

def read_raw_stream(path):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(path)
        .select(
            "*", 
            col("_metadata.file_path").alias("_sq_source_file"), 
            current_timestamp().alias("_sq_rcvd_at")
        )
    )

@dlt.table(name="bronze_catalog_sales", comment="Raw streaming sales")
def bronze_catalog_sales():
    return read_raw_stream("/Volumes/meridian_t06/landing/raw_files/batch_*/catalog_sales")

@dlt.table(name="bronze_catalog_returns", comment="Raw streaming returns")
def bronze_catalog_returns():
    return read_raw_stream("/Volumes/meridian_t06/landing/raw_files/batch_*/catalog_returns")

@dlt.table(name="bronze_call_center", comment="Raw streaming call center dimension")
def bronze_call_center():
    return read_raw_stream("/Volumes/meridian_t06/landing/raw_files/dims/call_center")