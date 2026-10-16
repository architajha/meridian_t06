# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Bronze Ingestion Pipeline (Lakeflow Declarative)
# Ingests raw batch files incrementally via Auto Loader (cloudFiles)
# Adds technical lineage columns: _source_file_path, _ingest_timestamp, _batch_id
# Pure append-only, preserving source data fidelity without filtering or deduction
# =============================================================================

import dlt
from pyspark.sql import functions as F

# Pipeline configuration parameter
catalog_name = spark.conf.get("meridian.catalog", "meridian_t06")
volume_base = f"/Volumes/{catalog_name}/landing/raw_files"

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_catalog_sales (Fact)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_catalog_sales",
    comment="Append-only raw catalog sales transactions with file lineage metadata",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def raw_catalog_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_catalog_sales")
        .load(f"{volume_base}/catalog_sales/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_catalog_returns (Fact)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_catalog_returns",
    comment="Append-only raw catalog returns with file lineage metadata",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def raw_catalog_returns():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_catalog_returns")
        .load(f"{volume_base}/catalog_returns/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_call_center (Dimension with SCD CDC updates)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_call_center",
    comment="Append-only raw call center dimension drops including CDC operation markers",
    table_properties={
        "quality": "bronze"
    }
)
def raw_call_center():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_call_center")
        .load(f"{volume_base}/call_center/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_catalog_page (Dimension)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_catalog_page",
    comment="Append-only raw catalog page dimension records",
    table_properties={"quality": "bronze"}
)
def raw_catalog_page():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_catalog_page")
        .load(f"{volume_base}/catalog_page/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_ship_mode (Dimension)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_ship_mode",
    comment="Append-only raw shipping mode dimension records",
    table_properties={"quality": "bronze"}
)
def raw_ship_mode():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_ship_mode")
        .load(f"{volume_base}/ship_mode/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )

# -----------------------------------------------------------------------------
# Bronze Streaming Table: raw_date_dim (Dimension)
# -----------------------------------------------------------------------------
@dlt.table(
    name="raw_date_dim",
    comment="Append-only raw date dimension records",
    table_properties={"quality": "bronze"}
)
def raw_date_dim():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{volume_base}/_schemas/raw_date_dim")
        .load(f"{volume_base}/date_dim/*")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_(\d+)", 1).cast("int").alias("_batch_id")
        )
    )
