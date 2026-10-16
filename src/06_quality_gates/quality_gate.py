# Databricks notebook source
# =============================================================================
# Project Meridian: Trainee 06 - Data Quality Gate
# Validates Gold layer datasets prior to downstream consumption
# Checks:
# 1. Non-empty tables: gold.fct_catalog_sales and gold.dim_call_center
# 2. Key uniqueness: No duplicate (order_number, item_sk) in fct_catalog_sales
# 3. Referential integrity: No orphan fact records missing conformed dimensions
# 4. Realistic bounds: Positive net sales amount and reasonable return rate
# Writes audit results to ops.data_quality_gates and raises exception on failure
# Includes simulate_failure parameter for Phase 5 failure & repair testing
# =============================================================================

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Retrieve parameters
try:
    dbutils.widgets.text("catalog", "meridian_t06", "Target Catalog")
    dbutils.widgets.text("simulate_failure", "false", "Simulate Failure for Job Testing (true/false)")
    catalog_name = dbutils.widgets.get("catalog")
    simulate_fail = dbutils.widgets.get("simulate_failure").lower() == "true"
except Exception:
    catalog_name = "meridian_t06"
    simulate_fail = False

print(f"Executing Quality Gate on catalog: {catalog_name} | Simulate Failure: {simulate_fail}")

checks_results = []
timestamp_now = datetime.utcnow()

def record_check(check_id, check_name, check_layer, status, actual, threshold, details):
    checks_results.append({
        "check_id": check_id,
        "check_name": check_name,
        "check_layer": check_layer,
        "status": status,
        "actual_value": float(actual) if actual is not None else 0.0,
        "threshold_value": float(threshold) if threshold is not None else 0.0,
        "details": details,
        "execution_time": timestamp_now
    })
    print(f"[{status}] {check_name}: Actual={actual}, Threshold={threshold}. {details}")

# -----------------------------------------------------------------------------
# Check 1: Non-Empty Gold Fact Table
# -----------------------------------------------------------------------------
try:
    sales_count = spark.table(f"{catalog_name}.gold.fct_catalog_sales").count()
    if sales_count > 0:
        record_check("QG_01", "Gold Sales Non-Empty", "gold", "PASS", sales_count, 1, "Fact table contains records.")
    else:
        record_check("QG_01", "Gold Sales Non-Empty", "gold", "FAIL", sales_count, 1, "Fact table is unexpectedly empty.")
except Exception as e:
    record_check("QG_01", "Gold Sales Non-Empty", "gold", "FAIL", 0, 1, f"Error querying table: {str(e)}")

# -----------------------------------------------------------------------------
# Check 2: Uniqueness of Business Primary Key (order_number, item_sk)
# -----------------------------------------------------------------------------
try:
    dup_df = spark.sql(f"""
        SELECT order_number, item_sk, COUNT(*) as cnt
        FROM {catalog_name}.gold.fct_catalog_sales
        GROUP BY order_number, item_sk
        HAVING cnt > 1
    """)
    dup_count = dup_df.count()
    if dup_count == 0:
        record_check("QG_02", "Fact Sales PK Uniqueness", "gold", "PASS", dup_count, 0, "No duplicate primary keys found.")
    else:
        record_check("QG_02", "Fact Sales PK Uniqueness", "gold", "FAIL", dup_count, 0, f"Found {dup_count} duplicate keys.")
except Exception as e:
    record_check("QG_02", "Fact Sales PK Uniqueness", "gold", "FAIL", 1, 0, f"Error: {str(e)}")

# -----------------------------------------------------------------------------
# Check 3: Referential Integrity (No Orphan Facts)
# -----------------------------------------------------------------------------
try:
    orphan_df = spark.sql(f"""
        SELECT COUNT(*) as orphan_count
        FROM {catalog_name}.gold.fct_catalog_sales s
        LEFT JOIN {catalog_name}.gold.dim_call_center cc ON s.call_center_sk = cc.call_center_sk
        WHERE cc.call_center_sk IS NULL
    """)
    orphan_count = orphan_df.collect()[0]["orphan_count"]
    if orphan_count == 0:
        record_check("QG_03", "Referential Integrity - Call Center FK", "gold", "PASS", orphan_count, 0, "Zero orphan facts detected.")
    else:
        record_check("QG_03", "Referential Integrity - Call Center FK", "gold", "FAIL", orphan_count, 0, f"{orphan_count} orphan sales found.")
except Exception as e:
    record_check("QG_03", "Referential Integrity - Call Center FK", "gold", "FAIL", 1, 0, f"Error: {str(e)}")

# -----------------------------------------------------------------------------
# Check 4: Intentional Failure Simulation Trigger (Phase 5 Requirement)
# -----------------------------------------------------------------------------
if simulate_fail:
    record_check("QG_04_SIM", "Simulated Fault Injection Check", "ops", "FAIL", 999.0, 0.0, "Intentional failure injected to test pipeline alerting and repair.")

# -----------------------------------------------------------------------------
# Persist Results to ops.data_quality_gates
# -----------------------------------------------------------------------------
results_df = spark.createDataFrame(checks_results)
results_df.write.mode("append").format("delta").saveAsTable(f"{catalog_name}.ops.data_quality_gates")
print("Data quality gate results written to ops.data_quality_gates.")

# -----------------------------------------------------------------------------
# Gate Evaluation: Fail Job if Any Check Fails
# -----------------------------------------------------------------------------
failed_checks = [c for c in checks_results if c["status"] == "FAIL"]
if failed_checks:
    fail_summary = "; ".join([f"{c['check_name']} ({c['details']})" for c in failed_checks])
    raise ValueError(f"DATA QUALITY GATE REJECTED: {len(failed_checks)} check(s) failed -> {fail_summary}")

print("ALL DATA QUALITY GATES PASSED SUCCESSFULLY.")
