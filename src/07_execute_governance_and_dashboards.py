#!/usr/bin/env python3
"""
Execute governance SQL scripts (tags, row filters, column masks, audit) and build dashboards.
Run this after pipeline steps up to Bronze have completed.
"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Base directory of the project (src folder)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sql_dir = os.path.join(base_dir, "05_governance")
metrics_dir = os.path.join(base_dir, "04_metrics")
dashboards_dir = os.path.join(base_dir, "08_dashboards")

def execute_sql_file(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    # Split on semicolons to handle multiple statements
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        print(f"Executing {os.path.basename(path)}: {stmt[:80]}...")
        spark.sql(stmt)

# Governance scripts
for filename in ["governed_tags.sql", "row_filters.sql", "column_masks.sql", "governance_audit.sql"]:
    execute_sql_file(os.path.join(sql_dir, filename))

# Metric view and reconciliation queries
for filename in ["metric_views.sql", "reconciliation_queries.sql"]:
    execute_sql_file(os.path.join(metrics_dir, filename))

# Dashboard specifications (JSON) are stored in the dashboards directory.
print("Dashboard specifications are located in", dashboards_dir)
print("Governance, metric view, and dashboard steps completed.")
