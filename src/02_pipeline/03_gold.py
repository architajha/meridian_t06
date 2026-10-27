# Databricks notebook source
import dlt

@dlt.table(name="gold.dim_call_center", comment="Gold call center dimension")
def dim_call_center():
    return dlt.read("silver_call_center")

@dlt.table(name="gold.fct_catalog_sales", comment="Gold catalog sales fact table")
def fct_catalog_sales():
    return dlt.read("silver_catalog_sales").select(
        "cs_sold_date_sk",
        "cs_call_center_sk",
        "cs_item_sk",
        "cs_order_number",
        "cs_quantity",
        "cs_sales_price",
        "cs_net_profit"
    )