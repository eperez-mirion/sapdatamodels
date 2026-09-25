# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Purchasing Groups
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | T024  | Purchasing Groups | One row per purchasing group code; provides buyer name |
# MAGIC
# MAGIC **Granularity:** One row per purchasing group (EKGRP)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | purchasing_group | T024.EKGRP | SAP purchasing group code (e.g. M01, P02) |
# MAGIC | buyer_name | T024.EKNAM | Name of the buyer or purchasing group responsible |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Final Table
df_purchasing_groups = (
    spark.table("median_hub_captured.sap.T024")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EKGRP").alias("purchasing_group"),
        F.col("EKNAM").alias("buyer_name")
    )
)

(
    df_purchasing_groups
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.purchasing_groups")
)
