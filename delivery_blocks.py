# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Delivery Blocks
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | TVLST | Delivery Block Texts | One row per delivery block code per language; provides code and English description |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - MANDT = '400'
# MAGIC - SPRAS = 'E' (English descriptions only)
# MAGIC
# MAGIC **Granularity:** One row per delivery block code (LIFSP)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | delivery_block | TVLST.LIFSP | Delivery block code; set on VBAK to prevent shipment processing |
# MAGIC | description | TVLST.VTEXT | English description of the delivery block |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Final Table
df_delivery_blocks = (
    spark.table("median_hub_captured.sap.TVLST")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(
        F.col("LIFSP").alias("delivery_block"),
        F.col("VTEXT").alias("description")
    )
)

(
    df_delivery_blocks
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.delivery_blocks")
)
