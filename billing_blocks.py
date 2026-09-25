# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Billing Blocks
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | TVFST | Billing Block Texts | One row per billing block code per language; provides code and English description |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - MANDT = '400'
# MAGIC - SPRAS = 'E' (English descriptions only)
# MAGIC
# MAGIC **Granularity:** One row per billing block code (FAKSP)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | billing_block | TVFST.FAKSP | Billing block code; set on VBAK or VBAP to prevent invoice creation |
# MAGIC | description | TVFST.VTEXT | English description of the billing block |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Final Table
df_billing_blocks = (
    spark.table("median_hub_captured.sap.TVFST")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(
        F.col("FAKSP").alias("billing_block"),
        F.col("VTEXT").alias("description")
    )
)

(
    df_billing_blocks
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.billing_blocks")
)
