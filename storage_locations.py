# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Storage Locations
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | T001L | Storage Locations | One row per storage location per plant; provides code and description |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - MANDT = '400'
# MAGIC - WERKS limited to NA Tech plants: 4002 (Concord), 4019 (Oak Ridge), 4020 (Meriden),
# MAGIC   4021 (Olen), 4022 (Oxfordshire)
# MAGIC
# MAGIC **Granularity:** One row per plant + storage location (WERKS + LGORT)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | plant | T001L.WERKS | Plant code |
# MAGIC | storage_location | T001L.LGORT | Storage location code within the plant |
# MAGIC | description | T001L.LGOBE | Description of the storage location |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Final Table
df_storage_locations = (
    spark.table("median_hub_captured.sap.T001L")
    .filter(F.col("MANDT") == "400")
    .filter(F.col("WERKS").isin("4002", "4019", "4020", "4021", "4022"))
    .select(
        F.col("WERKS").alias("plant"),

        F.when(F.col("LGORT").rlike("^[0-9]+$"), F.regexp_replace(F.col("LGORT"), "^0+", ""))
         .otherwise(F.col("LGORT"))
         .alias("storage_location"),

        F.col("LGOBE").alias("description")
    )
)

(
    df_storage_locations
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.storage_locations")
)
