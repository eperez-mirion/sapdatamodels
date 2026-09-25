# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Payment Terms
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | T052  | Payment Terms | One row per payment terms key (ZTERM); provides baseline date indicator |
# MAGIC | T052U | Payment Terms Texts | Language-dependent description for each payment terms key |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - T052 is the base table providing the payment terms key (ZTERM) and configuration.
# MAGIC - T052U joins on ZTERM (left) filtered to MANDT = '400' and SPRAS = 'E' to add the
# MAGIC   English description.
# MAGIC
# MAGIC **Granularity:** One row per payment terms key (ZTERM)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | payment_terms | T052.ZTERM | SAP payment terms key (e.g. N030 = net 30 days, Z001 = immediate) |
# MAGIC | description | T052U.TEXT1 | English description of the payment terms |
# MAGIC | baseline_date | T052.ZTAG1 | Baseline date indicator for due date calculation |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,T052U: Payment Terms Descriptions
stg_t052u = (
    spark.table("median_hub_captured.sap.T052U")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(F.col("ZTERM"), F.col("TEXT1"))
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_payment_terms = (
    spark.table("median_hub_captured.sap.T052")
    .filter(F.col("MANDT") == "400")
    .join(stg_t052u, on="ZTERM", how="left")
    .select(
        F.col("ZTERM").alias("payment_terms"),
        F.col("TEXT1").alias("description"),
        F.col("ZTAG1").alias("baseline_date")
    )
)

(
    df_payment_terms
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.payment_terms")
)
