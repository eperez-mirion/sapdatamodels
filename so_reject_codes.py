# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Sales Order Rejection Codes
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | TVAGT | Sales Document Rejection Reason Texts | One row per rejection reason per language; provides code and English description |
# MAGIC
# MAGIC **status logic:**
# MAGIC A Mirion-defined classification grouping rejection codes by their business meaning.
# MAGIC Rejection codes not in the mapping default to 'none'.
# MAGIC
# MAGIC | status | ABGRU codes |
# MAGIC |--------|-------------|
# MAGIC | Canceled | 08, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 |
# MAGIC | Closed | Z5 |
# MAGIC | none | all others |
# MAGIC
# MAGIC **Granularity:** One row per rejection reason code (ABGRU)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | rejection_code | TVAGT.ABGRU | SAP sales order rejection reason code |
# MAGIC | description | TVAGT.BEZEI | English description of the rejection reason |
# MAGIC | status | Derived | Mirion classification: Canceled \| Closed \| none |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Status Mapping
# Mirion status classification by ABGRU. Codes not listed default to 'none'.
status_dict = {
    '08': 'Canceled', '20': 'Canceled', '21': 'Canceled', '22': 'Canceled',
    '23': 'Canceled', '24': 'Canceled', '25': 'Canceled', '26': 'Canceled',
    '27': 'Canceled', '28': 'Canceled', '29': 'Canceled', '30': 'Canceled',
    'Z5': 'Closed',
}

status_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(status_dict.items())])

# COMMAND ----------

# DBTITLE 1,Final Table
df_so_reject_codes = (
    spark.table("median_hub_captured.sap.TVAGT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(
        F.col("ABGRU").alias("rejection_code"),
        F.col("BEZEI").alias("description"),
        F.coalesce(status_map[F.col("ABGRU")], F.lit("none")).alias("status")
    )
)

(
    df_so_reject_codes
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.so_reject_codes")
)
