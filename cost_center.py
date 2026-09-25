# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Cost Centers
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | CSKS | Cost Center Master Data | One row per cost center per validity period; provides code, responsible person, and department |
# MAGIC | CSKT | Cost Center Texts | English descriptions per cost center and validity period |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - CSKS: MANDT = '400', DATBI = '9999-12-31' (active records only; expired cost centers excluded)
# MAGIC - CSKT: MANDT = '400', SPRAS = 'E'
# MAGIC - CSKT joined on KOSTL + KOKRS + DATBI to avoid cross-product duplicates from date-ranged rows
# MAGIC
# MAGIC **Granularity:** One row per active cost center (KOSTL + KOKRS)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | cost_center | CSKS.KOSTL | Cost center code; leading zeros removed for numeric codes |
# MAGIC | description | CSKT.KTEXT | English description of the cost center |
# MAGIC | controlling_area | CSKS.KOKRS | Controlling area the cost center belongs to (e.g. BUMN for NA Tech) |
# MAGIC | responsible_person | CSKS.VERAK | Name of the person responsible for the cost center |
# MAGIC | department | CSKS.ABTEI | Department code and label assigned to the cost center |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Final Table
df_cost_center = (
    spark.table("median_hub_captured.sap.CSKS")
    .filter((F.col("MANDT") == "400") & (F.col("DATBI").cast("date") == F.lit("9999-12-31")))
    .alias("csks")
    .join(
        spark.table("median_hub_captured.sap.CSKT")
        .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
        .alias("cskt"),
        on=(
            (F.col("csks.KOSTL") == F.col("cskt.KOSTL")) &
            (F.col("csks.KOKRS") == F.col("cskt.KOKRS")) &
            (F.col("csks.DATBI") == F.col("cskt.DATBI"))
        ),
        how="left"
    )
    .select(
        F.when(F.col("csks.KOSTL").rlike("^[0-9]+$"), F.regexp_replace(F.col("csks.KOSTL"), "^0+", ""))
         .otherwise(F.col("csks.KOSTL"))
         .alias("cost_center"),

        F.col("cskt.KTEXT").alias("description"),
        F.col("csks.KOKRS").alias("controlling_area"),
        F.col("csks.VERAK").alias("responsible_person"),
        F.col("csks.ABTEI").alias("department"),
    )
)

(
    df_cost_center
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.cost_center")
)
