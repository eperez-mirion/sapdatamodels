# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Material Last Movement
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MSEG  | Material Document Items | Base table — one row per goods movement line item |
# MAGIC | MKPF  | Material Document Headers | Provides posting date (BUDAT) via MBLNR + MJAHR |
# MAGIC | MAKT  | Material Descriptions | Provides material short text in English (SPRAS = E) |
# MAGIC | MARA  | General Material Data | Provides material type and material group |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - MSEG and MKPF are joined on MBLNR + MJAHR (inner) to produce one row per movement line with its posting date.
# MAGIC - A window function partitioned by MATNR + WERKS and ordered by BUDAT descending (then MBLNR
# MAGIC   descending as a tie-breaker) ranks each movement per material + plant. Only rank = 1 is kept,
# MAGIC   giving the single most recent movement for each material at each plant.
# MAGIC - MAKT joins on MATNR (left) to add the English description.
# MAGIC - MARA joins on MATNR (left) to add material type and group.
# MAGIC - days_since_last_movement is calculated as the difference between the current run date and
# MAGIC   last_movement_date. Useful for identifying slow-moving or dormant inventory.
# MAGIC
# MAGIC **Granularity:** One row per material + plant (most recent movement only)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | material_number | MSEG.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | material_type | MARA.MTART | Material type code (ROH, HALB, FERT, etc.) |
# MAGIC | material_group | MARA.MATKL | Material group / commodity code |
# MAGIC | plant | MSEG.WERKS | Plant |
# MAGIC | last_movement_date | MKPF.BUDAT | Posting date of the most recent goods movement (YYYYMMDD) |
# MAGIC | last_movement_type | MSEG.BWART | Movement type of the most recent goods movement |
# MAGIC | last_document_number | MSEG.MBLNR | Material document number of the most recent movement |
# MAGIC | days_since_last_movement | Derived | Calendar days between last_movement_date and the date this notebook was run |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,MSEG + MKPF: Movements with Posting Date
stg_mseg = (
    spark.table("median_hub_captured.sap.MSEG")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MBLNR"),
        F.col("MJAHR"),
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("BWART")
    )
)

stg_mkpf = (
    spark.table("median_hub_captured.sap.MKPF")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MBLNR"),
        F.col("MJAHR"),
        F.col("BUDAT")
    )
)

stg_movements = (
    stg_mseg
    .join(stg_mkpf, on=["MBLNR", "MJAHR"], how="inner")
)

# COMMAND ----------

# DBTITLE 1,Last Movement per Material + Plant
last_movement_window = Window.partitionBy("MATNR", "WERKS").orderBy(
    F.col("BUDAT").desc(),
    F.col("MBLNR").desc()
)

stg_last = (
    stg_movements
    .withColumn("mvt_rank", F.row_number().over(last_movement_window))
    .filter(F.col("mvt_rank") == 1)
    .drop("mvt_rank", "MJAHR")
)

# COMMAND ----------

# DBTITLE 1,Reference Tables
makt = (
    spark.table("median_hub_captured.sap.MAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select("MATNR", "MAKTX")
)

stg_mara = (
    spark.table("median_hub_captured.sap.MARA")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("MTART"),
        F.col("MATKL")
    )
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_material_last_movement = (
    stg_last
    .join(makt,     on="MATNR", how="left")
    .join(stg_mara, on="MATNR", how="left")
    .select(
        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("MTART").alias("material_type"),
        F.col("MATKL").alias("material_group"),
        F.col("WERKS").alias("plant"),
        F.col("BUDAT").alias("last_movement_date"),
        F.col("BWART").alias("last_movement_type"),
        F.col("MBLNR").alias("last_document_number"),
        F.datediff(
            F.current_date(),
            F.to_date(F.col("BUDAT"), "yyyyMMdd")
        ).alias("days_since_last_movement")
    )
)

(
    df_material_last_movement
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.material_last_movement")
)
