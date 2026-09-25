# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Material Class
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MARA  | General Material Data | Provides distinct material type codes (MTART) |
# MAGIC | MARC  | Plant Data for Material | Provides distinct procurement type codes (BESKZ) |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - MARA and MARC are not joined on a key. A cross join produces every combination of
# MAGIC   material type × procurement type, giving one row per valid pairing.
# MAGIC - Material types ZONT, ZHER, and HERS are excluded — these are inactive or legacy types
# MAGIC   not used in active procurement or production planning.
# MAGIC - The `material_class` column is a Mirion-defined business classification derived from the
# MAGIC   combination of material type and procurement type. It groups SAP material types into
# MAGIC   reporting categories used across NA Tech dashboards and reports.
# MAGIC
# MAGIC **material_class derivation logic:**
# MAGIC | Material Type | Procurement Type | Material Class |
# MAGIC |---------------|-----------------|----------------|
# MAGIC | ZERT | F | Tradable/Resale |
# MAGIC | ZALB | F | Raw |
# MAGIC | ZALB | E, X, blank, or NULL | Semi-FG |
# MAGIC | ZERT | E, X, blank, or NULL | FG |
# MAGIC | ZERP, ZHMI, ZIBE | any | Operating/Packing Supplies |
# MAGIC | ZIEN | any | Service |
# MAGIC | ZLAG, ZROC | any | Non-Stock Material |
# MAGIC | ZROH | any | Raw |
# MAGIC | ZAWB, ZAWA | any | Tradable/Resale |
# MAGIC | all others | any | Unknown |
# MAGIC
# MAGIC **Granularity:** One row per material type + procurement type combination
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | material_type | MARA.MTART | SAP material type code |
# MAGIC | procurement_type | MARC.BESKZ | SAP procurement type code (F = external, E = in-house, X = both, blank = unset) |
# MAGIC | material_class | Derived | Mirion business classification derived from material type and procurement type |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,MARA: Distinct Material Types
stg_mara = (
    spark.table("median_hub_captured.sap.MARA")
    .filter(F.col("MANDT") == "400")
    .filter(~F.col("MTART").isin("ZONT", "ZHER", "HERS"))
    .select(F.col("MTART"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,MARC: Distinct Procurement Types
stg_marc = (
    spark.table("median_hub_captured.sap.MARC")
    .filter(F.col("MANDT") == "400")
    .select(F.col("BESKZ"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_dim_material_class = (
    stg_mara
    .crossJoin(stg_marc)
    .select(
        F.col("MTART").alias("material_type"),
        F.col("BESKZ").alias("procurement_type"),

        F.when((F.col("MTART") == "ZERT") & (F.col("BESKZ") == "F"), "Tradable/Resale")
         .when((F.col("MTART") == "ZALB") & (F.col("BESKZ") == "F"), "Raw")
         .when((F.col("MTART") == "ZALB") & (F.col("BESKZ").isin("E", "X") | F.col("BESKZ").isNull() | (F.col("BESKZ") == "")), "Semi-FG")
         .when((F.col("MTART") == "ZERT") & (F.col("BESKZ").isin("E", "X") | F.col("BESKZ").isNull() | (F.col("BESKZ") == "")), "FG")
         .when(F.col("MTART").isin("ZERP", "ZHMI", "ZIBE"), "Operating/Packing Supplies")
         .when(F.col("MTART") == "ZIEN", "Service")
         .when(F.col("MTART").isin("ZLAG", "ZROC"), "Non-Stock Material")
         .when(F.col("MTART") == "ZROH", "Raw")
         .when(F.col("MTART").isin("ZAWB", "ZAWA"), "Tradable/Resale")
         .otherwise("Unknown")
         .alias("material_class")
    )
)

(
    df_dim_material_class
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.material_class")
)
