# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Material Reservations Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | RESB  | Reservation / Requirements | Base table — one row per reservation line; provides all reservation attributes |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - RESB is the base table. Rows with deletion flag XLOEK = 'X' are excluded at source.
# MAGIC - MAKT joins on MATNR (inner) to add the material description — rows without a material master description are excluded.
# MAGIC - All-numeric values in AUFNR and MATNR have leading zeros stripped.
# MAGIC
# MAGIC **Mirrors:** SAP MB25 transaction
# MAGIC
# MAGIC **Granularity:** One row per reservation item (RSNUM + RSPOS)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | reservation_number | RESB.RSNUM | SAP reservation or dependent requirement number |
# MAGIC | reservation_item | RESB.RSPOS | Line item number within the reservation |
# MAGIC | order_number | RESB.AUFNR | Production, maintenance, or internal order number; leading zeros stripped |
# MAGIC | material_number | RESB.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | plant | RESB.WERKS | Plant where the material is required |
# MAGIC | storage_location | RESB.LGORT | Storage location from which the material will be issued |
# MAGIC | special_stock_indicator | RESB.SOBKZ | Special stock type (E = sales order, Q = project, O = subcontracting) |
# MAGIC | reservation_type | RESB.RSART | Record type: distinguishes reservations from dependent requirements |
# MAGIC | account_assignment_category | RESB.KNTTP | Cost object type: K = cost center, F = order, P = WBS element, A = asset |
# MAGIC | movement_type | RESB.BWART | Inventory movement type used when goods are issued |
# MAGIC | requirement_date | RESB.BDTER | Date the material must be available (YYYYMMDD) |
# MAGIC | base_unit_of_measure | RESB.MEINS | Base unit of measure for all quantities |
# MAGIC | requirement_qty | RESB.BDMNG | Total quantity required for this reservation line |
# MAGIC | withdrawn_qty | RESB.ENMNG | Quantity already issued against this reservation |
# MAGIC | difference_qty | Derived | BDMNG − ENMNG — outstanding open quantity |
# MAGIC | final_issue_indicator | RESB.KZEAR | X = final goods issue posted; blank = still open |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,RESB
stg_resb = (
    spark.table("median_hub_captured.sap.RESB")
    .filter(
        (F.col("MANDT") == "400") &
        (F.coalesce(F.col("XLOEK"), F.lit("")) != "X")
    )
    .select(
        F.col("RSNUM"),
        F.col("RSPOS"),
        F.col("AUFNR"),
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.col("SOBKZ"),
        F.col("RSART"),
        F.col("KNTTP"),
        F.col("BWART"),
        F.col("BDTER"),
        F.col("MEINS"),
        F.col("BDMNG"),
        F.col("ENMNG"),
        F.col("KZEAR")
    )
)

# COMMAND ----------

# DBTITLE 1,MAKT: Material Descriptions
makt = (
    spark.table("median_hub_captured.sap.MAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select("MATNR", "MAKTX")
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_material_reservation = (
    stg_resb
    .join(makt, on="MATNR", how="inner")
    .select(
        F.col("RSNUM").alias("reservation_number"),
        F.col("RSPOS").alias("reservation_item"),

        F.when(F.col("AUFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("AUFNR"), "^0+", ""))
         .otherwise(F.col("AUFNR"))
         .alias("order_number"),

        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("WERKS").alias("plant"),
        F.col("LGORT").alias("storage_location"),
        F.col("SOBKZ").alias("special_stock_indicator"),
        F.col("RSART").alias("reservation_type"),
        F.col("KNTTP").alias("account_assignment_category"),
        F.col("BWART").alias("movement_type"),
        F.col("BDTER").alias("requirement_date"),
        F.col("MEINS").alias("base_unit_of_measure"),
        F.col("BDMNG").alias("requirement_qty"),
        F.col("ENMNG").alias("withdrawn_qty"),
        (F.col("BDMNG") - F.col("ENMNG")).alias("difference_qty"),
        F.col("KZEAR").alias("final_issue_indicator")
    )
)

(
    df_material_reservation
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.material_reservation")
)
