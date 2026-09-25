# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Approved Manufacturer Parts List (AMPL) Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | AMPL  | Approved Manufacturer Parts List | Base table — one row per approved manufacturer/material pairing |
# MAGIC | MARA  | General Material Data | Provides manufacturer part number (MFRPN) via EMATN → MATNR |
# MAGIC | MAKT  | Material Descriptions | Provides material description (MAKTX) via BMATN → MATNR |
# MAGIC | LFA1  | Vendor Master | Provides manufacturer name (NAME1) via MFRNR → LIFNR |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - AMPL is the base table. Each row represents one approved manufacturer for one internal material.
# MAGIC - MARA joins on `AMPL.EMATN = MARA.MATNR` to retrieve the manufacturer's part number (MFRPN).
# MAGIC - MAKT joins on `AMPL.BMATN = MAKT.MATNR` to retrieve the internal material description.
# MAGIC - LFA1 joins on `AMPL.MFRNR = LFA1.LIFNR` to retrieve the manufacturer name.
# MAGIC - All joins are left joins — AMPL rows with no match are retained.
# MAGIC - Rows where Plant (WERKS) is null are excluded.
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | MPN_Number | AMPL.EMATN | Manufacturer's part number in SAP (leading zeros stripped) |
# MAGIC | Material_Number | AMPL.BMATN / EMATN | Internal Mirion material number; falls back to EMATN if BMATN is blank |
# MAGIC | Material_Description | MAKT.MAKTX | Internal material description |
# MAGIC | Manufacturer_Name | LFA1.NAME1 | Manufacturer name from vendor master |
# MAGIC | Manufacturer_Number | AMPL.MFRNR | SAP vendor number for the manufacturer |
# MAGIC | Manufacturer_Material_Number | MARA.MFRPN | Manufacturer's own part number |
# MAGIC | Plant | AMPL.WERKS | Plant where this approval applies |
# MAGIC | Valid_From | AMPL.DATUV | Date this approval becomes effective (YYYYMMDD) |
# MAGIC | Valid_To | AMPL.DATUB | Date this approval expires (YYYYMMDD) |

# COMMAND ----------

from pyspark.sql import functions as F

def strip_leading_zeros(col):
    return F.when(
        col.rlike("^[0-9]+$"),
        F.regexp_replace(col, "^0+(?!$)", "")
    ).otherwise(col)

ampl = (
    spark.table("median_hub_captured.sap.ampl")
    .filter(F.col("MANDT") == "400")
    .alias("ampl")
)

mara = (
    spark.table("median_hub_captured.sap.mara")
    .filter(F.col("MANDT") == "400")
    .alias("mara")
)

makt = (
    spark.table("median_hub_captured.sap.makt")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .alias("makt")
)

lfa1 = (
    spark.table("median_hub_captured.sap.lfa1")
    .filter(F.col("MANDT") == "400")
    .select("LIFNR", "NAME1")
    .alias("lfa1")
)

result = (
    ampl
    .join(mara,
        (F.col("ampl.MANDT") == F.col("mara.MANDT")) &
        (F.col("ampl.EMATN") == F.col("mara.MATNR")),
        "left")
    .join(makt,
        (F.col("ampl.MANDT") == F.col("makt.MANDT")) &
        (F.col("ampl.BMATN") == F.col("makt.MATNR")),
        "left")
    .join(lfa1,
        F.col("ampl.MFRNR") == F.col("lfa1.LIFNR"),
        "left")
    .select(
        strip_leading_zeros(F.col("ampl.EMATN")).alias("MPN_Number"),
        strip_leading_zeros(
            F.when(
                F.col("ampl.BMATN").isNotNull() & (F.col("ampl.BMATN") != ""),
                F.col("ampl.BMATN")
            ).otherwise(F.col("ampl.EMATN"))
        ).alias("Material_Number"),
        F.col("makt.MAKTX").alias("Material_Description"),
        F.col("lfa1.NAME1").alias("Manufacturer_Name"),
        F.col("ampl.MFRNR").alias("Manufacturer_Number"),
        F.col("mara.MFRPN").alias("Manufacturer_Material_Number"),
        F.col("ampl.WERKS").alias("Plant"),
        F.col("ampl.DATUV").alias("Valid_From"),
        F.col("ampl.DATUB").alias("Valid_To")
    )
    .filter(F.col("Plant").isNotNull())
)

display(result)

result.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hub_live_transformed.sap.approved_mfg_part_list")
