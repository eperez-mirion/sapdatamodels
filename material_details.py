# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Material Details
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MARC  | Plant Data for Material | Base table — one row per material + plant |
# MAGIC | MARA  | General Material Data | Material-level attributes: type, group, UOM, weight, dimensions |
# MAGIC | MAKT  | Material Descriptions | Material short text in English (SPRAS = E) |
# MAGIC | MBEW  | Material Valuation | Standard/moving-average price and valuation class at plant level |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - MARC is the base table, providing one row per material per plant.
# MAGIC - MARA joins on MATNR (inner) — only materials present in MARA are included.
# MAGIC - MAKT joins on MATNR (left) to add the English short text.
# MAGIC - MBEW joins on MATNR + BWKEY = WERKS (left) to add plant-level valuation.
# MAGIC   BWKEY is renamed to WERKS before the join. Filtered to standard valuation records (BWTAR = '').
# MAGIC
# MAGIC **Granularity:** One row per material + plant (MARC)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | material_number | MARC.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | plant | MARC.WERKS | Plant |
# MAGIC | material_type | MARA.MTART | Material type code (ROH = raw, HALB = semi-finished, FERT = finished) |
# MAGIC | material_group | MARA.MATKL | Material group / commodity code |
# MAGIC | base_unit_of_measure | MARA.MEINS | Base unit of measure |
# MAGIC | division | MARA.SPART | Division |
# MAGIC | old_material_number | MARA.BISMT | Legacy / predecessor material number |
# MAGIC | creation_date | MARA.ERSDA | Date the material master record was created (YYYYMMDD) |
# MAGIC | net_weight | MARA.NTGEW | Net weight per base unit |
# MAGIC | gross_weight | MARA.BRGEW | Gross weight per base unit |
# MAGIC | weight_unit | MARA.GEWEI | Unit of weight (KG, G, LB, etc.) |
# MAGIC | volume | MARA.VOLUM | Volume per base unit |
# MAGIC | volume_unit | MARA.VOLEH | Unit of volume (L, ML, CM3, etc.) |
# MAGIC | manufacturer_part_number | MARA.MFRPN | Manufacturer's own part number |
# MAGIC | profit_center | MARC.PRCTR | Profit center at plant level |
# MAGIC | mrp_type | MARC.DISMM | MRP type code (PD = MRP, VB = reorder point, ND = no planning) |
# MAGIC | mrp_controller | MARC.DISPO | MRP controller / planner code |
# MAGIC | lot_sizing_procedure | MARC.DISLS | Lot sizing procedure (EX = lot-for-lot, FX = fixed, etc.) |
# MAGIC | procurement_type | MARC.BESKZ | Procurement type: E = in-house, F = external, X = both |
# MAGIC | special_procurement_type | MARC.SOBSL | Special procurement key (subcontracting, consignment, phantom, etc.) |
# MAGIC | plant_material_status | MARC.MMSTA | Plant-specific material status; blocks certain transactions when set |
# MAGIC | abc_indicator | MARC.ABCIN | ABC classification at plant level: A = high value, B = medium, C = low |
# MAGIC | purchasing_group | MARC.EKGRP | Purchasing group responsible for this material at this plant |
# MAGIC | planned_delivery_time_days | MARC.PLIFZ | Planned delivery time in calendar days |
# MAGIC | gr_processing_time_days | MARC.WEBAZ | Goods receipt processing time in workdays |
# MAGIC | safety_stock_qty | MARC.EISBE | Safety stock level |
# MAGIC | reorder_point | MARC.MINBE | Reorder point quantity |
# MAGIC | maximum_stock_level | MARC.MABST | Maximum stock level |
# MAGIC | valuation_class | MBEW.BKLAS | Valuation class controlling G/L account assignment for inventory postings |
# MAGIC | price_control | MBEW.VPRSV | Price control: S = standard price, V = moving average price |
# MAGIC | standard_price | MBEW.STPRS | Standard price per price unit in local currency |
# MAGIC | moving_average_price | MBEW.VERPR | Moving average price per price unit in local currency |
# MAGIC | price_unit | MBEW.PEINH | Price unit for valuation prices |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,MARC: Plant Data for Material
stg_marc = (
    spark.table("median_hub_captured.sap.MARC")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("PRCTR"),
        F.col("DISMM"),
        F.col("DISPO"),
        F.col("DISLS"),
        F.col("BESKZ"),
        F.col("SOBSL"),
        F.col("MMSTA"),
        F.col("ABCIN"),
        F.col("EKGRP"),
        F.col("PLIFZ"),
        F.col("WEBAZ"),
        F.col("EISBE"),
        F.col("MINBE"),
        F.col("MABST")
    )
)

# COMMAND ----------

# DBTITLE 1,MARA: General Material Data
stg_mara = (
    spark.table("median_hub_captured.sap.MARA")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("MTART"),
        F.col("MATKL"),
        F.col("MEINS"),
        F.col("SPART"),
        F.col("BISMT"),
        F.col("ERSDA"),
        F.col("NTGEW"),
        F.col("BRGEW"),
        F.col("GEWEI"),
        F.col("VOLUM"),
        F.col("VOLEH"),
        F.col("MFRPN")
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

# DBTITLE 1,MBEW: Material Valuation
stg_mbew = (
    spark.table("median_hub_captured.sap.MBEW")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("BWTAR") == "")
    )
    .select(
        F.col("MATNR"),
        F.col("BWKEY").alias("WERKS"),
        F.col("BKLAS"),
        F.col("VPRSV"),
        F.col("STPRS"),
        F.col("VERPR"),
        F.col("PEINH").alias("val_peinh")
    )
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_material_details = (
    stg_marc
    .join(stg_mara,  on="MATNR",            how="inner")
    .join(makt,      on="MATNR",            how="left")
    .join(stg_mbew,  on=["MATNR", "WERKS"], how="left")
    .select(
        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("WERKS").alias("plant"),
        F.col("MTART").alias("material_type"),
        F.col("MATKL").alias("material_group"),
        F.col("MEINS").alias("base_unit_of_measure"),
        F.col("SPART").alias("division"),
        F.col("BISMT").alias("old_material_number"),
        F.col("ERSDA").alias("creation_date"),
        F.col("NTGEW").alias("net_weight"),
        F.col("BRGEW").alias("gross_weight"),
        F.col("GEWEI").alias("weight_unit"),
        F.col("VOLUM").alias("volume"),
        F.col("VOLEH").alias("volume_unit"),
        F.col("MFRPN").alias("manufacturer_part_number"),
        F.col("PRCTR").alias("profit_center"),
        F.col("DISMM").alias("mrp_type"),
        F.col("DISPO").alias("mrp_controller"),
        F.col("DISLS").alias("lot_sizing_procedure"),
        F.col("BESKZ").alias("procurement_type"),
        F.col("SOBSL").alias("special_procurement_type"),
        F.col("MMSTA").alias("plant_material_status"),
        F.col("ABCIN").alias("abc_indicator"),
        F.col("EKGRP").alias("purchasing_group"),
        F.col("PLIFZ").alias("planned_delivery_time_days"),
        F.col("WEBAZ").alias("gr_processing_time_days"),
        F.col("EISBE").alias("safety_stock_qty"),
        F.col("MINBE").alias("reorder_point"),
        F.col("MABST").alias("maximum_stock_level"),
        F.col("BKLAS").alias("valuation_class"),
        F.col("VPRSV").alias("price_control"),
        F.col("STPRS").alias("standard_price"),
        F.col("VERPR").alias("moving_average_price"),
        F.col("val_peinh").alias("price_unit")
    )
)

(
    df_material_details
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.material_details")
)
