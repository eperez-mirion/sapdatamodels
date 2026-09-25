# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Inventory Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MARD  | Plant Stock | Base stock quantities for standard plant stock by material, plant, and storage location |
# MAGIC | MSKA  | Sales Order Stock | Special stock type E — stock tied to a specific sales order |
# MAGIC | MSPR  | Project / WBS Stock | Special stock type Q — stock assigned to a project or WBS element |
# MAGIC | MSSL  | Vendor Stock | Special stock types O (subcontracting) and V (returnable packaging) — parts physically at the vendor |
# MAGIC | MARA  | General Material Data | Provides material type (MTART), division (SPART), base UOM (MEINS), and material group (MATKL) |
# MAGIC | MARC  | Plant-Level Material Data | Provides profit center (PRCTR) and ABC indicator (ABCIN) |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC | MBEW  | Material Valuation | Provides standard price (STPRS / PEINH) and valuation class (BKLAS); filtered to standard valuation (BWTAR = '') |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - MARD, MSKA, MSPR, and MSSL are each filtered and projected into a common stock schema then unioned.
# MAGIC - Each row carries a `special_stock_indicator`: blank for MARD (standard), E for MSKA, Q for MSPR, O or V for MSSL.
# MAGIC - MSSL rows carry NULL for `storage_location` — parts at the vendor have no plant storage location.
# MAGIC - The combined stock DataFrame is joined inner to MARA and MAKT on MATNR — only materials with master data are included.
# MAGIC - MARC joins on MATNR + WERKS (inner) to supply plant-level profit center and ABC indicator.
# MAGIC - MBEW joins on MATNR + WERKS (left) for standard price; BWKEY functions as the plant key.
# MAGIC - Rows where ALL stock quantities are zero are excluded from the output.
# MAGIC - Special stock W (customer consignment, MSKU) is excluded — source table not currently accessible.
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | material_number | MATNR | SAP material number; leading zeros stripped for all-numeric values |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | plant | WERKS | Plant code |
# MAGIC | profit_center | MARC.PRCTR | Profit center assigned to the material at plant level |
# MAGIC | division | MARA.SPART | Division assigned to the material |
# MAGIC | storage_location | LGORT | Storage location; NULL for vendor stock (MSSL) |
# MAGIC | special_stock_indicator | Derived | Blank = standard \| E = sales order \| Q = project/WBS \| O = subcontracting \| V = returnable packaging |
# MAGIC | material_type | MARA.MTART | Material type code (e.g. ROH, HALB, FERT) |
# MAGIC | abc_indicator | MARC.ABCIN | ABC classification: A = high value, B = medium, C = low |
# MAGIC | base_unit_of_measure | MARA.MEINS | Base unit of measure for all stock quantities |
# MAGIC | material_group | MARA.MATKL | Material group / commodity code |
# MAGIC | valuation_class | MBEW.BKLAS | Valuation class controlling G/L account determination |
# MAGIC | unit_price | MBEW.STPRS / PEINH | Standard price per base unit of measure in local currency |
# MAGIC | unrestricted_qty | LABST | Unrestricted-use stock; fully available for MRP and production |
# MAGIC | quality_inspection_qty | INSME | Stock in quality inspection; not yet released for use |
# MAGIC | blocked_qty | SPEME | Blocked stock; excluded from MRP |
# MAGIC | transfer_qty | MARD.UMLME | Stock in transit (plant-to-plant); standard stock only |
# MAGIC | restricted_use_qty | EINME | Restricted-use stock; standard stock only |
# MAGIC | returns_qty | MARD.RETME | Returns stock pending inspection or reprocessing; standard stock only |
# MAGIC | total_qty | Derived | Sum of all stock categories |
# MAGIC | total_value | Derived | total_qty × unit_price |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,MARD: Plant stock
stg_mard = (
    spark.table("median_hub_captured.sap.MARD")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.lit("").alias("special_stock_indicator"),
        F.coalesce(F.col("LABST"), F.lit(0)).alias("unrestricted_qty"),
        F.coalesce(F.col("INSME"), F.lit(0)).alias("quality_inspection_qty"),
        F.coalesce(F.col("SPEME"), F.lit(0)).alias("blocked_qty"),
        F.coalesce(F.col("UMLME"), F.lit(0)).alias("transfer_qty"),
        F.coalesce(F.col("EINME"), F.lit(0)).alias("restricted_use_qty"),
        F.coalesce(F.col("RETME"), F.lit(0)).alias("returns_qty")
    )
)

# COMMAND ----------

# DBTITLE 1,MSKA: Sales Order Stock
stg_mska = (
    spark.table("median_hub_captured.sap.MSKA")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.lit("E").alias("special_stock_indicator"),
        F.coalesce(F.col("KALAB"), F.lit(0)).alias("unrestricted_qty"),
        F.coalesce(F.col("KAINS"), F.lit(0)).alias("quality_inspection_qty"),
        F.coalesce(F.col("KASPE"), F.lit(0)).alias("blocked_qty"),
        F.lit(0).alias("transfer_qty"),
        F.coalesce(F.col("KAEIN"), F.lit(0)).alias("restricted_use_qty"),
        F.lit(0).alias("returns_qty")
    )
)

# COMMAND ----------

# DBTITLE 1,MSPR: Project Stock
stg_mspr = (
    spark.table("median_hub_captured.sap.MSPR")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.lit("Q").alias("special_stock_indicator"),
        F.coalesce(F.col("PRLAB"), F.lit(0)).alias("unrestricted_qty"),
        F.coalesce(F.col("PRINS"), F.lit(0)).alias("quality_inspection_qty"),
        F.coalesce(F.col("PRSPE"), F.lit(0)).alias("blocked_qty"),
        F.lit(0).alias("transfer_qty"),
        F.coalesce(F.col("PREIN"), F.lit(0)).alias("restricted_use_qty"),
        F.lit(0).alias("returns_qty")
    )
)

# COMMAND ----------

# DBTITLE 1,MSSL: Vendor Stock
stg_mssl = (
    spark.table("median_hub_captured.sap.MSSL")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("SOBKZ").isin("O", "V"))
    )
    .select(
        F.col("MATNR"),
        F.col("WERKS"),
        F.lit(None).cast("string").alias("LGORT"),
        F.col("SOBKZ").alias("special_stock_indicator"),
        F.coalesce(F.col("SLLAB"), F.lit(0)).alias("unrestricted_qty"),
        F.coalesce(F.col("SLINS"), F.lit(0)).alias("quality_inspection_qty"),
        F.lit(0).alias("blocked_qty"),
        F.coalesce(F.col("SLUML"), F.lit(0)).alias("transfer_qty"),
        F.coalesce(F.col("SLEIN"), F.lit(0)).alias("restricted_use_qty"),
        F.lit(0).alias("returns_qty")
    )
)

# COMMAND ----------

# DBTITLE 1,Combine Stocks
stg_stock = (
    stg_mard
    .union(stg_mska)
    .union(stg_mspr)
    .union(stg_mssl)
)

# COMMAND ----------

# DBTITLE 1,Reference Tables
mara = (
    spark.table("median_hub_captured.sap.MARA")
    .filter(F.col("MANDT") == "400")
    .select("MATNR", "MTART", "SPART", "MEINS", "MATKL")
)

makt = (
    spark.table("median_hub_captured.sap.MAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select("MATNR", "MAKTX")
)

marc = (
    spark.table("median_hub_captured.sap.MARC")
    .filter(F.col("MANDT") == "400")
    .select("MATNR", "WERKS", "PRCTR", "ABCIN")
)

# COMMAND ----------

# DBTITLE 1,MBEW: Valuation
stg_valuation = (
    spark.table("median_hub_captured.sap.MBEW")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("BWTAR") == "")
    )
    .select(
        F.col("MATNR"),
        F.col("BWKEY").alias("WERKS"),
        F.col("BKLAS"),
        F.round(
            F.col("STPRS") / F.when(F.col("PEINH") == 0, None).otherwise(F.col("PEINH")),
            2
        ).alias("unit_price")
    )
)

# COMMAND ----------

# DBTITLE 1,Final Table
total_qty = (
    F.col("unrestricted_qty") +
    F.col("quality_inspection_qty") +
    F.col("blocked_qty") +
    F.col("transfer_qty") +
    F.col("restricted_use_qty") +
    F.col("returns_qty")
)

df_inventory = (
    stg_stock
    .join(mara,          on="MATNR",            how="inner")
    .join(makt,          on="MATNR",            how="inner")
    .join(marc,          on=["MATNR", "WERKS"],  how="inner")
    .join(stg_valuation, on=["MATNR", "WERKS"],  how="left")
    .filter(
        (F.col("unrestricted_qty")       != 0) |
        (F.col("quality_inspection_qty") != 0) |
        (F.col("blocked_qty")            != 0) |
        (F.col("transfer_qty")           != 0) |
        (F.col("restricted_use_qty")     != 0) |
        (F.col("returns_qty")            != 0)
    )
    .select(
        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("WERKS").alias("plant"),
        F.col("PRCTR").alias("profit_center"),
        F.col("SPART").alias("division"),
        F.col("LGORT").alias("storage_location"),
        F.col("special_stock_indicator"),
        F.col("MTART").alias("material_type"),
        F.col("ABCIN").alias("abc_indicator"),
        F.col("MEINS").alias("base_unit_of_measure"),
        F.col("MATKL").alias("material_group"),
        F.col("BKLAS").alias("valuation_class"),
        F.col("unit_price"),
        F.col("unrestricted_qty"),
        F.col("quality_inspection_qty"),
        F.col("blocked_qty"),
        F.col("transfer_qty"),
        F.col("restricted_use_qty"),
        F.col("returns_qty"),
        total_qty.alias("total_qty"),
        F.round(total_qty * F.col("unit_price"), 2).alias("total_value")
    )
)

(
    df_inventory
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.inventory")
)
