# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Material Usage (Goods Movements)
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MSEG  | Material Document Items | Base table — one row per goods movement line item |
# MAGIC | MKPF  | Material Document Headers | Provides document date and posting date via MBLNR + MJAHR |
# MAGIC | MAKT  | Material Descriptions | Provides material short text in English (SPRAS = E) |
# MAGIC | MARA  | General Material Data | Provides material type and material group |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - MSEG is the base table; each row is one line item of a goods movement document.
# MAGIC - MKPF joins on MBLNR + MJAHR (inner) to add the document date and posting date.
# MAGIC - MAKT joins on MATNR (left) to add the English material description.
# MAGIC - MARA joins on MATNR (left) to add material type and material group.
# MAGIC - No movement type or date filters are applied — the full goods movement history is included.
# MAGIC   Use movement_type and posting_date to slice in downstream queries.
# MAGIC
# MAGIC **Granularity:** One row per material document item (MSEG)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | posting_date | MKPF.BUDAT | Date the goods movement was posted to the ledger (YYYYMMDD) |
# MAGIC | document_date | MKPF.BLDAT | Date on the physical document (YYYYMMDD) |
# MAGIC | material_document_number | MSEG.MBLNR | Material document number |
# MAGIC | material_document_year | MSEG.MJAHR | Fiscal year of the material document |
# MAGIC | material_document_item | MSEG.ZEILE | Line item number within the material document |
# MAGIC | movement_type | MSEG.BWART | SAP movement type (e.g. 101 = GR for PO, 261 = GI for production, 201 = GI for cost center) |
# MAGIC | debit_credit_indicator | MSEG.SHKZG | Stock direction: S = stock increase (debit), H = stock decrease (credit) |
# MAGIC | material_number | MSEG.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | material_type | MARA.MTART | Material type code (ROH, HALB, FERT, etc.) |
# MAGIC | material_group | MARA.MATKL | Material group / commodity code |
# MAGIC | plant | MSEG.WERKS | Plant where the movement occurred |
# MAGIC | storage_location | MSEG.LGORT | Storage location within the plant |
# MAGIC | special_stock_indicator | MSEG.SOBKZ | Special stock type (E = sales order, Q = project, blank = standard) |
# MAGIC | quantity | MSEG.MENGE | Quantity moved in the unit of measure of the document line |
# MAGIC | unit_of_measure | MSEG.MEINS | Unit of measure for the quantity |
# MAGIC | amount_local_currency | MSEG.DMBTR | Amount in company code local currency |
# MAGIC | order_number | MSEG.AUFNR | Production, maintenance, or internal order associated with the movement; leading zeros stripped |
# MAGIC | purchase_order_number | MSEG.EBELN | Purchase order number associated with the movement; leading zeros stripped |
# MAGIC | purchase_order_item | MSEG.EBELP | PO line item number; leading zeros stripped |
# MAGIC | cost_center | MSEG.KOSTL | Cost center charged for the goods issue |
# MAGIC | reservation_number | MSEG.RSNUM | Reservation number fulfilled by this movement; leading zeros stripped |
# MAGIC | created_by | MKPF.USNAM | Username of the person who posted the document |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,MSEG: Material Document Items
stg_mseg = (
    spark.table("median_hub_captured.sap.MSEG")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MBLNR"),
        F.col("MJAHR"),
        F.col("ZEILE"),
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.col("BWART"),
        F.col("SHKZG"),
        F.col("SOBKZ"),
        F.col("MENGE"),
        F.col("MEINS"),
        F.col("DMBTR"),
        F.col("AUFNR"),
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("KOSTL"),
        F.col("RSNUM")
    )
)

# COMMAND ----------

# DBTITLE 1,MKPF: Material Document Headers
stg_mkpf = (
    spark.table("median_hub_captured.sap.MKPF")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("MBLNR"),
        F.col("MJAHR"),
        F.col("BLDAT"),
        F.col("BUDAT"),
        F.col("USNAM")
    )
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
df_material_usage = (
    stg_mseg
    .join(stg_mkpf, on=["MBLNR", "MJAHR"], how="inner")
    .join(makt,     on="MATNR",            how="left")
    .join(stg_mara, on="MATNR",            how="left")
    .select(
        F.col("BUDAT").alias("posting_date"),
        F.col("BLDAT").alias("document_date"),
        F.col("MBLNR").alias("material_document_number"),
        F.col("MJAHR").alias("material_document_year"),
        F.col("ZEILE").alias("material_document_item"),
        F.col("BWART").alias("movement_type"),
        F.col("SHKZG").alias("debit_credit_indicator"),

        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("MTART").alias("material_type"),
        F.col("MATKL").alias("material_group"),
        F.col("WERKS").alias("plant"),
        F.col("LGORT").alias("storage_location"),
        F.col("SOBKZ").alias("special_stock_indicator"),
        F.col("MENGE").alias("quantity"),
        F.col("MEINS").alias("unit_of_measure"),
        F.col("DMBTR").alias("amount_local_currency"),

        F.when(F.col("AUFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("AUFNR"), "^0+", ""))
         .otherwise(F.col("AUFNR"))
         .alias("order_number"),

        F.when(F.col("EBELN").rlike("^[0-9]+$"), F.regexp_replace(F.col("EBELN"), "^0+", ""))
         .otherwise(F.col("EBELN"))
         .alias("purchase_order_number"),

        F.when(F.col("EBELP").rlike("^[0-9]+$"), F.regexp_replace(F.col("EBELP"), "^0+", ""))
         .otherwise(F.col("EBELP"))
         .alias("purchase_order_item"),

        F.col("KOSTL").alias("cost_center"),

        F.when(F.col("RSNUM").rlike("^[0-9]+$"), F.regexp_replace(F.col("RSNUM"), "^0+", ""))
         .otherwise(F.col("RSNUM"))
         .alias("reservation_number"),

        F.col("USNAM").alias("created_by")
    )
)

(
    df_material_usage
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.material_usage")
)
