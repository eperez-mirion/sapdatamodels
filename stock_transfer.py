# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Stock Transfer Orders Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | EKKO  | Purchasing Document Header | One row per STO — provides document type, currency, date, purchasing org/group, company code |
# MAGIC | EKPO  | Purchasing Document Item | One row per STO line — provides material, receiving plant, supplying plant, quantities, and status flags |
# MAGIC | EKET  | Delivery Schedule Lines | One row per schedule line per STO item — provides delivery dates, scheduled qty, and GR qty |
# MAGIC | T001  | Company Codes | Provides local currency (WAERS) via EKKO.BUKRS → T001.BUKRS |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - EKKO is the base (STO header), filtered to document numbers 0003000000–0003999999 (stock transfers).
# MAGIC   Standard Purchase Orders (0004xxxxxx) are handled in `purchase_order.py`.
# MAGIC - EKPO joins on EBELN (inner) to add line-item details.
# MAGIC   EKPO.WERKS is the receiving plant; EKPO.RESWK is the supplying (issuing) plant.
# MAGIC - EKET joins on EBELN + EBELP (left) to expand each STO item into its delivery schedule lines.
# MAGIC   An STO item with no schedule lines produces one row with NULL schedule fields.
# MAGIC - T001 joins on BUKRS (left) to add the local currency of the company code.
# MAGIC - MAKT joins on MATNR (left) to add the material description.
# MAGIC - STOs are internal plant-to-plant transfers and have no external vendor — LFA1 is not joined.
# MAGIC
# MAGIC **Mirrors:** SAP ME2M transaction (filtered to STO document range)
# MAGIC
# MAGIC **Granularity:** One row per schedule line (EKKO + EKPO + EKET)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | purchasing_document_number | EKKO.EBELN | STO document number; leading zeros stripped |
# MAGIC | purchasing_document_item | EKPO.EBELP | STO line item number; leading zeros stripped |
# MAGIC | schedule_line_counter | EKET.ETENR | Delivery schedule line counter within the STO item |
# MAGIC | purchasing_document_type | EKKO.BSART | SAP document type code (e.g. UB = stock transfer order) |
# MAGIC | material_number | EKPO.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | po_unit_of_measure | EKPO.MEINS | Unit of measure for the ordered quantity |
# MAGIC | receiving_plant | EKPO.WERKS | Plant receiving the stock |
# MAGIC | supplying_plant | EKPO.RESWK | Plant issuing / shipping the stock |
# MAGIC | material_group | EKPO.MATKL | Material group / commodity code |
# MAGIC | purchasing_document_date | EKKO.BEDAT | Date the STO was created (YYYYMMDD) |
# MAGIC | item_delivery_date | EKET.EINDT | Requested delivery date for the schedule line (YYYYMMDD) |
# MAGIC | statistics_delivery_date | EKET.SLFDT | Statistical delivery date for reporting (YYYYMMDD) |
# MAGIC | local_currency | T001.WAERS | Local currency of the company code |
# MAGIC | po_quantity | EKPO.MENGE | Total ordered quantity at the STO item level |
# MAGIC | scheduled_quantity | EKET.MENGE | Schedule line quantity |
# MAGIC | gr_quantity | EKET.WEMNG | Goods receipt quantity posted against this schedule line |
# MAGIC | quantity_to_be_delivered | Derived | scheduled_quantity − gr_quantity |
# MAGIC | price_unit | EKPO.PEINH | Quantity basis for the net price |
# MAGIC | net_price | EKPO.NETPR | Net price per price unit |
# MAGIC | total_value | Derived | scheduled_quantity × net_price / price_unit |
# MAGIC | gr_indicator | EKPO.WEPOS | Goods receipt required: X = yes |
# MAGIC | item_category | EKPO.PSTYP | STO item category |
# MAGIC | deletion_indicator | EKPO.LOEKZ | Deletion flag: X = deleted |
# MAGIC | delivery_completed_indicator | EKPO.ELIKZ | Final delivery flag: X = complete |
# MAGIC | sto_status | Derived | Deleted \| Delivery Complete \| Fully Received \| Partially Received \| Open |
# MAGIC | purchasing_group | EKKO.EKGRP | Purchasing group / buyer code |
# MAGIC | storage_location | EKPO.LGORT | Destination storage location in the receiving plant |
# MAGIC | purchasing_organization | EKKO.EKORG | Purchasing organization |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,EKKO: STO Header
stg_ekko = (
    spark.table("median_hub_captured.sap.EKKO")
    .filter(F.col("MANDT") == "400")
    .filter(F.col("EBELN").between("0003000000", "0003999999"))
    .select(
        F.col("EBELN"),
        F.col("BSART"),
        F.col("WAERS").alias("po_currency"),
        F.col("BEDAT"),
        F.col("EKORG"),
        F.col("EKGRP"),
        F.col("BUKRS")
    )
)

# COMMAND ----------

# DBTITLE 1,EKPO: STO Item
stg_ekpo = (
    spark.table("median_hub_captured.sap.EKPO")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("MATNR"),
        F.col("WERKS"),
        F.col("RESWK"),
        F.col("LGORT"),
        F.col("MATKL"),
        F.col("MENGE").alias("po_menge"),
        F.col("MEINS"),
        F.col("NETPR"),
        F.col("PEINH"),
        F.col("WEPOS"),
        F.col("PSTYP"),
        F.col("LOEKZ"),
        F.col("ELIKZ")
    )
)

# COMMAND ----------

# DBTITLE 1,EKET: Delivery Schedule Lines
stg_eket = (
    spark.table("median_hub_captured.sap.EKET")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("ETENR"),
        F.col("EINDT"),
        F.col("SLFDT"),
        F.col("MENGE").alias("sched_menge"),
        F.col("WEMNG")
    )
)

# COMMAND ----------

# DBTITLE 1,Additional Reference Tables
t001 = (
    spark.table("median_hub_captured.sap.T001")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("BUKRS"),
        F.col("WAERS").alias("local_currency")
    )
)

makt = (
    spark.table("median_hub_captured.sap.MAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select("MATNR", "MAKTX")
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_stock_transfer = (
    stg_ekko
    .join(stg_ekpo, on="EBELN",            how="inner")
    .join(stg_eket, on=["EBELN", "EBELP"],  how="left")
    .join(t001,     on="BUKRS",             how="left")
    .join(makt,     on="MATNR",             how="left")
    .select(
        F.when(F.col("EBELN").rlike("^[0-9]+$"), F.regexp_replace(F.col("EBELN"), "^0+", ""))
         .otherwise(F.col("EBELN"))
         .alias("purchasing_document_number"),

        F.when(F.col("EBELP").rlike("^[0-9]+$"), F.regexp_replace(F.col("EBELP"), "^0+", ""))
         .otherwise(F.col("EBELP"))
         .alias("purchasing_document_item"),

        F.col("ETENR").alias("schedule_line_counter"),
        F.col("BSART").alias("purchasing_document_type"),

        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.col("MAKTX").alias("material_description"),
        F.col("MEINS").alias("po_unit_of_measure"),
        F.col("WERKS").alias("receiving_plant"),
        F.col("RESWK").alias("supplying_plant"),
        F.col("MATKL").alias("material_group"),
        F.col("BEDAT").alias("purchasing_document_date"),
        F.col("EINDT").alias("item_delivery_date"),
        F.col("SLFDT").alias("statistics_delivery_date"),
        F.col("local_currency"),
        F.col("po_menge").alias("po_quantity"),
        F.col("sched_menge").alias("scheduled_quantity"),
        F.col("WEMNG").alias("gr_quantity"),
        (F.col("sched_menge") - F.col("WEMNG")).alias("quantity_to_be_delivered"),

        F.col("PEINH").alias("price_unit"),
        F.col("NETPR").alias("net_price"),
        F.round(
            F.col("sched_menge") * F.col("NETPR") / F.when(F.col("PEINH") == 0, None).otherwise(F.col("PEINH")),
            2
        ).alias("total_value"),

        F.col("WEPOS").alias("gr_indicator"),
        F.col("PSTYP").alias("item_category"),
        F.col("LOEKZ").alias("deletion_indicator"),
        F.col("ELIKZ").alias("delivery_completed_indicator"),

        F.when(F.col("LOEKZ") == "X", "Deleted")
         .when(F.col("ELIKZ") == "X", "Delivery Complete")
         .when(F.col("WEMNG") >= F.col("sched_menge"), "Fully Received")
         .when(F.col("WEMNG") > 0, "Partially Received")
         .otherwise("Open")
         .alias("sto_status"),

        F.col("EKGRP").alias("purchasing_group"),
        F.col("LGORT").alias("storage_location"),
        F.col("EKORG").alias("purchasing_organization")
    )
)

(
    df_stock_transfer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.stock_transfer")
)
