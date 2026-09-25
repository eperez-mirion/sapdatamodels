# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Purchase Documents Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | EKKO  | Purchasing Document Header | One row per PO — provides vendor, currency, date, purchasing org/group, company code |
# MAGIC | EKPO  | Purchasing Document Item | One row per PO line — provides material, plant, quantities, pricing, and status flags |
# MAGIC | EKET  | Delivery Schedule Lines | One row per schedule line per PO item — provides delivery dates, scheduled qty, and GR qty |
# MAGIC | LFA1  | Vendor Master | Provides vendor name (NAME1) via EKKO.LIFNR → LFA1.LIFNR |
# MAGIC | T001  | Company Codes | Provides local currency (WAERS) via EKKO.BUKRS → T001.BUKRS |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - EKKO is the base (PO header). EKPO joins on EBELN (inner) to add line-item details.
# MAGIC - EKET joins on EBELN + EBELP (left) to expand each PO item into its delivery schedule lines.
# MAGIC   A PO item with no schedule lines produces one row with NULL schedule fields.
# MAGIC - LFA1 joins on LIFNR (left) to add the vendor name.
# MAGIC - T001 joins on BUKRS (left) to add the local currency of the company code.
# MAGIC - MAKT joins on MATNR (left) to add the material description. If the PO item has no MATNR
# MAGIC   (free-text description only), EKPO.TXZ01 is used as the description fallback.
# MAGIC - Includes both standard Purchase Orders and Stock Transfer Orders (STO).
# MAGIC - PO text block is excluded — requires STXH/STXL aggregation not yet available.
# MAGIC
# MAGIC **Mirrors:** SAP ME2M transaction with additions
# MAGIC
# MAGIC **Granularity:** One row per schedule line (EKKO + EKPO + EKET)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | purchasing_document_number | EKKO.EBELN | Purchase order number; leading zeros stripped |
# MAGIC | purchasing_document_item | EKPO.EBELP | PO line item number; leading zeros stripped |
# MAGIC | schedule_line_counter | EKET.ETENR | Delivery schedule line counter within the PO item |
# MAGIC | vendor_account_number | EKKO.LIFNR | SAP vendor account number; leading zeros stripped |
# MAGIC | vendor_name | LFA1.NAME1 | Vendor name from vendor master |
# MAGIC | material_number | EKPO.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX / EKPO.TXZ01 | Material short text; falls back to free-text description if no MATNR |
# MAGIC | po_unit_of_measure | EKPO.MEINS | Unit of measure for the ordered quantity |
# MAGIC | po_currency | EKKO.WAERS | Currency of the purchase order |
# MAGIC | plant | EKPO.WERKS | Receiving plant |
# MAGIC | material_group | EKPO.MATKL | Material group / commodity code |
# MAGIC | purchasing_document_date | EKKO.BEDAT | Date the purchase order was created (YYYYMMDD) |
# MAGIC | item_delivery_date | EKET.EINDT | Requested delivery date for the schedule line (YYYYMMDD) |
# MAGIC | statistics_delivery_date | EKET.SLFDT | Statistical delivery date for reporting (YYYYMMDD) |
# MAGIC | local_currency | T001.WAERS | Local currency of the company code |
# MAGIC | po_quantity | EKPO.MENGE | Total ordered quantity at the PO item level |
# MAGIC | scheduled_quantity | EKET.MENGE | Quantity for this specific schedule line |
# MAGIC | gr_quantity | EKET.WEMNG | Goods receipt quantity posted against this schedule line |
# MAGIC | quantity_to_be_delivered | Derived | scheduled_quantity − gr_quantity |
# MAGIC | purchase_requisition_number | EKET.BANFN | Originating PR number; leading zeros stripped |
# MAGIC | price_unit | EKPO.PEINH | Quantity basis for the net price |
# MAGIC | net_price | EKPO.NETPR | Net price per price unit in PO currency |
# MAGIC | total_value | Derived | scheduled_quantity × net_price / price_unit |
# MAGIC | gr_indicator | EKPO.WEPOS | Goods receipt required: X = yes |
# MAGIC | invoice_receipt_indicator | EKPO.REPOS | Invoice receipt required: X = yes |
# MAGIC | item_category | EKPO.PSTYP | PO item category (blank = standard, D = service, K = consignment) |
# MAGIC | account_assignment_category | EKPO.KNTTP | Cost object type (blank = stock, K = cost center, F = order, P = WBS) |
# MAGIC | deletion_indicator | EKPO.LOEKZ | Deletion flag: X = deleted |
# MAGIC | delivery_completed_indicator | EKPO.ELIKZ | Final delivery flag: X = complete |
# MAGIC | po_status | Derived | Deleted \| Delivery Complete \| Fully Received \| Partially Received \| Open |
# MAGIC | purchasing_group | EKKO.EKGRP | Purchasing group / buyer code |
# MAGIC | storage_location | EKPO.LGORT | Destination storage location within the plant |
# MAGIC | purchasing_organization | EKKO.EKORG | Purchasing organization |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,EKKO: PO Header
stg_ekko = (
    spark.table("median_hub_captured.sap.EKKO")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EBELN"),
        F.col("LIFNR"),
        F.col("WAERS").alias("po_currency"),
        F.col("BEDAT"),
        F.col("EKORG"),
        F.col("EKGRP"),
        F.col("BUKRS")
    )
)

# COMMAND ----------

# DBTITLE 1,EKPO: PO Item
stg_ekpo = (
    spark.table("median_hub_captured.sap.EKPO")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("MATNR"),
        F.col("TXZ01"),
        F.col("WERKS"),
        F.col("LGORT"),
        F.col("MATKL"),
        F.col("MENGE").alias("po_menge"),
        F.col("MEINS"),
        F.col("NETPR"),
        F.col("PEINH"),
        F.col("NETWR"),
        F.col("WEPOS"),
        F.col("REPOS"),
        F.col("PSTYP"),
        F.col("KNTTP"),
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
        F.col("WEMNG"),
        F.col("BANFN")
    )
)

# COMMAND ----------

# DBTITLE 1,Additional Reference Tables
lfa1 = (
    spark.table("median_hub_captured.sap.LFA1")
    .filter(F.col("MANDT") == "400")
    .select("LIFNR", "NAME1")
)

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
df_purchase_order = (
    stg_ekko
    .join(stg_ekpo, on="EBELN",            how="inner")
    .join(stg_eket, on=["EBELN", "EBELP"],  how="left")
    .join(lfa1,     on="LIFNR",             how="left")
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

        F.when(F.col("LIFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("LIFNR"), "^0+", ""))
         .otherwise(F.col("LIFNR"))
         .alias("vendor_account_number"),

        F.col("NAME1").alias("vendor_name"),

        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.when(F.col("MATNR").isNull() | (F.col("MATNR") == ""), F.col("TXZ01"))
         .otherwise(F.col("MAKTX"))
         .alias("material_description"),

        F.col("MEINS").alias("po_unit_of_measure"),
        F.col("po_currency"),
        F.col("WERKS").alias("plant"),
        F.col("MATKL").alias("material_group"),
        F.col("BEDAT").alias("purchasing_document_date"),
        F.col("EINDT").alias("item_delivery_date"),
        F.col("SLFDT").alias("statistics_delivery_date"),
        F.col("local_currency"),
        F.col("po_menge").alias("po_quantity"),
        F.col("sched_menge").alias("scheduled_quantity"),
        F.col("WEMNG").alias("gr_quantity"),
        (F.col("sched_menge") - F.col("WEMNG")).alias("quantity_to_be_delivered"),

        F.when(F.col("BANFN").rlike("^[0-9]+$"), F.regexp_replace(F.col("BANFN"), "^0+", ""))
         .otherwise(F.col("BANFN"))
         .alias("purchase_requisition_number"),

        F.col("PEINH").alias("price_unit"),
        F.col("NETPR").alias("net_price"),
        F.round(
            F.col("sched_menge") * F.col("NETPR") / F.when(F.col("PEINH") == 0, None).otherwise(F.col("PEINH")),
            2
        ).alias("total_value"),

        F.col("WEPOS").alias("gr_indicator"),
        F.col("REPOS").alias("invoice_receipt_indicator"),
        F.col("PSTYP").alias("item_category"),
        F.col("KNTTP").alias("account_assignment_category"),
        F.col("LOEKZ").alias("deletion_indicator"),
        F.col("ELIKZ").alias("delivery_completed_indicator"),

        F.when(F.col("LOEKZ") == "X", "Deleted")
         .when(F.col("ELIKZ") == "X", "Delivery Complete")
         .when(F.col("WEMNG") >= F.col("sched_menge"), "Fully Received")
         .when(F.col("WEMNG") > 0, "Partially Received")
         .otherwise("Open")
         .alias("po_status"),

        F.col("EKGRP").alias("purchasing_group"),
        F.col("LGORT").alias("storage_location"),
        F.col("EKORG").alias("purchasing_organization")

        # TODO: add line_creation_date — EKPO.CREATIONDATE is unpopulated in current extraction.
        # Options: EKPO.AEDAT (last changed date) or MIN(CDHDR.UDATE) per EBELN/EBELP for true creation date.
    )
)

(
    df_purchase_order
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.purchase_order")
)
