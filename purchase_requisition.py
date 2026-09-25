# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Purchase Requisitions Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | EBAN  | Purchase Requisition Item | Base table — one row per PR item; provides all requisition attributes |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC | EKPO  | Purchasing Document Item | Provides the net price from the linked purchase order (NETPR) |
# MAGIC | EKET  | Delivery Schedule Lines | Provides total goods receipt quantity (sum of WEMNG) per PO item |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - EBAN is the base table. Each row is one PR item identified by BANFN + BNFPO.
# MAGIC - MAKT joins on MATNR (left) for the material description. If the PR item has no MATNR
# MAGIC   (free-text description only), EBAN.TXZ01 is used as the description fallback.
# MAGIC - EKPO joins on EBELN + EBELP (left) to bring the net price from the linked PO item.
# MAGIC - EKET is aggregated to one row per EBELN + EBELP (sum of WEMNG) then joined left
# MAGIC   to provide the total GR quantity received against the linked PO item.
# MAGIC - All-numeric values in BANFN, MATNR, EBELN, and LIFNR have leading zeros stripped.
# MAGIC
# MAGIC **Mirrors:** SAP ME5A transaction
# MAGIC
# MAGIC **Granularity:** One row per PR item (EBAN)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | purchase_requisition_number | EBAN.BANFN | PR number; leading zeros stripped |
# MAGIC | purchase_requisition_item | EBAN.BNFPO | Line item number within the PR |
# MAGIC | material_number | EBAN.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX / EBAN.TXZ01 | Material short text; falls back to free-text if no MATNR |
# MAGIC | purchasing_group | EBAN.EKGRP | Purchasing group / buyer code |
# MAGIC | material_group | EBAN.MATKL | Material group / commodity code |
# MAGIC | processing_status | EBAN.STATU | PR processing status (N = not processed, B = PO created) |
# MAGIC | release_status | EBAN.FRGST | Overall release / approval status |
# MAGIC | release_indicator | EBAN.FRGKZ | Which approval levels have been granted |
# MAGIC | purchasing_document_number | EBAN.EBELN | Linked PO number; leading zeros stripped |
# MAGIC | vendor_account_number | EBAN.LIFNR | Preferred or assigned vendor; leading zeros stripped |
# MAGIC | pr_closed | EBAN.EBAKZ | X = PR item is closed |
# MAGIC | plant | EBAN.WERKS | Plant where the material is required |
# MAGIC | requisition_date | EBAN.BADAT | Date the PR item was created (YYYYMMDD) |
# MAGIC | release_date | EBAN.FRGDT | Date the PR was fully approved (YYYYMMDD) |
# MAGIC | item_delivery_date | EBAN.LFDAT | Requested delivery date (YYYYMMDD) |
# MAGIC | unit_of_measure | EBAN.MEINS | Base unit of measure for the requisition quantity |
# MAGIC | currency | EBAN.WAERS | Currency of the price on the PR item |
# MAGIC | planned_delivery_time_days | EBAN.PLIFZ | Planned delivery time in calendar days |
# MAGIC | pr_quantity | EBAN.MENGE | Requested quantity |
# MAGIC | pr_price | EBAN.PREIS | Estimated price per price unit |
# MAGIC | price_unit | EBAN.PEINH | Quantity basis for pr_price |
# MAGIC | committed_quantity | EBAN.MNG02 | Quantity already committed / reserved in planning |
# MAGIC | ordered_quantity | EBAN.BSMNG | Quantity already converted to a purchase order |
# MAGIC | net_price_po | EKPO.NETPR | Net price from the linked PO item |
# MAGIC | gr_quantity | Derived | Total GR quantity posted against the linked PO item (sum of EKET.WEMNG) |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,EBAN: Purchase Requisition Items
stg_eban = (
    spark.table("median_hub_captured.sap.EBAN")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("BANFN"),
        F.col("BNFPO"),
        F.col("MATNR"),
        F.col("TXZ01"),
        F.col("EKGRP"),
        F.col("MATKL"),
        F.col("STATU"),
        F.col("FRGST"),
        F.col("FRGKZ"),
        F.col("EBAKZ"),
        F.col("WERKS"),
        F.col("MEINS"),
        F.col("WAERS"),
        F.col("PLIFZ"),
        F.col("MENGE"),
        F.col("PREIS"),
        F.col("PEINH"),
        F.col("BADAT"),
        F.col("LFDAT"),
        F.col("FRGDT"),
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("LIFNR"),
        F.col("BSMNG"),
        F.col("MNG02")
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

# DBTITLE 1,EKPO and EKET: PO Linkage
ekpo = (
    spark.table("median_hub_captured.sap.EKPO")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("EBELN"),
        F.col("EBELP"),
        F.col("NETPR")
    )
)

eket = (
    spark.table("median_hub_captured.sap.EKET")
    .filter(F.col("MANDT") == "400")
    .groupBy("EBELN", "EBELP")
    .agg(F.sum("WEMNG").alias("gr_qty"))
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_purchase_requisition = (
    stg_eban
    .join(makt, on="MATNR",            how="left")
    .join(ekpo, on=["EBELN", "EBELP"],  how="left")
    .join(eket, on=["EBELN", "EBELP"],  how="left")
    .select(
        F.when(F.col("BANFN").rlike("^[0-9]+$"), F.regexp_replace(F.col("BANFN"), "^0+", ""))
         .otherwise(F.col("BANFN"))
         .alias("purchase_requisition_number"),

        F.col("BNFPO").alias("purchase_requisition_item"),

        F.when(F.col("MATNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("MATNR"), "^0+", ""))
         .otherwise(F.col("MATNR"))
         .alias("material_number"),

        F.when(F.col("MATNR").isNull() | (F.col("MATNR") == ""), F.col("TXZ01"))
         .otherwise(F.col("MAKTX"))
         .alias("material_description"),

        F.col("EKGRP").alias("purchasing_group"),
        F.col("MATKL").alias("material_group"),
        F.col("STATU").alias("processing_status"),
        F.col("FRGST").alias("release_status"),
        F.col("FRGKZ").alias("release_indicator"),

        F.when(F.col("EBELN").rlike("^[0-9]+$"), F.regexp_replace(F.col("EBELN"), "^0+", ""))
         .otherwise(F.col("EBELN"))
         .alias("purchasing_document_number"),

        F.when(F.col("LIFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("LIFNR"), "^0+", ""))
         .otherwise(F.col("LIFNR"))
         .alias("vendor_account_number"),

        F.col("EBAKZ").alias("pr_closed"),
        F.col("WERKS").alias("plant"),
        F.col("BADAT").alias("requisition_date"),
        F.col("FRGDT").alias("release_date"),
        F.col("LFDAT").alias("item_delivery_date"),
        F.col("MEINS").alias("unit_of_measure"),
        F.col("WAERS").alias("currency"),
        F.col("PLIFZ").alias("planned_delivery_time_days"),
        F.col("MENGE").alias("pr_quantity"),
        F.col("PREIS").alias("pr_price"),
        F.col("PEINH").alias("price_unit"),
        F.col("MNG02").alias("committed_quantity"),
        F.col("BSMNG").alias("ordered_quantity"),
        F.col("NETPR").alias("net_price_po"),
        F.col("gr_qty").alias("gr_quantity")
    )
)

(
    df_purchase_requisition
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.purchase_requisition")
)
