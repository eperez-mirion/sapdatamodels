# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Vendor On-Time Delivery (OTD)
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | EKET  | Delivery Schedule Lines | Base table — one row per schedule line per PO item |
# MAGIC | EKKO  | Purchasing Document Header | PO header — provides vendor, currency, dates, purchasing org/group |
# MAGIC | EKPO  | Purchasing Document Item | PO item — provides material, plant, quantities |
# MAGIC | EKBE  | PO History | Goods receipt history — aggregated per PO item to derive GR dates and quantities |
# MAGIC | LFA1  | Vendor Master | Provides vendor name (NAME1) via EKKO.LIFNR → LFA1.LIFNR |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - EKET is the base, giving one row per delivery schedule line.
# MAGIC - EKPO joins on EBELN + EBELP (inner) to add material, plant, and item quantities.
# MAGIC - EKKO joins on EBELN (inner), filtered to standard POs (0004000000–0004999999).
# MAGIC   Stock Transfer Orders are excluded — vendor OTD is not applicable to internal transfers.
# MAGIC - EKBE is pre-aggregated to one row per PO item before joining:
# MAGIC     - Filtered to BEWTP = 'E' (goods receipts, including reversals and returns).
# MAGIC     - Quantity is signed: S (debit/receipt) = +MENGE, H (credit/reversal) = -MENGE.
# MAGIC     - Aggregations: total_gr_qty (net), first_gr_date (earliest receipt), last_gr_date (latest receipt).
# MAGIC   Joins on EBELN + EBELP (left) so schedule lines with no GR yet are retained.
# MAGIC - LFA1 joins on LIFNR (left) to add the vendor name.
# MAGIC - MAKT joins on MATNR (left) to add the material description.
# MAGIC
# MAGIC **OTD logic:**
# MAGIC - delivered_on_time: True if first_gr_date <= scheduled_delivery_date; False if late; NULL if no GR.
# MAGIC - days_early_late: scheduled_delivery_date − first_gr_date in calendar days.
# MAGIC   Positive = delivered early, negative = delivered late, NULL = not yet received.
# MAGIC - otd_status: On Time | Late | Overdue (no GR, date passed) | Open (no GR, date in future)
# MAGIC
# MAGIC **Granularity:** One row per PO schedule line (EKET)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | purchasing_document_number | EKKO.EBELN | Purchase order number; leading zeros stripped |
# MAGIC | purchasing_document_item | EKPO.EBELP | PO line item number; leading zeros stripped |
# MAGIC | schedule_line_counter | EKET.ETENR | Delivery schedule line counter within the PO item |
# MAGIC | purchasing_document_type | EKKO.BSART | SAP document type code (e.g. NB = standard PO) |
# MAGIC | vendor_account_number | EKKO.LIFNR | SAP vendor account number; leading zeros stripped |
# MAGIC | vendor_name | LFA1.NAME1 | Vendor name from vendor master |
# MAGIC | material_number | EKPO.MATNR | SAP material number; leading zeros stripped |
# MAGIC | material_description | MAKT.MAKTX | Material short text in English |
# MAGIC | material_group | EKPO.MATKL | Material group / commodity code |
# MAGIC | plant | EKPO.WERKS | Receiving plant |
# MAGIC | purchasing_group | EKKO.EKGRP | Purchasing group / buyer code |
# MAGIC | purchasing_organization | EKKO.EKORG | Purchasing organization |
# MAGIC | purchasing_document_date | EKKO.BEDAT | Date the PO was created (YYYYMMDD) |
# MAGIC | scheduled_delivery_date | EKET.EINDT | Requested delivery date for this schedule line (YYYYMMDD) |
# MAGIC | statistics_delivery_date | EKET.SLFDT | Statistical delivery date for reporting (YYYYMMDD) |
# MAGIC | scheduled_quantity | EKET.MENGE | Quantity expected on this schedule line |
# MAGIC | total_gr_qty | EKBE derived | Net goods receipt quantity (receipts minus reversals and returns) |
# MAGIC | open_quantity | Derived | scheduled_quantity − total_gr_qty |
# MAGIC | first_gr_date | EKBE derived | Date of the first goods receipt posted against this PO item (YYYYMMDD) |
# MAGIC | last_gr_date | EKBE derived | Date of the most recent goods receipt posted against this PO item (YYYYMMDD) |
# MAGIC | gr_document_count | EKBE derived | Number of distinct GR documents posted against this PO item |
# MAGIC | delivered_on_time | Derived | True = first GR on or before scheduled date; False = late; NULL = not yet received |
# MAGIC | days_early_late | Derived | scheduled_delivery_date − first_gr_date in days. Positive = early, negative = late, NULL = open |
# MAGIC | otd_status | Derived | On Time \| Late \| Overdue \| Open |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,EKKO: PO Header
stg_ekko = (
    spark.table("median_hub_captured.sap.EKKO")
    .filter(F.col("MANDT") == "400")
    .filter(F.col("EBELN").between("0004000000", "0004999999"))
    .select(
        F.col("EBELN"),
        F.col("BSART"),
        F.col("LIFNR"),
        F.col("BEDAT"),
        F.col("EKORG"),
        F.col("EKGRP")
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
        F.col("MATKL")
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
        F.col("MENGE").alias("sched_menge")
    )
)

# COMMAND ----------

# DBTITLE 1,EKBE: GR History (aggregated per PO item)
stg_ekbe_gr = (
    spark.table("median_hub_captured.sap.EKBE")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("BEWTP") == "E")
    )
    .withColumn(
        "signed_qty",
        F.when(F.col("SHKZG") == "H", -F.col("MENGE"))
         .otherwise(F.col("MENGE"))
    )
    .groupBy("EBELN", "EBELP")
    .agg(
        F.sum("signed_qty").alias("total_gr_qty"),
        F.min(F.when(F.col("SHKZG") == "S", F.col("BUDAT"))).alias("first_gr_date"),
        F.max(F.when(F.col("SHKZG") == "S", F.col("BUDAT"))).alias("last_gr_date"),
        F.countDistinct("BELNR").alias("gr_document_count")
    )
)

# COMMAND ----------

# DBTITLE 1,Reference Tables
lfa1 = (
    spark.table("median_hub_captured.sap.LFA1")
    .filter(F.col("MANDT") == "400")
    .select("LIFNR", "NAME1")
)

makt = (
    spark.table("median_hub_captured.sap.MAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select("MATNR", "MAKTX")
)

# COMMAND ----------

# DBTITLE 1,Final Table
today = F.date_format(F.current_date(), "yyyyMMdd")

df_vendor_otd = (
    stg_eket
    .join(stg_ekpo, on=["EBELN", "EBELP"],  how="inner")
    .join(stg_ekko, on="EBELN",             how="inner")
    .join(stg_ekbe_gr, on=["EBELN", "EBELP"], how="left")
    .join(lfa1,     on="LIFNR",             how="left")
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

        F.col("MATKL").alias("material_group"),
        F.col("WERKS").alias("plant"),
        F.col("EKGRP").alias("purchasing_group"),
        F.col("EKORG").alias("purchasing_organization"),
        F.col("BEDAT").alias("purchasing_document_date"),
        F.col("EINDT").alias("scheduled_delivery_date"),
        F.col("SLFDT").alias("statistics_delivery_date"),
        F.col("sched_menge").alias("scheduled_quantity"),
        F.coalesce(F.col("total_gr_qty"), F.lit(0)).alias("total_gr_qty"),
        (F.col("sched_menge") - F.coalesce(F.col("total_gr_qty"), F.lit(0))).alias("open_quantity"),
        F.col("first_gr_date"),
        F.col("last_gr_date"),
        F.coalesce(F.col("gr_document_count"), F.lit(0)).alias("gr_document_count"),

        F.when(F.col("first_gr_date").isNull(), F.lit(None).cast("boolean"))
         .when(F.col("first_gr_date") <= F.col("EINDT"), F.lit(True))
         .otherwise(F.lit(False))
         .alias("delivered_on_time"),

        F.when(
            F.col("first_gr_date").isNotNull() & F.col("EINDT").isNotNull(),
            F.datediff(
                F.to_date(F.col("EINDT"), "yyyyMMdd"),
                F.to_date(F.col("first_gr_date"), "yyyyMMdd")
            )
        ).alias("days_early_late"),

        F.when(F.col("first_gr_date").isNull(),
            F.when(F.col("EINDT") < today, "Overdue")
             .otherwise("Open")
         )
         .when(F.col("first_gr_date") <= F.col("EINDT"), "On Time")
         .otherwise("Late")
         .alias("otd_status")
    )
)

(
    df_vendor_otd
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.vendor_otd")
)
