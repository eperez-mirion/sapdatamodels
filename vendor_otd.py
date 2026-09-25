# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Vendor On-Time Delivery (OTD)
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | EKET  | Delivery Schedule Lines | Base table — one row per scheduled delivery line per PO item |
# MAGIC | EKKO  | Purchasing Document Header | PO header — provides vendor, currency, dates, purchasing org/group |
# MAGIC | EKPO  | Purchasing Document Item | PO item — provides material, plant, and item-level data |
# MAGIC | EKBE  | PO History | Goods receipt events — cumulative-matched to schedule lines for per-line OTD scoring |
# MAGIC | LFA1  | Vendor Master | Provides vendor name (NAME1) via EKKO.LIFNR → LFA1.LIFNR |
# MAGIC | MAKT  | Material Descriptions | Provides material short text (MAKTX) in English (SPRAS = E) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Schedule Lines Are Matched to Goods Receipts
# MAGIC
# MAGIC ### The Challenge
# MAGIC SAP stores delivery schedule lines in EKET and goods receipt events in EKBE. In a standard
# MAGIC SAP system, EKBE.ETENS (the schedule line counter) links each GR event back to the specific
# MAGIC schedule line it was received against. In this extraction, ETENS is unpopulated (all records
# MAGIC show 0000), so a direct line-to-line link is not available.
# MAGIC
# MAGIC A simple approach — rank schedule lines by due date and rank GR events by posting date, then
# MAGIC match on rank — works only when each schedule line receives exactly one GR. When a vendor
# MAGIC makes partial deliveries (multiple GRs fulfilling a single schedule line), the ranks shift out
# MAGIC of alignment and subsequent schedule lines are matched to the wrong GR events.
# MAGIC
# MAGIC **Example of rank mismatch with partial deliveries:**
# MAGIC
# MAGIC | Schedule Line | Qty | Qty Received By |
# MAGIC |---------------|-----|-----------------|
# MAGIC | Sch 1 | 10 | GR 1 (qty 10) — one delivery, clean |
# MAGIC | Sch 2 | 10 | GR 2 (qty 2) + GR 3 (qty 8) — two partial deliveries |
# MAGIC | Sch 3 | 10 | GR 4 (qty 10) — one delivery, clean |
# MAGIC | Sch 4 | 10 | GR 5 (qty 6) + GR 6 (qty 4) — two partial deliveries |
# MAGIC
# MAGIC With rank-based matching: Sch 2 → GR 2 (partial only), Sch 3 → GR 3 (actually Sch 2's
# MAGIC completion), Sch 4 → GR 4 (actually Sch 3's GR). Ranks 5 and 6 are dropped.
# MAGIC
# MAGIC ### The Solution: Cumulative Quantity Matching
# MAGIC Instead of matching by rank, this notebook uses a **cumulative quantity threshold** approach:
# MAGIC
# MAGIC 1. **EKET schedule lines** are sorted by due date ascending (EINDT, with ETENR as tie-breaker)
# MAGIC    and assigned a running total of scheduled quantities: `cum_sched_qty`. This represents the
# MAGIC    total quantity that should have been received through each schedule line in sequence.
# MAGIC
# MAGIC 2. **EKBE GR events** are filtered to positive receipts only (BEWTP = 'E', SHKZG = 'S' —
# MAGIC    reversals excluded), sorted by posting date ascending (BUDAT, with BELNR as tie-breaker),
# MAGIC    and assigned a running total of received quantities: `cum_gr_qty`. This builds the
# MAGIC    cumulative delivery history in chronological order.
# MAGIC
# MAGIC 3. **Each schedule line is matched** to the GR event that first pushed `cum_gr_qty` to or
# MAGIC    above its `cum_sched_qty` threshold. This GR is the **completion event** for that schedule
# MAGIC    line — the delivery that fulfilled it. The join is non-equi:
# MAGIC    `cum_gr_qty >= cum_sched_qty` (within the same EBELN + EBELP).
# MAGIC
# MAGIC 4. **After the join**, multiple GR rows may satisfy the condition for a single schedule line
# MAGIC    (every GR after the threshold-crossing one also satisfies it). A deduplication window
# MAGIC    keeps only the first — the GR with the lowest posting-date rank, which is the actual
# MAGIC    completion event.
# MAGIC
# MAGIC **Same example, resolved correctly:**
# MAGIC
# MAGIC | Schedule | cum_sched_qty | Completing GR | cum_gr_qty | gr_date |
# MAGIC |----------|--------------|--------------|------------|---------|
# MAGIC | Sch 1 | 10 | GR 1 | 10 | GR 1 posting date |
# MAGIC | Sch 2 | 20 | GR 3 | 20 | GR 3 posting date (the completion, not GR 2 the partial) |
# MAGIC | Sch 3 | 30 | GR 4 | 30 | GR 4 posting date |
# MAGIC | Sch 4 | 40 | GR 6 | 40 | GR 6 posting date |
# MAGIC
# MAGIC ### OTD Scoring
# MAGIC - A schedule line is scored **On Time** if its completion GR date (gr_date) is on or before
# MAGIC   its scheduled delivery date (EINDT).
# MAGIC - A schedule line is scored **Late** if its completion GR date is after its scheduled date.
# MAGIC - A schedule line shows **Open** if no GR has yet pushed cumulative qty to its threshold and
# MAGIC   the scheduled date has not yet passed.
# MAGIC - A schedule line shows **Overdue** if no GR has yet pushed cumulative qty to its threshold
# MAGIC   and the scheduled date has already passed.
# MAGIC - Partial deliveries (vendor has delivered some quantity but not yet completed the line) are
# MAGIC   treated as undelivered — gr_date is NULL until the line is fully realized. This reflects
# MAGIC   the business rule that on-time delivery requires the full committed quantity.
# MAGIC
# MAGIC ### Known Limitations
# MAGIC - Cumulative matching assumes all GR quantities flow into the PO item's schedule lines in
# MAGIC   posting-date order. If a vendor posts GRs out of chronological order (e.g. a backdated
# MAGIC   receipt), the matching order reflects the BUDAT sequence, not the physical delivery sequence.
# MAGIC - If cumulative GR quantity exceeds cumulative scheduled quantity (over-delivery), the extra
# MAGIC   quantity is absorbed without additional schedule line rows. EKET.WEMNG (SAP-maintained GR
# MAGIC   qty per schedule line) remains accurate regardless.
# MAGIC - EKBE.ETENS is confirmed unpopulated in this extraction; if future extractions populate it,
# MAGIC   direct line-to-line matching should replace this approach.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Granularity:** One row per PO schedule line (EKET). Standard POs only (0004000000–0004999999); STOs excluded.
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
# MAGIC | gr_quantity | EKET.WEMNG | Goods receipt quantity for this schedule line as maintained by SAP |
# MAGIC | open_quantity | Derived | scheduled_quantity − gr_quantity |
# MAGIC | gr_date | EKBE.BUDAT | Posting date of the completing GR for this schedule line (YYYYMMDD). The completing GR is the first GR event where cumulative received qty reached or exceeded the cumulative scheduled qty threshold. NULL if the line has not yet been fully received |
# MAGIC | gr_document_number | EKBE.BELNR | Accounting document number of the completing GR event; NULL if not yet fully received |
# MAGIC | delivered_on_time | Derived | True = gr_date on or before scheduled date; False = late; NULL = not yet fully received |
# MAGIC | days_early_late | Derived | scheduled_delivery_date − gr_date in days. Positive = early, negative = late, NULL = open |
# MAGIC | otd_status | Derived | On Time \| Late \| Overdue \| Open |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

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

# DBTITLE 1,EKET: Schedule Lines with Cumulative Scheduled Quantity
# Schedule lines are ordered by due date ascending (EINDT, ETENR as tie-breaker) and
# assigned a running cumulative total of scheduled quantities. cum_sched_qty represents
# the total quantity that should have been received through each schedule line in sequence.
eket_window = Window.partitionBy("EBELN", "EBELP").orderBy("EINDT", "ETENR")
eket_cum_window = eket_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        F.col("WEMNG").alias("gr_qty_wemng")
    )
    .withColumn("sched_rank", F.row_number().over(eket_window))
    .withColumn("cum_sched_qty", F.sum("sched_menge").over(eket_cum_window))
)

# COMMAND ----------

# DBTITLE 1,EKBE: Positive GR Events with Cumulative Received Quantity
# GR events are filtered to positive receipts only: BEWTP = 'E' (goods receipt category)
# and SHKZG = 'S' (stock increase). Reversals (SHKZG = 'H') are excluded so they do not
# inflate the running total and cause premature threshold crossing.
#
# GRs are ordered by posting date ascending (BUDAT, BELNR as tie-breaker) and assigned a
# running cumulative total. cum_gr_qty represents the total quantity physically received
# through each GR event in chronological order.
#
# EBELN and EBELP are renamed to avoid column ambiguity in the subsequent non-equi join.
ekbe_window = Window.partitionBy("EBELN", "EBELP").orderBy("BUDAT", "BELNR")
ekbe_cum_window = ekbe_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

stg_ekbe = (
    spark.table("median_hub_captured.sap.EKBE")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("BEWTP") == "E") &
        (F.col("SHKZG") == "S")
    )
    .select(
        F.col("EBELN").alias("gr_EBELN"),
        F.col("EBELP").alias("gr_EBELP"),
        F.col("BUDAT"),
        F.col("BELNR"),
        F.col("MENGE").alias("gr_menge")
    )
    .withColumn("gr_rank", F.row_number().over(ekbe_window))
    .withColumn("cum_gr_qty", F.sum("gr_menge").over(ekbe_cum_window))
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

# Non-equi join: for each schedule line, match all EKBE rows where the cumulative GR
# quantity has reached or exceeded the schedule line's cumulative scheduled quantity
# threshold. Left join so schedule lines with no qualifying GR produce a NULL gr_date.
join_condition = (
    (F.col("EBELN")       == F.col("gr_EBELN")) &
    (F.col("EBELP")       == F.col("gr_EBELP")) &
    (F.col("cum_gr_qty")  >= F.col("cum_sched_qty"))
)

# After the non-equi join, every EKBE row at or above the threshold satisfies the condition
# for a given schedule line — not just the one that first crossed it. This dedup window
# orders matched GR rows by gr_rank ascending and keeps only the first: the GR event that
# actually pushed the cumulative over the threshold (the completion event).
completion_window = Window.partitionBy("EBELN", "EBELP", "sched_rank").orderBy(
    F.col("gr_rank").asc_nulls_last()
)

df_vendor_otd = (
    stg_eket
    .join(stg_ekpo, on=["EBELN", "EBELP"], how="inner")
    .join(stg_ekko, on="EBELN",            how="inner")
    .join(stg_ekbe, join_condition,         how="left")
    .withColumn("completion_rank", F.row_number().over(completion_window))
    .filter(F.col("completion_rank") == 1)
    .join(lfa1, on="LIFNR", how="left")
    .join(makt, on="MATNR", how="left")
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
        F.col("gr_qty_wemng").alias("gr_quantity"),
        (F.col("sched_menge") - F.col("gr_qty_wemng")).alias("open_quantity"),
        F.col("BUDAT").alias("gr_date"),
        F.col("BELNR").alias("gr_document_number"),

        F.when(F.col("BUDAT").isNull(), F.lit(None).cast("boolean"))
         .when(F.col("BUDAT") <= F.col("EINDT"), F.lit(True))
         .otherwise(F.lit(False))
         .alias("delivered_on_time"),

        F.when(
            F.col("BUDAT").isNotNull() & F.col("EINDT").isNotNull(),
            F.datediff(
                F.to_date(F.col("EINDT"), "yyyyMMdd"),
                F.to_date(F.col("BUDAT"), "yyyyMMdd")
            )
        ).alias("days_early_late"),

        F.when(F.col("BUDAT").isNull(),
            F.when(F.col("EINDT") < today, "Overdue")
             .otherwise("Open")
         )
         .when(F.col("BUDAT") <= F.col("EINDT"), "On Time")
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
