# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Vendor Master
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | LFA1  | General Vendor Master | Base table — one row per vendor with address and general data |
# MAGIC | ADR6  | Email Addresses | Provides SMTP email address via LFA1.ADRNR → ADR6.ADDRNUMBER |
# MAGIC | LFM1  | Vendor Master: Purchasing Organization | Provides payment terms, order currency, and incoterms per purchasing org |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - LFA1 is the base table. One row per vendor account.
# MAGIC - ADR6 joins on LFA1.ADRNR = ADR6.ADDRNUMBER (left). ADR6 is deduplicated to one email
# MAGIC   per address using a window function: the default email (FLGDEFAULT = X) is preferred;
# MAGIC   where multiple defaults or no default exists the lowest SMTP_ADDR is taken.
# MAGIC   ADR6 uses CLIENT rather than MANDT for the client filter.
# MAGIC - LFM1 joins on LIFNR (left). Each purchasing organization a vendor is set up in produces
# MAGIC   a separate row. Vendors not set up in any purchasing org appear once with NULL purchasing
# MAGIC   org fields. This reflects the fact that each site maintains its own vendor records.
# MAGIC
# MAGIC **Granularity:** One row per vendor + purchasing organization (LFA1 × LFM1)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | vendor_account_number | LFA1.LIFNR | SAP vendor account number; leading zeros stripped |
# MAGIC | vendor_name | LFA1.NAME1 | Primary vendor name |
# MAGIC | vendor_name_2 | LFA1.NAME2 | Secondary name line (continuation or trade name) |
# MAGIC | account_group | LFA1.KTOKK | Vendor account group controlling field selection and number range |
# MAGIC | industry | LFA1.BRSCH | Industry sector / industry key |
# MAGIC | country | LFA1.LAND1 | Country key |
# MAGIC | region | LFA1.REGIO | Region / state within the country |
# MAGIC | city | LFA1.ORT01 | City |
# MAGIC | postal_code | LFA1.PSTLZ | Postal / ZIP code |
# MAGIC | street_address | LFA1.STRAS | Street name and house number |
# MAGIC | telephone | LFA1.TELF1 | Primary telephone number |
# MAGIC | fax | LFA1.TELFX | Fax number |
# MAGIC | tax_number_1 | LFA1.STCD1 | Tax number 1 (country-specific; e.g. EIN in the US) |
# MAGIC | tax_number_2 | LFA1.STCD2 | Tax number 2 (country-specific; e.g. VAT number) |
# MAGIC | posting_block | LFA1.SPERR | Central posting block: X = all postings blocked for this vendor |
# MAGIC | central_deletion_flag | LFA1.LOEVM | Central deletion flag: X = vendor marked for deletion |
# MAGIC | email | ADR6.SMTP_ADDR | Primary email address; NULL if no email on file |
# MAGIC | purchasing_organization | LFM1.EKORG | Purchasing organization this row applies to — one row per org the vendor is set up in |
# MAGIC | payment_terms | LFM1.ZTERM | Payment terms key (e.g. N030 = net 30 days) |
# MAGIC | order_currency | LFM1.WAERS | Default order currency for purchase orders |
# MAGIC | incoterms | LFM1.INCO1 | Incoterms code (e.g. EXW, FOB, CIF) |
# MAGIC | incoterms_description | LFM1.INCO2 | Incoterms location or supplementary description |
# MAGIC | minimum_order_value | LFM1.MINBW | Minimum order value in order currency |
# MAGIC | gr_based_iv_indicator | LFM1.WEBRE | GR-based invoice verification: X = invoice can only be posted after GR |
# MAGIC | purchasing_block | LFM1.LOEVM | Purchasing-org-level deletion / block flag |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,LFA1: General Vendor Master
stg_lfa1 = (
    spark.table("median_hub_captured.sap.LFA1")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("LIFNR"),
        F.col("ADRNR"),
        F.col("NAME1"),
        F.col("NAME2"),
        F.col("KTOKK"),
        F.col("BRSCH"),
        F.col("LAND1"),
        F.col("REGIO"),
        F.col("ORT01"),
        F.col("PSTLZ"),
        F.col("STRAS"),
        F.col("TELF1"),
        F.col("TELFX"),
        F.col("STCD1"),
        F.col("STCD2"),
        F.col("SPERR"),
        F.col("LOEVM")
    )
)

# COMMAND ----------

# DBTITLE 1,ADR6: Email Addresses (deduplicated to primary per address)
adr6_window = Window.partitionBy("ADDRNUMBER").orderBy(
    F.coalesce(F.col("FLGDEFAULT"), F.lit("")).desc(),
    F.col("SMTP_ADDR")
)

stg_adr6 = (
    spark.table("median_hub_captured.sap.ADR6")
    .filter(F.col("CLIENT") == "400")
    .filter(F.col("SMTP_ADDR").isNotNull() & (F.col("SMTP_ADDR") != ""))
    .withColumn("email_rank", F.row_number().over(adr6_window))
    .filter(F.col("email_rank") == 1)
    .select(
        F.col("ADDRNUMBER"),
        F.col("SMTP_ADDR")
    )
)

# COMMAND ----------

# DBTITLE 1,LFM1: Purchasing Organization Data
stg_lfm1 = (
    spark.table("median_hub_captured.sap.LFM1")
    .filter(F.col("MANDT") == "400")
    .select(
        F.col("LIFNR"),
        F.col("EKORG"),
        F.col("ZTERM"),
        F.col("WAERS"),
        F.col("INCO1"),
        F.col("INCO2"),
        F.col("MINBW"),
        F.col("WEBRE"),
        F.col("LOEVM").alias("purch_loevm")
    )
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_vendor_master = (
    stg_lfa1
    .join(stg_adr6, stg_lfa1["ADRNR"] == stg_adr6["ADDRNUMBER"], how="left")
    .join(stg_lfm1, on="LIFNR", how="left")
    .select(
        F.when(F.col("LIFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("LIFNR"), "^0+", ""))
         .otherwise(F.col("LIFNR"))
         .alias("vendor_account_number"),

        F.col("NAME1").alias("vendor_name"),
        F.col("NAME2").alias("vendor_name_2"),
        F.col("KTOKK").alias("account_group"),
        F.col("BRSCH").alias("industry"),
        F.col("LAND1").alias("country"),
        F.col("REGIO").alias("region"),
        F.col("ORT01").alias("city"),
        F.col("PSTLZ").alias("postal_code"),
        F.col("STRAS").alias("street_address"),
        F.col("TELF1").alias("telephone"),
        F.col("TELFX").alias("fax"),
        F.col("STCD1").alias("tax_number_1"),
        F.col("STCD2").alias("tax_number_2"),
        F.col("SPERR").alias("posting_block"),
        F.col("LOEVM").alias("central_deletion_flag"),
        F.col("SMTP_ADDR").alias("email"),
        F.col("EKORG").alias("purchasing_organization"),
        F.col("ZTERM").alias("payment_terms"),
        F.col("WAERS").alias("order_currency"),
        F.col("INCO1").alias("incoterms"),
        F.col("INCO2").alias("incoterms_description"),
        F.col("MINBW").alias("minimum_order_value"),
        F.col("WEBRE").alias("gr_based_iv_indicator"),
        F.col("purch_loevm").alias("purchasing_block")
    )
)

(
    df_vendor_master
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.vendor_master")
)
