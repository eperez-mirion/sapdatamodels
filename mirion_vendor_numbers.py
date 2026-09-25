# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Mirion Vendor Numbers
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | LFA1  | General Vendor Master | One row per vendor account; provides name, country, and general data |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - MANDT = '400'
# MAGIC - NAME1 contains MIRION, CANBERRA, or CAPINTEC (case-insensitive) — captures all Mirion
# MAGIC   business units regardless of legacy brand name
# MAGIC
# MAGIC **short_name logic:**
# MAGIC Each LIFNR is mapped to a standardized Mirion site label (e.g. 'MIRION (MERIDEN)') via a
# MAGIC hardcoded dictionary. LIFNRs not in the mapping default to 'TBD'. This list is maintained
# MAGIC manually and should be reviewed when new Mirion entity vendor accounts are created in SAP.
# MAGIC
# MAGIC **Granularity:** One row per Mirion-affiliated vendor account (LFA1)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | vendor_number | LFA1.LIFNR | SAP vendor account number; leading zeros stripped for numeric accounts |
# MAGIC | vendor_name | LFA1.NAME1 | Vendor name as stored in SAP |
# MAGIC | country | LFA1.LAND1 | Country key from the vendor master |
# MAGIC | short_name | Derived | Mirion standardized site label; TBD if LIFNR not in the mapping |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Short Name Mapping
# Hardcoded LIFNR → Mirion site label mapping.
# Review and update when new Mirion entity vendor accounts are added in SAP.
lifnr_short_names = {
    '0000103998': 'MIRION (PASADENA)',
    '0000104494': 'MIRION (DO NOT USE)',
    '0000104541': 'MIRION (SMRYNA)',
    '0000106187': 'MIRION (ONTARIO)',
    '0000106981': 'MIRION (ATLANTA)',
    '0000107051': 'MIRION (HAMBURG)',
    '0000107338': 'MIRION (TURKU)',
    'R103998':    'MIRION (PASADENA)',
    'R104541':    'MIRION (DALLAS)',
    'V0001':      'MIRION (ATLANTA)',
    'V0190':      'MIRION (TURKU)',
    'V026018A':   'MIRION (AREVA LYNCHBURG)',
    'V0820':      'MIRION (CAPINTEC FLORHAM PARK)',
    'V104541':    'MIRION (ATLANTIA)',
    'V106981':    'MIRION (ATLANTA)',
    'V106982':    'MIRION (PAYROLL)',
    'V107338':    'MIRION (TURKU)',
    'V224001':    'MIRION (AREVA CHICAGO)',
    'V4002':      'MIRION (CONCORD)',
    'V4002A':     'MIRION (CONCORD)',
    'V4003':      'MIRION (DOVER)',
    'V4007':      'MIRION (ZELLIK)',
    'V4008':      'MIRION (RUSSELSHEIM)',
    'V4010':      'MIRION (LOCHES CEDEX)',
    'V4011':      'MIRION (LINGOLSHEIM)',
    'V4012':      'MIRION (ST. QUENTIN)',
    'V4019A':     'MIRION (OAK RIDGE)',
    'V4020':      'MIRION (MERIDEN)',
    'V4020A':     'MIRION (MERIDEN)',
    'V4021':      'MIRION (OLEN)',
    'V4022':      'MIRION (OXFORDSHIRE)',
    'V4023':      'MIRION (ATLANTA)',
    'V4026':      'MIRION (TOKYO)',
    'V4028':      'MIRION (CAMBRIDGE)',
    'V4045':      'MIRION (HAMBURG)',
    'V4111':      'MIRION (MUNCHEN)',
    'V4181':      'MIRION (NORROY-LE-VENEUR)',
    'V6010':      'MIRION (HORSEHEADS)',
    'V6110':      'MIRION (HORSEHEADS)',
    'V9101':      'MIRION (LAMANON)',
    'V9604':      'MIRION (FARNBOROUGH)',
    '0000111593': 'MIRION (PREMIUM ANALYSE)',
    '0000108226': 'MIRION (MONTINGNY-LE-BRETONNEUX)',
    'V0730':      'MIRION (SHANGHAI)',
}

short_name_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(lifnr_short_names.items())])

# COMMAND ----------

# DBTITLE 1,Final Table
df_mirion_vendor_numbers = (
    spark.table("median_hub_captured.sap.LFA1")
    .filter(F.col("MANDT") == "400")
    .filter(
        F.upper(F.col("NAME1")).contains("MIRION") |
        F.upper(F.col("NAME1")).contains("CANBERRA") |
        F.upper(F.col("NAME1")).contains("CAPINTEC")
    )
    .select(
        F.when(F.col("LIFNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("LIFNR"), "^0+", ""))
         .otherwise(F.col("LIFNR"))
         .alias("vendor_number"),

        F.col("NAME1").alias("vendor_name"),
        F.col("LAND1").alias("country"),

        F.coalesce(short_name_map[F.col("LIFNR")], F.lit("TBD")).alias("short_name")
    )
)

(
    df_mirion_vendor_numbers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.mirion_vendor_numbers")
)
