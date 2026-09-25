# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Mirion Customer Numbers
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | KNA1  | Customer Master | One row per customer account; provides name, city, and deletion/block flags |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - MANDT = '400'
# MAGIC - NAME1 contains MIRION, CANBERRA, SUN NUCLEAR, or CAPINTEC (case-insensitive) — captures all
# MAGIC   Mirion business units regardless of legacy brand name
# MAGIC - LOEVM is null or blank — exclude centrally deleted customers
# MAGIC - SPERR is null or blank — exclude centrally blocked customers
# MAGIC - NAME1 does not start with 'BLK3' — exclude bulk/test accounts
# MAGIC - KUNNR does not contain 'FR' — exclude French entity customer numbers
# MAGIC
# MAGIC **short_name logic:**
# MAGIC Each KUNNR is mapped to a standardized Mirion site label (e.g. 'MIRION (MERIDEN)') via a
# MAGIC hardcoded dictionary. KUNNRs not in the mapping default to 'TBD'. This list is maintained
# MAGIC manually and should be reviewed when new Mirion entity customer accounts are created in SAP.
# MAGIC
# MAGIC **Granularity:** One row per Mirion-affiliated customer account (KNA1)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | customer_number | KNA1.KUNNR | SAP customer account number; leading zeros stripped for numeric accounts |
# MAGIC | customer_name | KNA1.NAME1 | Customer name as stored in SAP |
# MAGIC | city | KNA1.ORT01 | City from the customer master address |
# MAGIC | short_name | Derived | Mirion standardized site label; TBD if KUNNR not in the mapping |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Short Name Mapping
# Hardcoded KUNNR → Mirion site label mapping.
# Review and update when new Mirion entity customer accounts are added in SAP.
kunnr_short_names = {
    '0001002236': 'MIRION (AIKEN)',
    '0001012367': 'MIRION (AIKEN)',
    'C3021':      'MIRION (ATLANTA)',
    '0001011969': 'MIRION (ATLANTA)',
    '0001011749': 'MIRION (ATLANTA)',
    '0001016098': 'MIRION (CAMBRIDGE)',
    '0001014154': 'MIRION (CAMBRIDGE)',
    '0001015338': 'MIRION (CAPINTEC)',
    '0001001381': 'MIRION (CAPINTEC)',
    '0001000839': 'MIRION (CAPINTEC)',
    '0001004584': 'MIRION (CONCORD)',
    'C3002':      'MIRION (CONCORD)',
    '0001011588': 'MIRION (FARNBOROUGH)',
    '0001015274': 'MIRION (FARNBOROUGH)',
    '0001011807': 'MIRION (HAMBURG)',
    '0001011806': 'MIRION (HAMBURG)',
    '0001012978': 'MIRION (HORSEHEADS 200)',
    '0001014153': 'MIRION (HORSEHEADS 200)',
    '0001013601': 'MIRION (HORSEHEADS 204)',
    '0001011788': 'MIRION (LAMANON)',
    '0001011789': 'MIRION (LAMANON)',
    '0001011775': 'MIRION (LAMANON)',
    '0001002523': 'MIRION (MERIDEN)',
    '0001005031': 'MIRION (MERIDEN)',
    '0001002841': 'MIRION (MERIDEN)',
    '0001005978': 'MIRION (MERIDEN)',
    'C3020':      'MIRION (MERIDEN)',
    '0001004585': 'MIRION (MERIDEN)',
    '0001012394': 'MIRION (MERIDEN)',
    '0001000251': 'MIRION (MONTIGNY-LE-BRETONNEUX)',
    '0001008953': 'MIRION (MONTIGNY-LE-BRETONNEUX)',
    '0001015151': 'MIRION (MONTIGNY-LE-BRETONNEUX)',
    '0001015456': 'MIRION (MONTIGNY-LE-BRETONNEUX)',
    '0001002826': 'MIRION (MOSCOW)',
    '0001009299': 'MIRION (MOSCOW)',
    '0001012508': 'MIRION (OAK RIDGE-DSD)',
    '0001002580': 'MIRION (OAK RIDGE)',
    '0001013811': 'MIRION (OAK RIDGE)',
    'C3019':      'MIRION (OAK RIDGE)',
    '0001013243': 'MIRION (OAK RIDGE)',
    '0001001520': 'MIRION (OLEN)',
    '0001001059': 'MIRION (OLEN)',
    '0001001837': 'MIRION (OLEN)',
    '0001000372': 'MIRION (OXFORDSHIRE)',
    '0001000248': 'MIRION (OXFORDSHIRE)',
    '0001001796': 'MIRION (OXFORDSHIRE)',
    '0001001838': 'MIRION (RUSSELSHEIM)',
    '0001000249': 'MIRION (RUSSELSHEIM)',
    '0001013554': 'MIRION (RUSSELSHEIM)',
    '0001016141': 'MIRION (RUSSELSHEIM)',
    '0001001053': 'MIRION (SAINT PAUL TROIS CHATEAUX)',
    '0001000241': 'MIRION (SCHWADORF)',
    '0001011336': 'MIRION (SCHWADORF)',
    '0001015318': 'MIRION (SCHWADORF)',
    '0001001980': 'MIRION (SCHWADORF)',
    '0001012335': 'MIRION (SHANGHAI)',
    '0001013205': 'MIRION (SHANGHAI)',
    '0001017643': 'MIRION (SIS)',
    '0001017644': 'MIRION (SIS)',
    '0001016295': 'MIRION (SIS)',
    '0001016296': 'MIRION (SIS)',
    '0001015150': 'MIRION (ST QUENTIN YVELINES)',
    '0001000250': 'MIRION (ST QUENTIN YVELINES)',
    '0001017283': 'MIRION (SUN NUCLEAR-MELBOURNE)',
    '0001015982': 'MIRION (SUN NUCLEAR-MELBOURNE)',
    '0001015329': 'MIRION (SUN NUCLEAR-MELBOURNE)',
    '0001015131': 'MIRION (SUN NUCLEAR-MIDDLETON)',
    '0001001058': 'MIRION (TANNERIES)',
    '0001000253': 'MIRION (TOKYO)',
    '0001013974': 'MIRION (TURKU)',
    '0001003754': 'MIRION (TURKU)',
    '0001016101': 'MIRION (TURKU)',
    '0001000252': 'MIRION (ZELLIK)',
    '0001013118': 'MIRION (ZELLIK)',
}

short_name_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(kunnr_short_names.items())])

# COMMAND ----------

# DBTITLE 1,Final Table
df_mirion_customer_numbers = (
    spark.table("median_hub_captured.sap.KNA1")
    .filter(F.col("MANDT") == "400")
    .filter(
        F.upper(F.col("NAME1")).contains("MIRION") |
        F.upper(F.col("NAME1")).contains("CANBERRA") |
        F.upper(F.col("NAME1")).contains("SUN NUCLEAR") |
        F.upper(F.col("NAME1")).contains("CAPINTEC")
    )
    .filter(F.col("LOEVM").isNull() | (F.col("LOEVM") == ""))
    .filter(F.col("SPERR").isNull() | (F.col("SPERR") == ""))
    .filter(~F.col("NAME1").startswith("BLK3"))
    .filter(~F.col("KUNNR").contains("FR"))
    .select(
        F.when(F.col("KUNNR").rlike("^[0-9]+$"), F.regexp_replace(F.col("KUNNR"), "^0+", ""))
         .otherwise(F.col("KUNNR"))
         .alias("customer_number"),

        F.col("NAME1").alias("customer_name"),
        F.col("ORT01").alias("city"),

        F.coalesce(short_name_map[F.col("KUNNR")], F.lit("TBD")).alias("short_name")
    )
)

(
    df_mirion_customer_numbers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.mirion_customer_numbers")
)
