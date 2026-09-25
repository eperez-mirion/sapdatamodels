# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Sales Order Item Category
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | TVAPT | Sales Document Item Category Texts | One row per item category per language; provides code and English description |
# MAGIC
# MAGIC **billing_type logic:**
# MAGIC A Mirion-defined classification grouping item categories by their billing behavior.
# MAGIC Item categories not in the mapping default to 'TBD'.
# MAGIC
# MAGIC | billing_type | Description |
# MAGIC |-------------|-------------|
# MAGIC | Standard | Standard line items billed directly against the order |
# MAGIC | Billing Plan | Items billed on a periodic or milestone billing plan |
# MAGIC | Milestone | Items billed at project milestones |
# MAGIC | TBD | Item categories not yet classified |
# MAGIC
# MAGIC **Granularity:** One row per sales document item category (PSTYV)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | item_category | TVAPT.PSTYV | SAP sales document item category code |
# MAGIC | description | TVAPT.VTEXT | English description of the item category |
# MAGIC | billing_type | Derived | Mirion billing classification: Standard \| Billing Plan \| Milestone \| TBD |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Billing Type Mapping
# Mirion billing classification by PSTYV. Types not listed default to 'TBD'.
billing_type_dict = {
    'AFTX': 'Standard', 'AGTX': 'Standard', 'G2N':  'Standard', 'G2TX': 'Standard',
    'L2N':  'Standard', 'TANN': 'Standard', 'TATX': 'Standard', 'ZAFN': 'Standard',
    'ZAGN': 'Standard', 'ZAGX': 'Standard', 'ZATX': 'Standard', 'ZBF2': 'Standard',
    'ZBIF': 'Standard', 'ZBIN': 'Standard', 'ZBM1': 'Standard', 'ZBOM': 'Standard',
    'ZBQT': 'Standard', 'ZCFC': 'Standard', 'ZCON': 'Standard', 'ZCOS': 'Standard',
    'ZCOT': 'Standard', 'ZCRS': 'Standard', 'ZCST': 'Standard', 'ZCXS': 'Standard',
    'ZDSR': 'Standard', 'ZFBD': 'Standard', 'ZFBF': 'Standard', 'ZFCP': 'Standard',
    'ZFFR': 'Standard', 'ZFOR': 'Standard', 'ZFQT': 'Standard', 'ZG2N': 'Standard',
    'ZGNN': 'Standard', 'ZKA1': 'Standard', 'ZKAN': 'Standard', 'ZKB1': 'Standard',
    'ZKBN': 'Standard', 'ZKBP': 'Standard', 'ZKEN': 'Standard', 'ZKOM': 'Standard',
    'ZKRN': 'Standard', 'ZL2N': 'Standard', 'ZL2W': 'Standard', 'ZLAN': 'Standard',
    'ZLNN': 'Standard', 'ZOBF': 'Standard', 'ZOFR': 'Standard', 'ZOSR': 'Standard',
    'ZPI':  'Standard', 'ZQAB': 'Standard', 'ZQBM': 'Standard', 'ZQM1': 'Standard',
    'ZQSF': 'Standard', 'ZQSR': 'Standard', 'ZRAF': 'Standard', 'ZRAL': 'Standard',
    'ZRAT': 'Standard', 'ZRCE': 'Standard', 'ZRCR': 'Standard', 'ZRE1': 'Standard',
    'ZRE2': 'Standard', 'ZREN': 'Standard', 'ZRGN': 'Standard', 'ZRIN': 'Standard',
    'ZRLA': 'Standard', 'ZRLB': 'Standard', 'ZRRB': 'Standard', 'ZRRE': 'Standard',
    'ZRRP': 'Standard', 'ZRRS': 'Standard', 'ZRVE': 'Standard', 'ZRWS': 'Standard',
    'ZSCQ': 'Standard', 'ZSNC': 'Standard', 'ZTAC': 'Standard', 'ZTAD': 'Standard',
    'ZTAJ': 'Standard', 'ZTAN': 'Standard', 'ZTAS': 'Standard', 'ZTAX': 'Standard',
    'ZTFS': 'Standard', 'ZWAR': 'Standard', 'ZWRP': 'Standard', 'ZWRR': 'Standard',
    'ZWVC': 'Standard', 'ZWVM': 'Standard', 'ZWVN': 'Standard', 'ZWVQ': 'Standard',
    'DLP':  'Billing Plan', 'ZBFB': 'Billing Plan', 'ZBFC': 'Billing Plan',
    'ZBFD': 'Billing Plan', 'ZBFF': 'Billing Plan', 'ZBM3': 'Billing Plan',
    'ZBOI': 'Billing Plan', 'ZSER': 'Billing Plan', 'ZSFR': 'Billing Plan',
    'ZTSB': 'Billing Plan',
    'ZADD': 'Milestone', 'ZPFC': 'Milestone', 'ZPRR': 'Milestone',
}

billing_type_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(billing_type_dict.items())])

# COMMAND ----------

# DBTITLE 1,Final Table
df_so_item_category = (
    spark.table("median_hub_captured.sap.TVAPT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(
        F.col("PSTYV").alias("item_category"),
        F.col("VTEXT").alias("description"),
        F.coalesce(billing_type_map[F.col("PSTYV")], F.lit("TBD")).alias("billing_type")
    )
)

(
    df_so_item_category
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.so_item_category")
)
