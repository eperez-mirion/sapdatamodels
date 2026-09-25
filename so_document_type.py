# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Sales Order Document Type
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | TVAK  | Sales Document Types | One row per sales document type code (AUART) |
# MAGIC | TVAKT | Sales Document Type Texts | Language-dependent description for each document type |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - TVAK is the base table providing the document type code (AUART).
# MAGIC - TVAKT joins on AUART (left) filtered to MANDT = '400' and SPRAS = 'E' to add the
# MAGIC   English description.
# MAGIC
# MAGIC **order_type logic:**
# MAGIC A Mirion-defined classification grouping SAP document types into business order categories.
# MAGIC Document types not in the mapping default to 'Unused'.
# MAGIC
# MAGIC | order_type | AUART codes |
# MAGIC |------------|-------------|
# MAGIC | Standard | ZAP, ZOR, ZPOR, ZRC1, ZRS1 |
# MAGIC | Inquiry | ZIN |
# MAGIC | Consignment | ZLI, ZLP, ZLPU |
# MAGIC | Customer Loan | ZLN |
# MAGIC | Quotation | ZPQT, ZQT |
# MAGIC | Return | ZPRE, ZRE |
# MAGIC | Warranty/Repair | ZRC, ZRC3, ZRS, ZRS2, ZRS3 |
# MAGIC | Unused | all others |
# MAGIC
# MAGIC **Granularity:** One row per sales document type (AUART)
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | sales_doc_type | TVAK.AUART | SAP sales document type code |
# MAGIC | description | TVAKT.BEZEI | English description of the document type |
# MAGIC | order_type | Derived | Mirion business classification for the document type |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Order Type Mapping
# Mirion business classification by AUART. Types not listed default to 'Unused'.
order_type_map_dict = {
    'ZAP':  'Standard',
    'ZOR':  'Standard',
    'ZPOR': 'Standard',
    'ZRC1': 'Standard',
    'ZRS1': 'Standard',
    'ZIN':  'Inquiry',
    'ZLI':  'Consignment',
    'ZLP':  'Consignment',
    'ZLPU': 'Consignment',
    'ZLN':  'Customer Loan',
    'ZPQT': 'Quotation',
    'ZQT':  'Quotation',
    'ZPRE': 'Return',
    'ZRE':  'Return',
    'ZRC':  'Warranty/Repair',
    'ZRC3': 'Warranty/Repair',
    'ZRS':  'Warranty/Repair',
    'ZRS2': 'Warranty/Repair',
    'ZRS3': 'Warranty/Repair',
}

order_type_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(order_type_map_dict.items())])

# COMMAND ----------

# DBTITLE 1,TVAKT: Document Type Descriptions
stg_tvakt = (
    spark.table("median_hub_captured.sap.TVAKT")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(F.col("AUART"), F.col("BEZEI"))
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_so_document_type = (
    spark.table("median_hub_captured.sap.TVAK")
    .filter(F.col("MANDT") == "400")
    .join(stg_tvakt, on="AUART", how="left")
    .select(
        F.col("AUART").alias("sales_doc_type"),
        F.col("BEZEI").alias("description"),
        F.coalesce(order_type_map[F.col("AUART")], F.lit("Unused")).alias("order_type")
    )
)

(
    df_so_document_type
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.so_document_type")
)
