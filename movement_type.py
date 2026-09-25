# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Movement Type
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | T156  | Movement Type Configuration | One row per movement type code (BWART) |
# MAGIC | T156T | Movement Type Texts | Provides the SAP standard description per movement type in English |
# MAGIC
# MAGIC **Join logic:**
# MAGIC - T156 is the base table providing the movement type code (BWART).
# MAGIC - T156T joins on BWART (left) filtered to MANDT = '400' and SPRAS = 'E' to add the SAP
# MAGIC   standard description. Deduped to one row per BWART before joining to prevent fan-out.
# MAGIC
# MAGIC **movement_type_text logic:**
# MAGIC A hardcoded mapping of BWART codes to Mirion plain-language descriptions. Movement types
# MAGIC not in the mapping are excluded from the output — this table intentionally covers only the
# MAGIC movement types active in NA Tech. To include all SAP movement types, remove the final filter.
# MAGIC
# MAGIC **breakdown logic:**
# MAGIC A hardcoded mapping grouping each movement type into one of five operational categories:
# MAGIC Goods Receipt, Goods Issue, Transfer, Scrap, or Adjustment. Used for high-level reporting
# MAGIC and filtering across goods movement history.
# MAGIC
# MAGIC **Granularity:** One row per movement type code (BWART), NA Tech active types only
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | movement_type | T156.BWART | SAP movement type code |
# MAGIC | sap_description | T156T.BTEXT | SAP standard description for the movement type in English |
# MAGIC | movement_type_text | Derived | Mirion plain-language description of the movement type |
# MAGIC | breakdown | Derived | High-level category: Goods Receipt \| Goods Issue \| Transfer \| Scrap \| Adjustment |

# COMMAND ----------

from itertools import chain as iterchain
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Movement Type Text Mapping
# Mirion plain-language descriptions by BWART.
# Movement types not listed here are excluded from the output.
# To include all SAP movement types, remove the filter on movement_type_text at the end.
movement_type_text = {
    '101': 'Goods receipt (PO)',
    '102': 'Goods receipt (PO) Reversal',
    '107': 'Goods Receipt to valuated blocked stock',
    '108': 'Goods Receipt to valuated blocked stock Reversal',
    '109': 'Goods Receipt from valuated blocked stock',
    '110': 'Goods Receipt from valuated blocked stock Reversal',
    '122': 'Return to Material to PO',
    '123': 'Return to Material to PO Reversal',
    '161': 'Return to supplier (PO)',
    '162': 'Return to supplier (PO) Reversal',
    '201': 'Goods issue (cost center)',
    '202': 'Goods issue (cost center) Reversal',
    '221': 'Goods issue (Project)',
    '222': 'Goods issue (Project) Reversal',
    '241': 'Goods issue (Asset)',
    '242': 'Goods issue (Asset) Reversal',
    '261': 'Goods issue (Production Order)',
    '262': 'Goods issue (Production Order) Reversal',
    '281': 'Goods issue (Network)',
    '282': 'Goods issue (Network) Reversal',
    '301': 'Transfer (Plant to Plant) - 1 Step',
    '302': 'Transfer (Plant to Plant) - 1 Step Reversal',
    '303': 'Transfer (Plant to Plant) - 2 Step',
    '304': 'Transfer (Plant to Plant) - 2 Step Reversal',
    '309': 'Transfer (material to material)',
    '310': 'Transfer (material to material) Reversal',
    '311': 'Transfer (Storage Location to Storage Location in 1 Step)',
    '312': 'Transfer (Storage Location to Storage Location in 1 Step) Reversal',
    '313': 'Transfer (Storage location to Storage Location in 2 Steps)',
    '314': 'Transfer (Storage location to Storage Location in 2 Steps) Reversal',
    '321': 'Transfer (Inspection Stock to Unrestricted Stock)',
    '322': 'Transfer (Inspection Stock to Unrestricted Stock) Reversal',
    '323': 'Transfer (Inspection Stock to Inspection Stock)',
    '324': 'Transfer (Inspection Stock to Inspection Stock) Reversal',
    '325': 'Transfer (Storage Location to Storage Location; Blocked Stock)',
    '326': 'Transfer (Storage Location to Storage Location; Blocked Stock) Reversal',
    '341': 'Status Change of a Batch (Unrestricted to restricted)',
    '342': 'Status Change of a Batch (Unrestricted to restricted) Reversal',
    '343': 'Transfer (Blocked Stock to Unrestricted Stock)',
    '344': 'Transfer (Blocked Stock to Unrestricted Stock) Reversal',
    '349': 'Transfer (Blocked Stock to Inspection Stock)',
    '350': 'Transfer (Blocked Stock to Inspection Stock) Reversal',
    '411': 'Transfer (Special Stock (E/K/Q) to Unrestricted Stock)',
    '412': 'Transfer (Special Stock (E/K/Q) to Unrestricted Stock) Reversal',
    '501': 'Goods Receipt (No PO to Unrestricted Stock)',
    '502': 'Goods Receipt (No PO to Unrestricted Stock) Reversal',
    '511': 'Goods Receipt (Free-of-Charge from Vendor)',
    '512': 'Goods Receipt (Free-of-Charge from Vendor) Reversal',
    '531': 'Goods Receipt (By-product from an order - Such as negative qty on WO)',
    '532': 'Goods Receipt (By-product from an order - Such as negative qty on WO) Reversal',
    '541': 'Transfer (Unrestricted Stock to Vendor Stock)',
    '542': 'Transfer (Unrestricted Stock to Vendor Stock) Reversal',
    '543': 'Goods Issue (Consumption of Vendor Subcontract Stock)',
    '544': 'Goods Issue (Consumption of Vendor Subcontract Stock) Reversal',
    '551': 'Scrap (from Unrestricted Stock)',
    '552': 'Scrap (from Unrestricted Stock) Reversal',
    '561': 'Initial Entry of Stock (to Unrestricted Stock)',
    '562': 'Initial Entry of Stock (to Unrestricted Stock) Reversal',
    '581': 'Goods Receipt (Network to Unrestricted Stock)',
    '582': 'Goods Receipt (Network to Unrestricted Stock) Reversal',
    '601': 'Goods Issue (Customer Order)',
    '602': 'Goods Issue (Customer Order) Reversal',
    '621': 'Transfer (Unrestricted Stock to Customer Stock)',
    '622': 'Transfer (Unrestricted Stock to Customer Stock) Reversal',
    '623': 'Goods Issue (from Customer Stock)',
    '624': 'Goods Issue (from Customer Stock) Reversal',
    '631': 'Transfer (Unrestricted Stock to Customer Consignment Stock)',
    '632': 'Transfer (Unrestricted Stock to Customer Consignment Stock) Reversal',
    '641': 'Goods Issue (Interco Stock Transfer)',
    '642': 'Goods Issue (Interco Stock Transfer) Reversal',
    '653': 'Returns (From Customer to Unrestricted Stock)',
    '654': 'Returns (From Customer to Unrestricted Stock) Reversal',
    '701': 'Inventory Adjustments (unrestricted stock)',
    '702': 'Inventory Adjustments (unrestricted stock) Reversal',
    '703': 'Inventory Adjustments (inspection stock)',
    '704': 'Inventory Adjustments (inspection stock) Reversal',
    '707': 'Inventory Adjustments (blocked stock)',
    '708': 'Inventory Adjustments (blocked stock) Reversal',
    '901': 'Goods Issue (Direct to Cost Center)',
    '902': 'Goods Issue (Direct to Cost Center) Reversal',
    '971': 'Transfer (Quality to Unrestricted Stock)',
}

movement_type_text_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(movement_type_text.items())])

# COMMAND ----------

# DBTITLE 1,Breakdown Category Mapping
# High-level category per BWART for reporting and filtering.
breakdown = {
    '101': 'Goods Receipt', '102': 'Goods Receipt', '107': 'Goods Receipt',
    '108': 'Goods Receipt', '109': 'Goods Receipt', '110': 'Goods Receipt',
    '161': 'Goods Receipt', '162': 'Goods Receipt', '501': 'Goods Receipt',
    '502': 'Goods Receipt', '511': 'Goods Receipt', '512': 'Goods Receipt',
    '531': 'Goods Receipt', '532': 'Goods Receipt', '581': 'Goods Receipt',
    '582': 'Goods Receipt',
    '201': 'Goods Issue', '202': 'Goods Issue', '221': 'Goods Issue',
    '222': 'Goods Issue', '241': 'Goods Issue', '242': 'Goods Issue',
    '261': 'Goods Issue', '262': 'Goods Issue', '281': 'Goods Issue',
    '282': 'Goods Issue', '543': 'Goods Issue', '544': 'Goods Issue',
    '601': 'Goods Issue', '602': 'Goods Issue', '621': 'Goods Issue',
    '622': 'Goods Issue', '623': 'Goods Issue', '624': 'Goods Issue',
    '631': 'Goods Issue', '632': 'Goods Issue', '641': 'Goods Issue',
    '642': 'Goods Issue', '653': 'Goods Issue', '654': 'Goods Issue',
    '901': 'Goods Issue', '902': 'Goods Issue',
    '122': 'Transfer', '123': 'Transfer', '301': 'Transfer', '302': 'Transfer',
    '303': 'Transfer', '304': 'Transfer', '309': 'Transfer', '310': 'Transfer',
    '311': 'Transfer', '312': 'Transfer', '313': 'Transfer', '314': 'Transfer',
    '321': 'Transfer', '322': 'Transfer', '323': 'Transfer', '324': 'Transfer',
    '325': 'Transfer', '326': 'Transfer', '341': 'Transfer', '342': 'Transfer',
    '343': 'Transfer', '344': 'Transfer', '349': 'Transfer', '350': 'Transfer',
    '411': 'Transfer', '412': 'Transfer', '541': 'Transfer', '542': 'Transfer',
    '971': 'Transfer',
    '551': 'Scrap', '552': 'Scrap',
    '561': 'Adjustment', '562': 'Adjustment', '701': 'Adjustment',
    '702': 'Adjustment', '703': 'Adjustment', '704': 'Adjustment',
    '707': 'Adjustment', '708': 'Adjustment',
}

breakdown_map = F.create_map(*[F.lit(x) for x in iterchain.from_iterable(breakdown.items())])

# COMMAND ----------

# DBTITLE 1,T156T: SAP Descriptions (deduped to one row per BWART)
stg_t156t = (
    spark.table("median_hub_captured.sap.T156T")
    .filter((F.col("MANDT") == "400") & (F.col("SPRAS") == "E"))
    .select(F.col("BWART"), F.col("BTEXT"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Final Table
df_movement_type = (
    spark.table("median_hub_captured.sap.T156")
    .filter(F.col("MANDT") == "400")
    .select(F.col("BWART"))
    .join(stg_t156t, on="BWART", how="left")
    .select(
        F.col("BWART").alias("movement_type"),
        F.col("BTEXT").alias("sap_description"),
        movement_type_text_map[F.col("BWART")].alias("movement_type_text"),
        F.coalesce(breakdown_map[F.col("BWART")], F.lit("Unknown")).alias("breakdown")
    )
    .filter(F.col("movement_type_text").isNotNull())
)

(
    df_movement_type
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.movement_type")
)
