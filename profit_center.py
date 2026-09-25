# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Dimension: Profit Centers
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | CEPC | Profit Center Master Data | One row per active profit center; base for all profit centers in BUMN controlling area |
# MAGIC | CEPCT | Profit Center Texts | English descriptions for each profit center |
# MAGIC | profit_center_hierarchy | Profit Center Hierarchy | Pre-built hierarchy table mapping profit centers to site and department groupings |
# MAGIC
# MAGIC **Filter logic:**
# MAGIC - CEPC: MANDT = '400', KOKRS = 'BUMN' (NA Tech controlling area), DATBI = '99991231' (active records only)
# MAGIC - CEPCT: MANDT = '400', KOKRS = 'BUMN', DATBI = '99991231', SPRAS = 'E'
# MAGIC - Hierarchy: left joined — profit centers missing from the hierarchy still appear with null level columns
# MAGIC
# MAGIC **Hierarchy depth note:**
# MAGIC The `profit_center_hierarchy` table has variable depth across business units:
# MAGIC - Meriden and Oak Ridge profit centers sit at **level 4** (Top → Site → Dept → Profit Center)
# MAGIC - RMS, SIS, and Corporate profit centers sit at **level 3** (Top → Site → Profit Center; no dept tier)
# MAGIC - Some profit centers are absent from the hierarchy entirely and will have null level columns
# MAGIC
# MAGIC **Granularity:** One row per active profit center (PRCTR) in BUMN controlling area
# MAGIC
# MAGIC **Output columns:**
# MAGIC | Column | Source | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | profit_center | CEPC.PRCTR | SAP profit center code |
# MAGIC | description | CEPCT.KTEXT | English short text from profit center master |
# MAGIC | lock_indicator | CEPC.LOCK_IND | Blank = active; X = locked/inactive |
# MAGIC | level_3_dept | hierarchy | Department-level grouping (level 3 node). Null for RMS/SIS/Corporate profit centers and those not in hierarchy |
# MAGIC | level_3_dept_desc | hierarchy | Department description |
# MAGIC | level_2_site | hierarchy | Site-level grouping (level 2 node), e.g. MERIDEN.CY24, OAKRIDGE.CY24, RMS.CY24 |
# MAGIC | level_2_site_desc | hierarchy | Site description |
# MAGIC | level_1_top | hierarchy | Top-level hierarchy node |
# MAGIC | level_1_top_desc | hierarchy | Top-level description |

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Source Tables
cepc = (
    spark.table("median_hub_captured.sap.CEPC")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("KOKRS") == "BUMN") &
        (F.col("DATBI") == "99991231")
    )
)

cepct = (
    spark.table("median_hub_captured.sap.CEPCT")
    .filter(
        (F.col("MANDT") == "400") &
        (F.col("KOKRS") == "BUMN") &
        (F.col("DATBI") == "99991231") &
        (F.col("SPRAS") == "E")
    )
    .select("PRCTR", "KTEXT")
)

# COMMAND ----------

# DBTITLE 1,Build Hierarchy Lookup
hier = spark.table("median_hub_captured.sap.profit_center_hierarchy")

l1 = hier.filter(F.col("LEVEL") == "1").select(
    F.col("CHILD").alias("l1_child"),
    F.col("CHILD_DESC").alias("l1_desc"),
)
l2 = hier.filter(F.col("LEVEL") == "2").select(
    F.col("CHILD").alias("l2_child"),
    F.col("CHILD_DESC").alias("l2_desc"),
    F.col("PARENT").alias("l2_parent"),
)
l3 = hier.filter(F.col("LEVEL") == "3").select(
    F.col("CHILD").alias("l3_child"),
    F.col("CHILD_DESC").alias("l3_desc"),
    F.col("PARENT").alias("l3_parent"),
)
l4 = hier.filter(F.col("LEVEL") == "4").select(
    F.col("CHILD").alias("l4_child"),
    F.col("PARENT").alias("l4_parent"),
)

# Level 3 leaf nodes: level 3 nodes that have no children at level 4
# These are the actual profit centers for RMS, SIS, and Corporate groups
l4_parents = l4.select(F.col("l4_parent").alias("l3_child")).distinct()
l3_leaves = l3.join(l4_parents, "l3_child", "left_anti")

# Hierarchy context for level 4 profit centers (L4 → L3 dept → L2 site → L1 top)
hier_l4 = (
    l4
    .join(l3, l4["l4_parent"] == l3["l3_child"], "left")
    .join(l2, l3["l3_parent"] == l2["l2_child"], "left")
    .join(l1, l2["l2_parent"] == l1["l1_child"], "left")
    .select(
        F.col("l4_child").alias("prctr"),
        F.col("l3_child").alias("level_3_dept"),
        F.col("l3_desc").alias("level_3_dept_desc"),
        F.col("l2_child").alias("level_2_site"),
        F.col("l2_desc").alias("level_2_site_desc"),
        F.col("l1_child").alias("level_1_top"),
        F.col("l1_desc").alias("level_1_top_desc"),
    )
)

# Hierarchy context for level 3 leaf profit centers (L3 → L2 site → L1 top; no dept tier)
hier_l3 = (
    l3_leaves
    .join(l2, l3_leaves["l3_parent"] == l2["l2_child"], "left")
    .join(l1, l2["l2_parent"] == l1["l1_child"], "left")
    .select(
        F.col("l3_child").alias("prctr"),
        F.lit(None).cast("string").alias("level_3_dept"),
        F.lit(None).cast("string").alias("level_3_dept_desc"),
        F.col("l2_child").alias("level_2_site"),
        F.col("l2_desc").alias("level_2_site_desc"),
        F.col("l1_child").alias("level_1_top"),
        F.col("l1_desc").alias("level_1_top_desc"),
    )
)

hier_lookup = hier_l4.union(hier_l3)

# COMMAND ----------

# DBTITLE 1,Final Table
df_profit_center = (
    cepc
    .join(cepct, "PRCTR", "left")
    .join(hier_lookup, cepc["PRCTR"] == hier_lookup["prctr"], "left")
    .select(
        F.col("PRCTR").alias("profit_center"),
        F.col("KTEXT").alias("description"),
        F.col("LOCK_IND").alias("lock_indicator"),
        F.col("level_3_dept"),
        F.col("level_3_dept_desc"),
        F.col("level_2_site"),
        F.col("level_2_site_desc"),
        F.col("level_1_top"),
        F.col("level_1_top_desc"),
    )
)

(
    df_profit_center
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hub_live_transformed.sap.profit_center")
)
