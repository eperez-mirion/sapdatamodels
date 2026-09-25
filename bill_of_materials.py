# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Multi-Level BOM Explosion Report
# MAGIC **Source:** `median_hub_captured.sap`
# MAGIC
# MAGIC **Tables Used:**
# MAGIC | Table | Description | Role |
# MAGIC |-------|-------------|------|
# MAGIC | MAST  | Material to BOM Link | Top-level assembly entry point |
# MAGIC | STKO  | BOM Header | BOM metadata, category, alternative |
# MAGIC | STAS  | BOM Item Selection | Active version control per item node |
# MAGIC | STPO  | BOM Item | Component details (qty, UOM, valid-from) |
# MAGIC | MAKT  | Material Descriptions | Parent and component descriptions |
# MAGIC
# MAGIC **Revision / dedup filtering (four-layer approach in bom_direct):**
# MAGIC - **MAST dedup:** Enforces one STLNR per business key `(MANDT, MATNR, WERKS, STLAN, STLAL)`,
# MAGIC   keeping the highest STLNR (most recently created BOM).
# MAGIC - **STKO dedup:** Enforces one row per `(MANDT, STLNR, STLAL)`, preferring STLTY = 'M'.
# MAGIC - **STAS window:** For each item node (`STLKN`), keep the STAS row with the highest
# MAGIC   `DATUV` ≤ today. Removes nodes deactivated via engineering change.
# MAGIC - **STPO window:** For each item node (`STLKN`), keep the STPO row with the highest
# MAGIC   `DATUV` ≤ today. Collapses multiple revision records per node.
# MAGIC - **Layer 1 — Alt BOM selection:** For each `(parent_matnr, plant, bom_usage)`, keep only
# MAGIC   the lowest-numbered alternative BOM.
# MAGIC - **Layer 2 — Revision-letter dedup:** For components matching `\d+[A-Z]` (e.g. 7072219J),
# MAGIC   keeps only the highest revision letter per base number within each BOM.
# MAGIC - **Layer 3 — Same-qty position dedup:** For each `(bom_number, alt_bom, component_matnr,
# MAGIC   qty, uom)`, keeps only the entry with the latest `valid_from`.
# MAGIC - **Layer 4 — Item-number dedup:** For each `(bom_number, alt_bom, item_number)`, keeps
# MAGIC   only the entry with the highest item_node (STLKN). SAP assigns STLKNs sequentially, so the
# MAGIC   highest STLKN is the most recently created ECO and is the authoritative active component.
# MAGIC - `STPO.LKENZ IS NULL OR LKENZ != 'X'` — deletion-flagged items excluded.
# MAGIC
# MAGIC **BOM Usage propagation:** The `bom_usage` of each parent row is carried forward
# MAGIC during the explosion. Each level looks up sub-assembly BOMs using the same usage type,
# MAGIC so a production BOM (usage=1) never picks up engineering-only BOMs (usage=2) deeper
# MAGIC in the chain.
# MAGIC
# MAGIC **Plant Separation:** `top_material` + `top_plant` are the unique key for each BOM.
# MAGIC Sub-assembly expansion is constrained to `top_plant` throughout to prevent cross-plant
# MAGIC contamination.
# MAGIC
# MAGIC **Sort Order:** Top material + plant → depth-first by item number.

# COMMAND ----------

# Configuration
CATALOG    = "median_hub_captured"
SCHEMA     = "sap"
MANDT      = "400"
SPRAS      = "E"
MAX_LEVELS = 10

# Optional filters — set to a value to restrict, None to include all.
# FILTER_MATERIAL controls the starting point only — sub-levels always expand fully.
# Supports a single value, a list, or None for the full dataset.
#   Single:   "DSA-LX"
#   Multiple: ["DSA-LX", "ABC-123", "XYZ-456"]
#   All:      None
FILTER_MATERIAL  = None
# FILTER_MATERIAL  = ["LYNX-II", "CP5-PLUS", "ICC-VD", "BE6530-DET", "AEGIS-BE5030-RDC", "7061780", "DSA-LX", "7074744", "7200", "NAIS-3X5X16-FS", "PA45018", "OSPREY-DTB", "GC4018-DET", "767", "747"]
FILTER_PLANT     = None  # e.g. "4019"
FILTER_BOM_USAGE = None  # e.g. "1" = Production, "2" = Engineering

# Scratch table — intermediate materialization needed for serverless compute.
# Requires write access to the target catalog/schema.
SCRATCH_CATALOG = "hub_live_transformed"
SCRATCH_SCHEMA  = "sap"
SCRATCH_TABLE   = f"`{SCRATCH_CATALOG}`.`{SCRATCH_SCHEMA}`.`_bom_direct_scratch`"


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

def tbl(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

def strip_zeros(col):
    """Remove leading zeros from all-numeric material numbers. Leave alphanumeric unchanged."""
    return F.when(
        col.rlike("^[0-9]+$"),
        F.regexp_replace(col, "^0+", "")
    ).otherwise(col)

# COMMAND ----------

# Load base tables — select only the columns needed to keep plans lean.
#
# MAST: enforce one STLNR per business key (MATNR, WERKS, STLAN, STLAL).
_w_mast = Window.partitionBy("MANDT", "MATNR", "WERKS", "STLAN", "STLAL").orderBy(F.col("STLNR").desc())
mast = (
    spark.table(tbl("MAST"))
    .filter(F.col("MANDT") == MANDT)
    .select("MANDT", "MATNR", "WERKS", "STLAN", "STLNR", "STLAL")
    .withColumn("_rn", F.row_number().over(_w_mast))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("MATNR", strip_zeros(F.col("MATNR")))
)

# STKO: enforce one row per (STLNR, STLAL), preferring STLTY = 'M'.
_w_stko = Window.partitionBy("MANDT", "STLNR", "STLAL").orderBy(
    F.when(F.col("STLTY") == "M", 0).otherwise(1).asc(),
    F.col("STLTY").asc(),
)
stko = (
    spark.table(tbl("STKO"))
    .filter(F.col("MANDT") == MANDT)
    .select("MANDT", "STLTY", "STLNR", "STLAL")
    .withColumn("_rn", F.row_number().over(_w_stko))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# STAS: one row per item node — the highest DATUV <= today.
w_stas = Window.partitionBy("MANDT", "STLTY", "STLNR", "STLAL", "STLKN").orderBy(F.col("DATUV").desc())
stas = (
    spark.table(tbl("STAS"))
    .filter(F.col("MANDT") == MANDT)
    .filter(F.col("DATUV") <= F.current_date())
    .select("MANDT", "STLTY", "STLNR", "STLAL", "STLKN", "DATUV")
    .withColumn("rn", F.row_number().over(w_stas))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# STPO: one row per item node — the highest DATUV <= today, deletion-flagged rows excluded.
w_stpo = Window.partitionBy("MANDT", "STLTY", "STLNR", "STLKN").orderBy(F.col("DATUV").desc())
stpo = (
    spark.table(tbl("STPO"))
    .filter(F.col("MANDT") == MANDT)
    .filter(F.col("LKENZ").isNull() | (F.col("LKENZ") != "X"))
    .filter(F.col("DATUV") <= F.current_date())
    .select("MANDT", "STLTY", "STLNR", "STLKN", "STPOZ", "POSNR", "IDNRK", "MENGE", "MEINS", "POSTP", "DATUV")
    .withColumn("rn", F.row_number().over(w_stpo))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("IDNRK", strip_zeros(F.col("IDNRK")))
)

makt_p = (
    spark.table(tbl("MAKT"))
    .filter((F.col("MANDT") == MANDT) & (F.col("SPRAS") == SPRAS))
    .select(
        strip_zeros(F.col("MATNR")).alias("parent_material"),
        F.col("MAKTX").alias("parent_description"),
    )
)
makt_c = (
    spark.table(tbl("MAKT"))
    .filter((F.col("MANDT") == MANDT) & (F.col("SPRAS") == SPRAS))
    .select(
        strip_zeros(F.col("MATNR")).alias("component_material"),
        F.col("MAKTX").alias("component_description"),
    )
)

if FILTER_PLANT:
    mast = mast.filter(F.col("WERKS") == FILTER_PLANT)
if FILTER_BOM_USAGE:
    mast = mast.filter(F.col("STLAN") == FILTER_BOM_USAGE)

# COMMAND ----------

# Build the direct BOM lookup: MAST → STKO → STAS → STPO.

bom_direct_df = (
    mast.alias("m")
    .join(
        stko.alias("k"),
        (F.col("m.STLNR") == F.col("k.STLNR"))
        & (F.col("m.STLAL") == F.col("k.STLAL")),
    )
    .join(
        stas.alias("s"),
        (F.col("k.STLTY") == F.col("s.STLTY"))
        & (F.col("k.STLNR") == F.col("s.STLNR"))
        & (F.col("k.STLAL") == F.col("s.STLAL")),
    )
    .join(
        stpo.alias("p"),
        (F.col("s.STLTY") == F.col("p.STLTY"))
        & (F.col("s.STLNR") == F.col("p.STLNR"))
        & (F.col("s.STLKN") == F.col("p.STLKN")),
    )
    .select(
        F.col("m.MATNR").alias("parent_matnr"),
        F.col("m.WERKS").alias("plant"),
        F.col("m.STLAN").alias("bom_usage"),
        F.col("k.STLTY").alias("bom_category"),
        F.col("k.STLNR").alias("bom_number"),
        F.col("k.STLAL").alias("alt_bom"),
        F.col("p.POSNR").alias("item_number"),
        F.col("p.IDNRK").alias("component_matnr"),
        F.col("p.MENGE").alias("qty"),
        F.col("p.MEINS").alias("uom"),
        F.col("p.POSTP").alias("item_category"),
        F.col("p.DATUV").alias("valid_from"),
        F.col("p.STLKN").alias("item_node"),
        F.col("p.STPOZ").alias("item_counter"),
    )
)

# Layer 1 — Alt BOM selection: for each (parent_matnr, plant, bom_usage), keep only
# the lowest-numbered alternative BOM.
_w_alt = Window.partitionBy("parent_matnr", "plant", "bom_usage")
bom_direct_df = (
    bom_direct_df
    .withColumn("_min_alt", F.min("alt_bom").over(_w_alt))
    .filter(F.col("alt_bom") == F.col("_min_alt"))
    .drop("_min_alt")
)

# Layer 2 — Revision-letter dedup: keep only the highest revision letter per
# base number per BOM (e.g. 7072219A–I removed, 7072219J kept).
_REV_PATTERN = r'^([0-9]+)([A-Z])$'

bom_direct_df = (
    bom_direct_df
    .withColumn("_rev_base",   F.regexp_extract(F.col("component_matnr"), _REV_PATTERN, 1))
    .withColumn("_rev_letter", F.regexp_extract(F.col("component_matnr"), _REV_PATTERN, 2))
)

_rev_rows     = bom_direct_df.filter(F.col("_rev_base") != "")
_non_rev_rows = bom_direct_df.filter(F.col("_rev_base") == "")

_w_rev = Window.partitionBy("bom_number", "_rev_base").orderBy(F.col("_rev_letter").desc())
_rev_deduped = (
    _rev_rows
    .withColumn("_rn", F.row_number().over(_w_rev))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

bom_direct_df = (
    _non_rev_rows
    .unionByName(_rev_deduped)
    .drop("_rev_base", "_rev_letter")
)

# Layer 3 — Same-qty position dedup: for each (bom_number, alt_bom, component_matnr,
# qty, uom), keep only the entry with the latest valid_from.
_w_pos = Window.partitionBy("bom_number", "alt_bom", "component_matnr", "qty", "uom").orderBy(F.col("valid_from").desc())
bom_direct_df = (
    bom_direct_df
    .withColumn("_pos_rn", F.row_number().over(_w_pos))
    .filter(F.col("_pos_rn") == 1)
    .drop("_pos_rn")
)

# Layer 4 — Item-number dedup: for each (bom_number, alt_bom, item_number), keeps
# only the entry with the highest item_node (STLKN). SAP assigns STLKNs sequentially, so the
# highest STLKN is the most recently created ECO and is the authoritative active component.
# Ordering by valid_from is incorrect — a superseded ECO can carry a future DATUV that
# appears newer than the true active component.
_w_item = Window.partitionBy("bom_number", "alt_bom", "item_number").orderBy(F.col("item_node").desc())
bom_direct_df = (
    bom_direct_df
    .withColumn("_item_rn", F.row_number().over(_w_item))
    .filter(F.col("_item_rn") == 1)
    .drop("_item_rn")
)

# Materialize to a managed Delta table to break source-table lineage.
# Required for serverless — .cache() / .persist() are not supported.
print(f"Writing bom_direct to {SCRATCH_TABLE} ...")
bom_direct_df.write.mode("overwrite").saveAsTable(SCRATCH_TABLE)
print(f"Done — {spark.table(SCRATCH_TABLE).count():,} rows")

spark.table(SCRATCH_TABLE).createOrReplaceTempView("v_bom_direct")

# COMMAND ----------

# Iterative multi-level BOM explosion.
#
# expand_one_level aliases the parent sub-select as "par" to resolve ambiguity:
# both "par" and "bd" carry a bom_usage column after the join, so the join
# condition and any reference to the parent's bom_usage must use "par.bom_usage".
#
# bom_usage is constrained in the join so a production BOM (usage=1) only expands
# sub-assemblies that also have a production BOM — it never picks up engineering
# BOMs (usage=2) deeper in the chain.
#
# top_plant constrains all sub-assembly lookups to the top-level BOM's plant.

def expand_one_level(level_num, parent_df):
    bd  = spark.table("v_bom_direct").alias("bd")
    par = (
        parent_df
        .select(
            "top_material",
            "top_plant",
            "bom_usage",
            F.col("component_material").alias("expand_matnr"),
            "sort_path",
        )
        .alias("par")
    )
    return (
        par
        .join(
            bd,
            (F.col("par.expand_matnr") == F.col("bd.parent_matnr"))
            & (F.col("par.top_plant")  == F.col("bd.plant"))
            & (F.col("par.bom_usage")  == F.col("bd.bom_usage")),
        )
        .select(
            F.col("par.top_material"),
            F.col("par.top_plant"),
            F.col("par.expand_matnr").alias("parent_material"),
            F.col("bd.component_matnr").alias("component_material"),
            F.lit(level_num).cast("int").alias("bom_level"),
            F.concat(
                F.col("par.sort_path"),
                F.lit("|"),
                F.lpad(F.col("bd.item_number").cast("string"), 8, "0"),
            ).alias("sort_path"),
            F.col("bd.plant"),
            F.col("bd.bom_usage"),
            F.col("bd.bom_category"),
            F.col("bd.bom_number"),
            F.col("bd.alt_bom"),
            F.col("bd.item_number"),
            F.col("bd.qty"),
            F.col("bd.uom"),
            F.col("bd.item_category"),
            F.col("bd.valid_from"),
        )
    )

level_1_source = spark.table("v_bom_direct")

if FILTER_MATERIAL is not None:
    if isinstance(FILTER_MATERIAL, list):
        level_1_source = level_1_source.filter(F.col("parent_matnr").isin(FILTER_MATERIAL))
    else:
        level_1_source = level_1_source.filter(F.col("parent_matnr") == FILTER_MATERIAL)

level_1 = (
    level_1_source
    .select(
        F.col("parent_matnr").alias("top_material"),
        F.col("plant").alias("top_plant"),
        F.col("parent_matnr").alias("parent_material"),
        F.col("component_matnr").alias("component_material"),
        F.lit(1).cast("int").alias("bom_level"),
        F.concat(
            F.col("parent_matnr"),
            F.lit("_"),
            F.col("plant"),
            F.lit("|"),
            F.lpad(F.col("item_number").cast("string"), 8, "0"),
        ).alias("sort_path"),
        "plant", "bom_usage", "bom_category", "bom_number", "alt_bom",
        "item_number", "qty", "uom", "item_category", "valid_from",
    )
)

level_1.createOrReplaceTempView("v_bom_lvl_1")
all_level_views = ["v_bom_lvl_1"]
current = spark.table("v_bom_lvl_1")

for lvl in range(2, MAX_LEVELS + 1):
    next_lvl = expand_one_level(lvl, current)
    row_count = next_lvl.count()
    if row_count == 0:
        print(f"Explosion complete — deepest level reached: {lvl - 1}")
        break
    print(f"Level {lvl}: {row_count:,} items found")
    view_name = f"v_bom_lvl_{lvl}"
    next_lvl.createOrReplaceTempView(view_name)
    all_level_views.append(view_name)
    current = spark.table(view_name)

all_levels_dfs = [spark.table(v) for v in all_level_views]
bom_exploded_df = reduce(lambda a, b: a.union(b), all_levels_dfs)
bom_exploded_df.createOrReplaceTempView("v_bom_exploded")

# COMMAND ----------

# Add material descriptions and produce final sorted output.

result = (
    spark.table("v_bom_exploded")
    .join(makt_p, on="parent_material", how="left")
    .join(makt_c, on="component_material", how="left")
    .select(
        "top_material",
        "top_plant",
        "bom_level",
        "parent_material",
        "parent_description",
        "component_material",
        "component_description",
        "plant",
        "bom_usage",
        "bom_number",
        "alt_bom",
        "bom_category",
        "item_number",
        "qty",
        "uom",
        "item_category",
        "valid_from",
    )
    .orderBy("top_material", "top_plant", "sort_path")
)

display(result)

result.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hub_live_transformed.sap.sap_bom")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC Run this cell after reviewing results to drop the scratch table.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SCRATCH_TABLE}")
print(f"Dropped {SCRATCH_TABLE}")
