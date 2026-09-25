# Databricks notebook source
# MAGIC %md
# MAGIC # SAP Data Model — DDL
# MAGIC
# MAGIC Creates the `hub_live_transformed.sap` schema and all tables.
# MAGIC
# MAGIC Run this notebook once before the first data load, or after a schema change.
# MAGIC
# MAGIC **Target:** `hub_live_transformed.sap`
# MAGIC
# MAGIC **Tables defined here:**
# MAGIC | Table | Mirrors | Description |
# MAGIC |-------|---------|-------------|
# MAGIC | inventory | SAP MB52 | Stock balances by material, plant, storage location, and special stock type |
# MAGIC | purchase_order | SAP ME2M | Purchase order and STO schedule lines |
# MAGIC | material_reservation | SAP MB25 | Material reservations and dependent requirements |
# MAGIC | purchase_requisition | SAP ME5A | Purchase requisition items with PO and GR linkage |
# MAGIC | approved_mfg_part_list | — | Approved Manufacturer Parts List |
# MAGIC | bill_of_materials | — | Multi-level BOM explosion |
# MAGIC
# MAGIC > **Note on surrogate keys:** Each table defines a `GENERATED ALWAYS AS IDENTITY` surrogate key.
# MAGIC > These are preserved when loading via `INSERT INTO`. Notebooks that use `saveAsTable("overwrite")`
# MAGIC > with `overwriteSchema = true` will replace the schema on each run; migrate to `INSERT INTO` to
# MAGIC > retain identity columns in production.

# COMMAND ----------

# DBTITLE 1,Create Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hub_live_transformed.sap

# COMMAND ----------

# DBTITLE 1,inventory
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.inventory (
# MAGIC
# MAGIC     inventory_id                BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each inventory record',
# MAGIC
# MAGIC     material_number             STRING          COMMENT 'SAP material number (MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description        STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     plant                       STRING          COMMENT 'Plant code (WERKS)',
# MAGIC     profit_center               STRING          COMMENT 'Profit center assigned to the material at plant level (MARC.PRCTR)',
# MAGIC     division                    STRING          COMMENT 'Division assigned to the material (MARA.SPART)',
# MAGIC     storage_location            STRING          COMMENT 'Storage location within the plant (LGORT). NULL for vendor stock (MSSL) as those parts are at the vendor site',
# MAGIC     special_stock_indicator     STRING          COMMENT 'Special stock type: blank = standard plant stock | E = sales order stock (MSKA) | Q = project/WBS stock (MSPR) | O = subcontracting at vendor (MSSL) | V = returnable packaging at vendor (MSSL)',
# MAGIC     material_type               STRING          COMMENT 'Material type code (MARA.MTART), e.g. ROH = raw material, HALB = semi-finished, FERT = finished goods',
# MAGIC     abc_indicator               STRING          COMMENT 'ABC classification for material planning at plant level (MARC.ABCIN): A = high value, B = medium, C = low',
# MAGIC     base_unit_of_measure        STRING          COMMENT 'Base unit of measure for all stock quantities (MARA.MEINS)',
# MAGIC     material_group              STRING          COMMENT 'Material group / commodity code (MARA.MATKL)',
# MAGIC     valuation_class             STRING          COMMENT 'Valuation class controlling G/L account determination for inventory postings (MBEW.BKLAS)',
# MAGIC     unit_price                  DECIMAL(18,4)   COMMENT 'Standard price per base unit of measure in local currency (MBEW.STPRS / MBEW.PEINH)',
# MAGIC     unrestricted_qty            DECIMAL(18,3)   COMMENT 'Unrestricted-use stock; fully available for MRP, sales, and production (LABST)',
# MAGIC     quality_inspection_qty      DECIMAL(18,3)   COMMENT 'Stock in quality inspection; received but not yet released for use (INSME)',
# MAGIC     blocked_qty                 DECIMAL(18,3)   COMMENT 'Blocked stock; excluded from MRP and not available for use (SPEME)',
# MAGIC     transfer_qty                DECIMAL(18,3)   COMMENT 'Stock in transit during plant-to-plant stock transfer. Standard plant stock only (MARD.UMLME)',
# MAGIC     restricted_use_qty          DECIMAL(18,3)   COMMENT 'Restricted-use stock; available for planning but subject to restrictions. Standard plant stock only (MARD.EINME)',
# MAGIC     returns_qty                 DECIMAL(18,3)   COMMENT 'Returns stock from customer deliveries pending inspection or reprocessing. Standard plant stock only (MARD.RETME)',
# MAGIC     total_qty                   DECIMAL(18,3)   COMMENT 'Total stock across all categories: unrestricted + QI + blocked + transfer + restricted + returns',
# MAGIC     total_value                 DECIMAL(18,2)   COMMENT 'Total inventory value in local currency: total_qty × unit_price'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Inventory stock balances by material, plant, and storage location. Mirrors SAP MB52. Covers standard plant stock (MARD) and special stock types: sales order (MSKA, S=E), project/WBS (MSPR, S=Q), and vendor subcontracting/returnable packaging (MSSL, S=O/V). Only records with at least one non-zero stock quantity are included. Special stock W (customer consignment, MSKU) excluded — source table not accessible. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,purchase_order
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.purchase_order (
# MAGIC
# MAGIC     purchase_order_id               BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each schedule line record',
# MAGIC
# MAGIC     purchasing_document_number      STRING          COMMENT 'Purchase order number (EKKO.EBELN). Leading zeros removed for numeric values',
# MAGIC     purchasing_document_item        STRING          COMMENT 'Line item number within the purchase order (EKPO.EBELP). Leading zeros removed',
# MAGIC     schedule_line_counter           STRING          COMMENT 'Delivery schedule line counter within the PO item (EKET.ETENR)',
# MAGIC     vendor_account_number           STRING          COMMENT 'SAP vendor account number (EKKO.LIFNR). Leading zeros removed for numeric values',
# MAGIC     vendor_name                     STRING          COMMENT 'Vendor name from vendor master (LFA1.NAME1)',
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (EKPO.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description            STRING          COMMENT 'Material short text (MAKT.MAKTX if MATNR is set, otherwise EKPO.TXZ01 free-text description)',
# MAGIC     po_unit_of_measure              STRING          COMMENT 'Unit of measure for the ordered quantity (EKPO.MEINS)',
# MAGIC     po_currency                     STRING          COMMENT 'Currency of the purchase order (EKKO.WAERS)',
# MAGIC     plant                           STRING          COMMENT 'Receiving plant (EKPO.WERKS)',
# MAGIC     material_group                  STRING          COMMENT 'Material group / commodity code (EKPO.MATKL)',
# MAGIC     purchasing_document_date        STRING          COMMENT 'Date the purchase order was created (EKKO.BEDAT, format YYYYMMDD)',
# MAGIC     item_delivery_date              STRING          COMMENT 'Requested delivery date for the schedule line (EKET.EINDT, format YYYYMMDD)',
# MAGIC     statistics_delivery_date        STRING          COMMENT 'Statistical delivery date for reporting (EKET.SLFDT, format YYYYMMDD)',
# MAGIC     local_currency                  STRING          COMMENT 'Local currency of the company code (T001.WAERS)',
# MAGIC     po_quantity                     DECIMAL(18,3)   COMMENT 'Total ordered quantity at the PO item level (EKPO.MENGE)',
# MAGIC     scheduled_quantity              DECIMAL(18,3)   COMMENT 'Quantity for this specific schedule line (EKET.MENGE)',
# MAGIC     gr_quantity                     DECIMAL(18,3)   COMMENT 'Goods receipt quantity posted against this schedule line (EKET.WEMNG)',
# MAGIC     quantity_to_be_delivered        DECIMAL(18,3)   COMMENT 'Outstanding open quantity: scheduled_quantity minus gr_quantity',
# MAGIC     purchase_requisition_number     STRING          COMMENT 'Purchase requisition that originated this PO item (EKET.BANFN). Leading zeros removed',
# MAGIC     price_unit                      DECIMAL(18,3)   COMMENT 'Price unit — the quantity basis for the net price (EKPO.PEINH)',
# MAGIC     net_price                       DECIMAL(18,4)   COMMENT 'Net price per price unit in PO currency (EKPO.NETPR)',
# MAGIC     total_value                     DECIMAL(18,2)   COMMENT 'Total line value: scheduled_quantity × net_price / price_unit',
# MAGIC     gr_indicator                    STRING          COMMENT 'Goods receipt required indicator (EKPO.WEPOS): X = GR required',
# MAGIC     invoice_receipt_indicator       STRING          COMMENT 'Invoice receipt required indicator (EKPO.REPOS): X = IR required',
# MAGIC     item_category                   STRING          COMMENT 'PO item category (EKPO.PSTYP): blank = standard, D = service, K = consignment',
# MAGIC     account_assignment_category     STRING          COMMENT 'Cost object type (EKPO.KNTTP): blank = stock, K = cost center, F = order, P = WBS element',
# MAGIC     deletion_indicator              STRING          COMMENT 'Deletion flag for the PO item (EKPO.LOEKZ): X = deleted',
# MAGIC     delivery_completed_indicator    STRING          COMMENT 'Delivery completed flag (EKPO.ELIKZ): X = final delivery posted',
# MAGIC     po_status                       STRING          COMMENT 'Derived status: Deleted | Delivery Complete | Fully Received | Partially Received | Open',
# MAGIC     purchasing_group                STRING          COMMENT 'Purchasing group / buyer code (EKKO.EKGRP)',
# MAGIC     storage_location                STRING          COMMENT 'Destination storage location within the plant (EKPO.LGORT)',
# MAGIC     purchasing_organization         STRING          COMMENT 'Purchasing organization (EKKO.EKORG)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Purchase order and stock transfer order (STO) schedule lines. Mirrors SAP ME2M. Granularity: one row per schedule line (EKKO + EKPO + EKET). PO text block excluded — requires STXH/STXL aggregation. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,material_reservation
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.material_reservation (
# MAGIC
# MAGIC     material_reservation_id         BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each reservation line',
# MAGIC
# MAGIC     reservation_number              STRING          COMMENT 'SAP reservation or dependent requirement number (RESB.RSNUM)',
# MAGIC     reservation_item                STRING          COMMENT 'Line item number within the reservation (RESB.RSPOS)',
# MAGIC     order_number                    STRING          COMMENT 'Production, maintenance, or internal order number associated with the reservation (RESB.AUFNR). Leading zeros removed for numeric values',
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (RESB.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description            STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     plant                           STRING          COMMENT 'Plant where the material is required (RESB.WERKS)',
# MAGIC     storage_location                STRING          COMMENT 'Storage location from which the material will be issued (RESB.LGORT)',
# MAGIC     special_stock_indicator         STRING          COMMENT 'Special stock type for the reserved material (RESB.SOBKZ), e.g. E = sales order, Q = project, O = subcontracting',
# MAGIC     reservation_type                STRING          COMMENT 'Record type distinguishing reservations from dependent requirements (RESB.RSART)',
# MAGIC     account_assignment_category     STRING          COMMENT 'Cost object type for consumption posting (RESB.KNTTP): K = cost center, F = order, P = WBS element, A = asset',
# MAGIC     movement_type                   STRING          COMMENT 'Inventory movement type used when goods are issued against this reservation (RESB.BWART)',
# MAGIC     requirement_date                STRING          COMMENT 'Date by which the material must be available (RESB.BDTER, format YYYYMMDD)',
# MAGIC     base_unit_of_measure            STRING          COMMENT 'Base unit of measure for all quantities (RESB.MEINS)',
# MAGIC     requirement_qty                 DECIMAL(13,3)   COMMENT 'Total quantity of material required for this reservation line (RESB.BDMNG)',
# MAGIC     withdrawn_qty                   DECIMAL(13,3)   COMMENT 'Quantity already issued against this reservation (RESB.ENMNG)',
# MAGIC     difference_qty                  DECIMAL(13,3)   COMMENT 'Outstanding open quantity remaining to be issued (RESB.BDMNG - RESB.ENMNG)',
# MAGIC     final_issue_indicator           STRING          COMMENT 'Flags that the final goods issue has been posted (RESB.KZEAR): X = final issue complete, blank = open'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Material reservations and dependent requirements by reservation line. Mirrors SAP MB25. Excludes logically deleted reservation lines (XLOEK = X). Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,purchase_requisition
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.purchase_requisition (
# MAGIC
# MAGIC     purchase_requisition_id         BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each PR item',
# MAGIC
# MAGIC     purchase_requisition_number     STRING          COMMENT 'Purchase requisition number (EBAN.BANFN). Leading zeros removed for numeric values',
# MAGIC     purchase_requisition_item       STRING          COMMENT 'Line item number within the requisition (EBAN.BNFPO)',
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (EBAN.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description            STRING          COMMENT 'Material short text (MAKT.MAKTX if MATNR is set, otherwise EBAN.TXZ01 free-text description)',
# MAGIC     purchasing_group                STRING          COMMENT 'Purchasing group / buyer code (EBAN.EKGRP)',
# MAGIC     material_group                  STRING          COMMENT 'Material group / commodity code (EBAN.MATKL)',
# MAGIC     processing_status               STRING          COMMENT 'Processing status of the PR item (EBAN.STATU): N = not yet processed, B = PO created, etc.',
# MAGIC     release_status                  STRING          COMMENT 'Overall release (approval) status of the PR (EBAN.FRGST)',
# MAGIC     release_indicator               STRING          COMMENT 'Release indicator showing which approval levels have been granted (EBAN.FRGKZ)',
# MAGIC     purchasing_document_number      STRING          COMMENT 'Purchase order number linked to this PR item (EBAN.EBELN). Leading zeros removed',
# MAGIC     vendor_account_number           STRING          COMMENT 'Preferred vendor for the PR item (EBAN.LIFNR). Leading zeros removed',
# MAGIC     pr_closed                       STRING          COMMENT 'Flag indicating the PR item is closed (EBAN.EBAKZ): X = closed',
# MAGIC     plant                           STRING          COMMENT 'Plant where the material is required (EBAN.WERKS)',
# MAGIC     requisition_date                STRING          COMMENT 'Date the PR item was created (EBAN.BADAT, format YYYYMMDD)',
# MAGIC     release_date                    STRING          COMMENT 'Date the PR item was fully released / approved (EBAN.FRGDT, format YYYYMMDD)',
# MAGIC     item_delivery_date              STRING          COMMENT 'Requested delivery date for the PR item (EBAN.LFDAT, format YYYYMMDD)',
# MAGIC     unit_of_measure                 STRING          COMMENT 'Base unit of measure for the requisition quantity (EBAN.MEINS)',
# MAGIC     currency                        STRING          COMMENT 'Currency of the price on the PR item (EBAN.WAERS)',
# MAGIC     planned_delivery_time_days      DECIMAL(5,0)    COMMENT 'Planned delivery time in calendar days from the vendor (EBAN.PLIFZ)',
# MAGIC     pr_quantity                     DECIMAL(18,3)   COMMENT 'Requested quantity on the PR item (EBAN.MENGE)',
# MAGIC     pr_price                        DECIMAL(18,4)   COMMENT 'Estimated price per price unit on the PR item (EBAN.PREIS)',
# MAGIC     price_unit                      DECIMAL(18,3)   COMMENT 'Price unit — the quantity basis for pr_price (EBAN.PEINH)',
# MAGIC     committed_quantity              DECIMAL(18,3)   COMMENT 'Quantity already committed / reserved in planning (EBAN.MNG02)',
# MAGIC     ordered_quantity                DECIMAL(18,3)   COMMENT 'Quantity already converted to a purchase order (EBAN.BSMNG)',
# MAGIC     net_price_po                    DECIMAL(18,4)   COMMENT 'Net price from the linked PO item (EKPO.NETPR)',
# MAGIC     gr_quantity                     DECIMAL(18,3)   COMMENT 'Total goods receipt quantity posted against the linked PO item (sum of EKET.WEMNG)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Purchase requisition items with linked PO and GR quantities. Mirrors SAP ME5A. Granularity: one row per PR item (EBAN). Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,approved_mfg_part_list
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.approved_mfg_part_list (
# MAGIC
# MAGIC     approved_mfg_part_list_id       BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each approved manufacturer record',
# MAGIC
# MAGIC     mpn_number                      STRING          COMMENT 'Manufacturer part number in SAP — the EMATN field from AMPL, leading zeros stripped for numeric values',
# MAGIC     material_number                 STRING          COMMENT 'Internal Mirion material number (AMPL.BMATN if populated, otherwise AMPL.EMATN). Leading zeros stripped',
# MAGIC     material_description            STRING          COMMENT 'Internal material description in English (MAKT.MAKTX via AMPL.BMATN → MAKT.MATNR)',
# MAGIC     manufacturer_name               STRING          COMMENT 'Manufacturer name from vendor master (LFA1.NAME1 via AMPL.MFRNR → LFA1.LIFNR)',
# MAGIC     manufacturer_number             STRING          COMMENT 'SAP vendor number for the manufacturer (AMPL.MFRNR)',
# MAGIC     manufacturer_material_number    STRING          COMMENT 'Manufacturer own part number (MARA.MFRPN via AMPL.EMATN → MARA.MATNR)',
# MAGIC     plant                           STRING          COMMENT 'Plant where this manufacturer approval applies (AMPL.WERKS). Rows with NULL plant excluded',
# MAGIC     valid_from                      STRING          COMMENT 'Date this approval becomes effective (AMPL.DATUV, format YYYYMMDD)',
# MAGIC     valid_to                        STRING          COMMENT 'Date this approval expires (AMPL.DATUB, format YYYYMMDD)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Approved Manufacturer Parts List — one row per approved manufacturer per internal material per plant. Source: median_hub_captured.sap (AMPL, MARA, MAKT, LFA1).'

# COMMAND ----------

# DBTITLE 1,bill_of_materials
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.bill_of_materials (
# MAGIC
# MAGIC     bill_of_materials_id        BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each BOM explosion row',
# MAGIC
# MAGIC     top_material                STRING          COMMENT 'Top-level assembly material number at the root of this explosion path',
# MAGIC     top_plant                   STRING          COMMENT 'Plant of the top-level assembly. All sub-assembly lookups are constrained to this plant',
# MAGIC     bom_level                   INT             COMMENT 'Depth in the BOM hierarchy: 1 = direct child of the top material',
# MAGIC     parent_material             STRING          COMMENT 'Material number of the immediate parent assembly at this level',
# MAGIC     parent_description          STRING          COMMENT 'Material short text of the parent assembly (MAKT.MAKTX, SPRAS = E)',
# MAGIC     component_material          STRING          COMMENT 'Material number of the component at this BOM level. Leading zeros stripped',
# MAGIC     component_description       STRING          COMMENT 'Material short text of the component (MAKT.MAKTX, SPRAS = E)',
# MAGIC     plant                       STRING          COMMENT 'Plant for this BOM relationship (MAST.WERKS)',
# MAGIC     bom_usage                   STRING          COMMENT 'BOM usage type (MAST.STLAN): 1 = Production, 2 = Engineering, 5 = Sales',
# MAGIC     bom_number                  STRING          COMMENT 'BOM document number (STKO.STLNR)',
# MAGIC     alt_bom                     STRING          COMMENT 'Alternative BOM number within the BOM group (STKO.STLAL). Only the lowest-numbered alternative is retained',
# MAGIC     bom_category                STRING          COMMENT 'BOM category (STKO.STLTY): M = material BOM',
# MAGIC     item_number                 STRING          COMMENT 'Item position number within the BOM (STPO.POSNR)',
# MAGIC     qty                         DECIMAL(18,3)   COMMENT 'Required quantity of the component per parent assembly (STPO.MENGE)',
# MAGIC     uom                         STRING          COMMENT 'Unit of measure for the component quantity (STPO.MEINS)',
# MAGIC     item_category               STRING          COMMENT 'BOM item category (STPO.POSTP): L = stock item, N = non-stock, D = document',
# MAGIC     valid_from                  STRING          COMMENT 'Effective-from date for this BOM item revision (STPO.DATUV, format YYYYMMDD)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Multi-level BOM explosion — one row per component at each level of the assembly hierarchy. Four deduplication layers applied: alt BOM selection, revision-letter dedup, same-qty position dedup, and item-number dedup. Source: median_hub_captured.sap (MAST, STKO, STAS, STPO, MAKT).'
