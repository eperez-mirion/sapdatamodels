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
# MAGIC | purchase_order | SAP ME2M | Standard purchase order schedule lines (doc range 0004xxxxxx) |
# MAGIC | stock_transfer | SAP ME2M | Internal stock transfer order schedule lines (doc range 0003xxxxxx) |
# MAGIC | material_reservation | SAP MB25 | Material reservations and dependent requirements |
# MAGIC | purchase_requisition | SAP ME5A | Purchase requisition items with PO and GR linkage |
# MAGIC | approved_mfg_part_list | — | Approved Manufacturer Parts List |
# MAGIC | bill_of_materials | — | Multi-level BOM explosion |
# MAGIC | material_details | — | Material master + plant planning and valuation data |
# MAGIC | material_usage | — | Full goods movement history (MSEG + MKPF) |
# MAGIC | vendor_master | — | Vendor address, contact, email, and purchasing org data |
# MAGIC | material_last_movement | — | Most recent goods movement date per material + plant |
# MAGIC | vendor_otd | — | Vendor on-time delivery by PO schedule line |
# MAGIC | material_class | — | Material type + procurement type → Mirion business classification |
# MAGIC | mirion_customer_numbers | — | Mirion-affiliated customer accounts with standardized site labels |
# MAGIC | mirion_vendor_numbers | — | Mirion-affiliated vendor accounts with standardized site labels |
# MAGIC | movement_type | — | SAP movement type codes with plain-language descriptions and breakdown categories |
# MAGIC | payment_terms | — | SAP payment terms keys with English descriptions |
# MAGIC | purchasing_groups | — | SAP purchasing group codes and buyer names |
# MAGIC | so_document_type | — | Sales order document type codes with Mirion order type classification |
# MAGIC | so_item_category | — | Sales order item category codes with Mirion billing type classification |
# MAGIC | so_reject_codes | — | Sales order rejection reason codes with Mirion status classification |
# MAGIC | storage_locations | — | Storage locations per plant with descriptions |
# MAGIC | delivery_blocks | — | Sales order delivery block codes with descriptions |
# MAGIC | billing_blocks | — | Sales order billing block codes with descriptions |
# MAGIC | profit_center | — | Profit center codes with hierarchy context (site, department, top-level rollup) |
# MAGIC | cost_center | — | Cost center codes with controlling area, validity dates, responsible person, and department |
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
# MAGIC     purchasing_document_number      STRING          COMMENT 'Purchase order number (EKKO.EBELN). Leading zeros removed for numeric values. Range: 0004000000–0004999999',
# MAGIC     purchasing_document_item        STRING          COMMENT 'Line item number within the purchase order (EKPO.EBELP). Leading zeros removed',
# MAGIC     schedule_line_counter           STRING          COMMENT 'Delivery schedule line counter within the PO item (EKET.ETENR)',
# MAGIC     purchasing_document_type        STRING          COMMENT 'SAP document type code (EKKO.BSART), e.g. NB = standard PO',
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
# MAGIC COMMENT 'Standard purchase order schedule lines (document range 0004xxxxxx). Mirrors SAP ME2M. Granularity: one row per schedule line (EKKO + EKPO + EKET). PO text block excluded — requires STXH/STXL aggregation. Stock Transfer Orders are in the stock_transfer table. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,stock_transfer
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.stock_transfer (
# MAGIC
# MAGIC     stock_transfer_id               BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each schedule line record',
# MAGIC
# MAGIC     purchasing_document_number      STRING          COMMENT 'Stock transfer order number (EKKO.EBELN). Leading zeros removed. Range: 0003000000–0003999999',
# MAGIC     purchasing_document_item        STRING          COMMENT 'Line item number within the STO (EKPO.EBELP). Leading zeros removed',
# MAGIC     schedule_line_counter           STRING          COMMENT 'Delivery schedule line counter within the STO item (EKET.ETENR)',
# MAGIC     purchasing_document_type        STRING          COMMENT 'SAP document type code (EKKO.BSART), e.g. UB = stock transfer order',
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (EKPO.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description            STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     po_unit_of_measure              STRING          COMMENT 'Unit of measure for the ordered quantity (EKPO.MEINS)',
# MAGIC     receiving_plant                 STRING          COMMENT 'Plant receiving the transferred stock (EKPO.WERKS)',
# MAGIC     supplying_plant                 STRING          COMMENT 'Plant issuing / shipping the stock (EKPO.RESWK)',
# MAGIC     material_group                  STRING          COMMENT 'Material group / commodity code (EKPO.MATKL)',
# MAGIC     purchasing_document_date        STRING          COMMENT 'Date the STO was created (EKKO.BEDAT, format YYYYMMDD)',
# MAGIC     item_delivery_date              STRING          COMMENT 'Requested delivery date for the schedule line (EKET.EINDT, format YYYYMMDD)',
# MAGIC     statistics_delivery_date        STRING          COMMENT 'Statistical delivery date for reporting (EKET.SLFDT, format YYYYMMDD)',
# MAGIC     local_currency                  STRING          COMMENT 'Local currency of the company code (T001.WAERS)',
# MAGIC     po_quantity                     DECIMAL(18,3)   COMMENT 'Total ordered quantity at the STO item level (EKPO.MENGE)',
# MAGIC     scheduled_quantity              DECIMAL(18,3)   COMMENT 'Quantity for this specific schedule line (EKET.MENGE)',
# MAGIC     gr_quantity                     DECIMAL(18,3)   COMMENT 'Goods receipt quantity posted against this schedule line (EKET.WEMNG)',
# MAGIC     quantity_to_be_delivered        DECIMAL(18,3)   COMMENT 'Outstanding open quantity: scheduled_quantity minus gr_quantity',
# MAGIC     price_unit                      DECIMAL(18,3)   COMMENT 'Price unit — the quantity basis for the net price (EKPO.PEINH)',
# MAGIC     net_price                       DECIMAL(18,4)   COMMENT 'Net price per price unit (EKPO.NETPR)',
# MAGIC     total_value                     DECIMAL(18,2)   COMMENT 'Total line value: scheduled_quantity × net_price / price_unit',
# MAGIC     gr_indicator                    STRING          COMMENT 'Goods receipt required indicator (EKPO.WEPOS): X = GR required',
# MAGIC     item_category                   STRING          COMMENT 'STO item category (EKPO.PSTYP)',
# MAGIC     deletion_indicator              STRING          COMMENT 'Deletion flag for the STO item (EKPO.LOEKZ): X = deleted',
# MAGIC     delivery_completed_indicator    STRING          COMMENT 'Delivery completed flag (EKPO.ELIKZ): X = final delivery posted',
# MAGIC     sto_status                      STRING          COMMENT 'Derived status: Deleted | Delivery Complete | Fully Received | Partially Received | Open',
# MAGIC     purchasing_group                STRING          COMMENT 'Purchasing group / buyer code (EKKO.EKGRP)',
# MAGIC     storage_location                STRING          COMMENT 'Destination storage location in the receiving plant (EKPO.LGORT)',
# MAGIC     purchasing_organization         STRING          COMMENT 'Purchasing organization (EKKO.EKORG)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Stock transfer order schedule lines (document range 0003xxxxxx). Plant-to-plant internal transfers — no external vendor. Granularity: one row per schedule line (EKKO + EKPO + EKET). Standard Purchase Orders are in the purchase_order table. Source: median_hub_captured.sap.'

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

# COMMAND ----------

# DBTITLE 1,material_details
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.material_details (
# MAGIC
# MAGIC     material_details_id             BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each material + plant record',
# MAGIC
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (MARC.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description            STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     plant                           STRING          COMMENT 'Plant where this material is managed (MARC.WERKS)',
# MAGIC     material_type                   STRING          COMMENT 'Material type code (MARA.MTART): ROH = raw material, HALB = semi-finished, FERT = finished goods, HAWA = trading goods',
# MAGIC     material_group                  STRING          COMMENT 'Material group / commodity code (MARA.MATKL)',
# MAGIC     base_unit_of_measure            STRING          COMMENT 'Base unit of measure for all stock quantities and planning (MARA.MEINS)',
# MAGIC     division                        STRING          COMMENT 'Division assigned to the material (MARA.SPART)',
# MAGIC     old_material_number             STRING          COMMENT 'Legacy or predecessor material number (MARA.BISMT)',
# MAGIC     creation_date                   STRING          COMMENT 'Date the material master record was first created (MARA.ERSDA, format YYYYMMDD)',
# MAGIC     net_weight                      DECIMAL(18,3)   COMMENT 'Net weight of one base unit, excluding packaging (MARA.NTGEW)',
# MAGIC     gross_weight                    DECIMAL(18,3)   COMMENT 'Gross weight of one base unit, including packaging (MARA.BRGEW)',
# MAGIC     weight_unit                     STRING          COMMENT 'Unit of weight for net_weight and gross_weight (MARA.GEWEI): KG, G, LB, etc.',
# MAGIC     volume                          DECIMAL(18,3)   COMMENT 'Volume of one base unit (MARA.VOLUM)',
# MAGIC     volume_unit                     STRING          COMMENT 'Unit of volume (MARA.VOLEH): L, ML, CM3, etc.',
# MAGIC     manufacturer_part_number        STRING          COMMENT 'Manufacturer own part number as stored on the material master (MARA.MFRPN)',
# MAGIC     profit_center                   STRING          COMMENT 'Profit center assigned to this material at plant level (MARC.PRCTR)',
# MAGIC     mrp_type                        STRING          COMMENT 'MRP planning procedure (MARC.DISMM): PD = MRP, VB = reorder point, ND = no planning, MO = manual',
# MAGIC     mrp_controller                  STRING          COMMENT 'MRP controller / planner responsible for this material at this plant (MARC.DISPO)',
# MAGIC     lot_sizing_procedure            STRING          COMMENT 'Lot sizing procedure for MRP (MARC.DISLS): EX = lot-for-lot, FX = fixed lot size, HB = replenish to max level',
# MAGIC     procurement_type                STRING          COMMENT 'Procurement type (MARC.BESKZ): E = in-house production, F = external procurement, X = both',
# MAGIC     special_procurement_type        STRING          COMMENT 'Special procurement key (MARC.SOBSL): controls subcontracting, consignment, phantom assembly, etc.',
# MAGIC     plant_material_status           STRING          COMMENT 'Plant-specific material status (MARC.MMSTA); restricts certain transactions such as GR, GI, or PO creation when set',
# MAGIC     abc_indicator                   STRING          COMMENT 'ABC classification at plant level (MARC.ABCIN): A = high value/volume, B = medium, C = low',
# MAGIC     purchasing_group                STRING          COMMENT 'Purchasing group / buyer responsible for procuring this material at this plant (MARC.EKGRP)',
# MAGIC     planned_delivery_time_days      DECIMAL(5,0)    COMMENT 'Planned delivery time from vendor in calendar days (MARC.PLIFZ)',
# MAGIC     gr_processing_time_days         DECIMAL(5,0)    COMMENT 'Time in workdays to process a goods receipt into usable stock (MARC.WEBAZ)',
# MAGIC     safety_stock_qty                DECIMAL(18,3)   COMMENT 'Safety stock level — MRP will not plan below this quantity (MARC.EISBE)',
# MAGIC     reorder_point                   DECIMAL(18,3)   COMMENT 'Reorder point — MRP triggers a replenishment proposal when stock falls below this level (MARC.MINBE)',
# MAGIC     maximum_stock_level             DECIMAL(18,3)   COMMENT 'Maximum stock level — target replenishment quantity for HB lot sizing (MARC.MABST)',
# MAGIC     valuation_class                 STRING          COMMENT 'Valuation class controlling G/L account assignment for inventory postings (MBEW.BKLAS)',
# MAGIC     price_control                   STRING          COMMENT 'Price control indicator (MBEW.VPRSV): S = standard price, V = moving average price',
# MAGIC     standard_price                  DECIMAL(18,4)   COMMENT 'Standard price per price unit in local currency (MBEW.STPRS); used when price_control = S',
# MAGIC     moving_average_price            DECIMAL(18,4)   COMMENT 'Moving average price per price unit in local currency (MBEW.VERPR); used when price_control = V',
# MAGIC     price_unit                      DECIMAL(18,3)   COMMENT 'Price unit — the quantity basis for standard_price and moving_average_price (MBEW.PEINH)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Material master and plant planning data — one row per material per plant. Combines MARC plant data with MARA general data, MAKT descriptions, and MBEW valuation. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,material_usage
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.material_usage (
# MAGIC
# MAGIC     material_usage_id           BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each goods movement line',
# MAGIC
# MAGIC     posting_date                STRING          COMMENT 'Date the goods movement was posted to inventory and accounting (MKPF.BUDAT, format YYYYMMDD)',
# MAGIC     document_date               STRING          COMMENT 'Date on the physical source document, e.g. delivery note or production confirmation (MKPF.BLDAT, format YYYYMMDD)',
# MAGIC     material_document_number    STRING          COMMENT 'Material document number assigned by SAP when the movement is posted (MSEG.MBLNR)',
# MAGIC     material_document_year      STRING          COMMENT 'Fiscal year of the material document (MSEG.MJAHR)',
# MAGIC     material_document_item      STRING          COMMENT 'Line item number within the material document (MSEG.ZEILE)',
# MAGIC     movement_type               STRING          COMMENT 'SAP movement type code (MSEG.BWART): 101 = GR for PO, 261 = GI for production order, 201 = GI for cost center, 311 = plant transfer, etc.',
# MAGIC     debit_credit_indicator      STRING          COMMENT 'Stock direction (MSEG.SHKZG): S = stock increase / debit, H = stock decrease / credit',
# MAGIC     material_number             STRING          COMMENT 'SAP material number (MSEG.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description        STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     material_type               STRING          COMMENT 'Material type code (MARA.MTART): ROH = raw material, HALB = semi-finished, FERT = finished goods',
# MAGIC     material_group              STRING          COMMENT 'Material group / commodity code (MARA.MATKL)',
# MAGIC     plant                       STRING          COMMENT 'Plant where the goods movement occurred (MSEG.WERKS)',
# MAGIC     storage_location            STRING          COMMENT 'Storage location within the plant (MSEG.LGORT)',
# MAGIC     special_stock_indicator     STRING          COMMENT 'Special stock type (MSEG.SOBKZ): blank = standard, E = sales order stock, Q = project stock',
# MAGIC     quantity                    DECIMAL(18,3)   COMMENT 'Quantity moved in the unit of measure of the document line (MSEG.MENGE)',
# MAGIC     unit_of_measure             STRING          COMMENT 'Unit of measure for the movement quantity (MSEG.MEINS)',
# MAGIC     amount_local_currency       DECIMAL(18,2)   COMMENT 'Value of the movement in the company code local currency (MSEG.DMBTR)',
# MAGIC     order_number                STRING          COMMENT 'Production, maintenance, or internal order associated with the movement (MSEG.AUFNR). Leading zeros removed',
# MAGIC     purchase_order_number       STRING          COMMENT 'Purchase order number associated with a GR movement (MSEG.EBELN). Leading zeros removed',
# MAGIC     purchase_order_item         STRING          COMMENT 'PO line item number (MSEG.EBELP). Leading zeros removed',
# MAGIC     cost_center                 STRING          COMMENT 'Cost center charged for a goods issue (MSEG.KOSTL)',
# MAGIC     reservation_number          STRING          COMMENT 'Reservation number fulfilled by this goods issue (MSEG.RSNUM). Leading zeros removed',
# MAGIC     created_by                  STRING          COMMENT 'Username of the SAP user who posted the material document (MKPF.USNAM)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Full goods movement history — one row per material document line item. No movement type or date filters applied; use movement_type and posting_date to slice. Source: median_hub_captured.sap (MSEG, MKPF, MAKT, MARA).'

# COMMAND ----------

# DBTITLE 1,vendor_master
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.vendor_master (
# MAGIC
# MAGIC     vendor_master_id            BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each vendor record',
# MAGIC
# MAGIC     vendor_account_number       STRING          COMMENT 'SAP vendor account number (LFA1.LIFNR). Leading zeros removed for numeric values',
# MAGIC     vendor_name                 STRING          COMMENT 'Primary vendor name line (LFA1.NAME1)',
# MAGIC     vendor_name_2               STRING          COMMENT 'Secondary vendor name line — continuation or trade name (LFA1.NAME2)',
# MAGIC     account_group               STRING          COMMENT 'Vendor account group controlling field selection and number range assignment (LFA1.KTOKK)',
# MAGIC     industry                    STRING          COMMENT 'Industry sector / industry key (LFA1.BRSCH)',
# MAGIC     country                     STRING          COMMENT 'Country key (LFA1.LAND1)',
# MAGIC     region                      STRING          COMMENT 'Region or state within the country (LFA1.REGIO)',
# MAGIC     city                        STRING          COMMENT 'City of the vendor (LFA1.ORT01)',
# MAGIC     postal_code                 STRING          COMMENT 'Postal / ZIP code (LFA1.PSTLZ)',
# MAGIC     street_address              STRING          COMMENT 'Street name and house number (LFA1.STRAS)',
# MAGIC     telephone                   STRING          COMMENT 'Primary telephone number (LFA1.TELF1)',
# MAGIC     fax                         STRING          COMMENT 'Fax number (LFA1.TELFX)',
# MAGIC     tax_number_1                STRING          COMMENT 'Tax number 1 — country-specific; typically EIN in the US (LFA1.STCD1)',
# MAGIC     tax_number_2                STRING          COMMENT 'Tax number 2 — country-specific; typically VAT registration number (LFA1.STCD2)',
# MAGIC     posting_block               STRING          COMMENT 'Central posting block (LFA1.SPERR): X = all financial postings blocked for this vendor',
# MAGIC     central_deletion_flag       STRING          COMMENT 'Central deletion flag (LFA1.LOEVM): X = vendor marked for deletion across all company codes',
# MAGIC     email                       STRING          COMMENT 'Primary email address from SAP address management (ADR6.SMTP_ADDR via LFA1.ADRNR). Default email preferred; NULL if no email on file',
# MAGIC     purchasing_organization     STRING          COMMENT 'Purchasing organization this row applies to (LFM1.EKORG). One row per org the vendor is set up in; vendors not in any org appear once with NULL',
# MAGIC     payment_terms               STRING          COMMENT 'Payment terms key for purchase orders (LFM1.ZTERM), e.g. N030 = net 30 days',
# MAGIC     order_currency              STRING          COMMENT 'Default currency for purchase orders to this vendor (LFM1.WAERS)',
# MAGIC     incoterms                   STRING          COMMENT 'Incoterms code (LFM1.INCO1): EXW, FOB, CIF, DAP, etc.',
# MAGIC     incoterms_description       STRING          COMMENT 'Incoterms location or supplementary description (LFM1.INCO2)',
# MAGIC     minimum_order_value         DECIMAL(18,2)   COMMENT 'Minimum order value in the order currency (LFM1.MINBW)',
# MAGIC     gr_based_iv_indicator       STRING          COMMENT 'GR-based invoice verification flag (LFM1.WEBRE): X = invoice posting requires a goods receipt to exist first',
# MAGIC     purchasing_block            STRING          COMMENT 'Purchasing-organization-level deletion / block flag (LFM1.LOEVM)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Vendor master — one row per vendor per purchasing organization. Reflects that each site maintains its own vendor records in LFM1. Vendors not set up in any purchasing org appear once with NULL org fields. Email from ADR6 is deduplicated to the default address per vendor. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,material_last_movement
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.material_last_movement (
# MAGIC
# MAGIC     material_last_movement_id   BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each material + plant record',
# MAGIC
# MAGIC     material_number             STRING          COMMENT 'SAP material number (MSEG.MATNR). Leading zeros removed for numeric values',
# MAGIC     material_description        STRING          COMMENT 'Material short text in English (MAKT.MAKTX, SPRAS = E)',
# MAGIC     material_type               STRING          COMMENT 'Material type code (MARA.MTART): ROH = raw material, HALB = semi-finished, FERT = finished goods',
# MAGIC     material_group              STRING          COMMENT 'Material group / commodity code (MARA.MATKL)',
# MAGIC     plant                       STRING          COMMENT 'Plant where the last goods movement was posted (MSEG.WERKS)',
# MAGIC     last_movement_date          STRING          COMMENT 'Posting date of the most recent goods movement for this material at this plant (MKPF.BUDAT, format YYYYMMDD)',
# MAGIC     last_movement_type          STRING          COMMENT 'SAP movement type of the most recent goods movement (MSEG.BWART)',
# MAGIC     last_document_number        STRING          COMMENT 'Material document number of the most recent goods movement (MSEG.MBLNR)',
# MAGIC     days_since_last_movement    INT             COMMENT 'Calendar days elapsed between last_movement_date and the date this table was last refreshed. Useful for slow-moving inventory analysis'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Most recent goods movement per material per plant — one row per MATNR + WERKS combination. Derived from MSEG + MKPF using a window function. days_since_last_movement reflects the age of the last activity as of the last notebook run. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,vendor_otd
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.vendor_otd (
# MAGIC
# MAGIC     vendor_otd_id                   BIGINT          GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each PO schedule line record',
# MAGIC
# MAGIC     purchasing_document_number      STRING          COMMENT 'Purchase order number (EKKO.EBELN). Leading zeros removed. Range: 0004000000–0004999999',
# MAGIC     purchasing_document_item        STRING          COMMENT 'PO line item number (EKPO.EBELP). Leading zeros removed',
# MAGIC     schedule_line_counter           STRING          COMMENT 'Delivery schedule line counter within the PO item (EKET.ETENR)',
# MAGIC     purchasing_document_type        STRING          COMMENT 'SAP document type code (EKKO.BSART), e.g. NB = standard PO',
# MAGIC     vendor_account_number           STRING          COMMENT 'SAP vendor account number (EKKO.LIFNR). Leading zeros removed',
# MAGIC     vendor_name                     STRING          COMMENT 'Vendor name from vendor master (LFA1.NAME1)',
# MAGIC     material_number                 STRING          COMMENT 'SAP material number (EKPO.MATNR). Leading zeros removed',
# MAGIC     material_description            STRING          COMMENT 'Material short text (MAKT.MAKTX if MATNR is set, otherwise EKPO.TXZ01 free-text description)',
# MAGIC     material_group                  STRING          COMMENT 'Material group / commodity code (EKPO.MATKL)',
# MAGIC     plant                           STRING          COMMENT 'Receiving plant (EKPO.WERKS)',
# MAGIC     purchasing_group                STRING          COMMENT 'Purchasing group / buyer code (EKKO.EKGRP)',
# MAGIC     purchasing_organization         STRING          COMMENT 'Purchasing organization (EKKO.EKORG)',
# MAGIC     purchasing_document_date        STRING          COMMENT 'Date the PO was created (EKKO.BEDAT, format YYYYMMDD)',
# MAGIC     scheduled_delivery_date         STRING          COMMENT 'Requested delivery date for this schedule line (EKET.EINDT, format YYYYMMDD)',
# MAGIC     statistics_delivery_date        STRING          COMMENT 'Statistical delivery date for reporting (EKET.SLFDT, format YYYYMMDD)',
# MAGIC     scheduled_quantity              DECIMAL(18,3)   COMMENT 'Quantity expected on this delivery schedule line (EKET.MENGE)',
# MAGIC     gr_quantity                     DECIMAL(18,3)   COMMENT 'Goods receipt quantity for this schedule line as maintained by SAP (EKET.WEMNG)',
# MAGIC     open_quantity                   DECIMAL(18,3)   COMMENT 'Quantity still outstanding: scheduled_quantity minus gr_quantity',
# MAGIC     gr_date                         STRING          COMMENT 'Posting date of the completing GR for this schedule line (EKBE.BUDAT, format YYYYMMDD). The completing GR is the first GR event where cumulative received qty reached or exceeded the cumulative scheduled qty threshold for this PO item. NULL if the schedule line has not yet been fully received',
# MAGIC     gr_document_number              STRING          COMMENT 'Accounting document number of the completing GR event (EKBE.BELNR). NULL if not yet fully received',
# MAGIC     delivered_on_time               BOOLEAN         COMMENT 'True = gr_date on or before scheduled_delivery_date; False = GR was late; NULL = not yet fully received',
# MAGIC     days_early_late                 INT             COMMENT 'scheduled_delivery_date minus gr_date in calendar days. Positive = delivered early, negative = delivered late, NULL = not yet fully received',
# MAGIC     otd_status                      STRING          COMMENT 'Derived delivery status: On Time | Late | Overdue (no GR and scheduled date has passed) | Open (no GR and scheduled date is in the future)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Vendor on-time delivery — one row per PO delivery schedule line (EKET). EKBE GR events are matched to schedule lines using a cumulative quantity threshold: schedule lines accumulate scheduled qty by due date order; GR events accumulate received qty by posting date order; each schedule line is scored against the GR that first pushed the running total to its threshold (the completion event). Partial deliveries score as Open/Overdue until the line is fully realized. Standard POs only (doc range 0004xxxxxx). STOs excluded. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,material_class
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.material_class (
# MAGIC
# MAGIC     material_class_id       BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each material type + procurement type combination',
# MAGIC
# MAGIC     material_type           STRING  COMMENT 'SAP material type code (MARA.MTART), e.g. ZERT = finished goods / tradable, ZALB = semi-finished, ZROH = raw material',
# MAGIC     procurement_type        STRING  COMMENT 'SAP procurement type code (MARC.BESKZ): F = external procurement, E = in-house production, X = both, blank = not set',
# MAGIC     material_class          STRING  COMMENT 'Mirion business classification derived from material type and procurement type: Tradable/Resale | Raw | Semi-FG | FG | Operating/Packing Supplies | Service | Non-Stock Material | Unknown'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table mapping SAP material type + procurement type combinations to Mirion business classifications. One row per unique combination. Cross-joined from distinct MTART values in MARA and distinct BESKZ values in MARC. Excludes inactive types ZONT, ZHER, HERS. Source: median_hub_captured.sap.'

# COMMAND ----------

# DBTITLE 1,mirion_customer_numbers
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.mirion_customer_numbers (
# MAGIC
# MAGIC     mirion_customer_numbers_id  BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each Mirion customer account',
# MAGIC
# MAGIC     customer_number             STRING  COMMENT 'SAP customer account number (KNA1.KUNNR). Leading zeros stripped for numeric accounts',
# MAGIC     customer_name               STRING  COMMENT 'Customer name as stored in SAP (KNA1.NAME1)',
# MAGIC     city                        STRING  COMMENT 'City from the customer master address (KNA1.ORT01)',
# MAGIC     short_name                  STRING  COMMENT 'Mirion standardized site label derived from a hardcoded KUNNR mapping (e.g. MIRION (MERIDEN)). TBD if KUNNR not in the mapping'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of Mirion-affiliated customer accounts in SAP. Filtered to customers whose name contains Mirion, Canberra, Sun Nuclear, or Capintec; excludes deleted, blocked, BLK3, and French entity accounts. short_name provides a standardized site label for reporting. Source: median_hub_captured.sap (KNA1).'

# COMMAND ----------

# DBTITLE 1,mirion_vendor_numbers
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.mirion_vendor_numbers (
# MAGIC
# MAGIC     mirion_vendor_numbers_id    BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each Mirion vendor account',
# MAGIC
# MAGIC     vendor_number               STRING  COMMENT 'SAP vendor account number (LFA1.LIFNR). Leading zeros stripped for numeric accounts; alphanumeric accounts (e.g. V4020, R103998) kept as-is',
# MAGIC     vendor_name                 STRING  COMMENT 'Vendor name as stored in SAP (LFA1.NAME1)',
# MAGIC     country                     STRING  COMMENT 'Country key from the vendor master (LFA1.LAND1)',
# MAGIC     short_name                  STRING  COMMENT 'Mirion standardized site label derived from a hardcoded LIFNR mapping (e.g. MIRION (MERIDEN)). TBD if LIFNR not in the mapping'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of Mirion-affiliated vendor accounts in SAP. Filtered to vendors whose name contains Mirion, Canberra, or Capintec. short_name provides a standardized site label for intercompany reporting. Source: median_hub_captured.sap (LFA1).'

# COMMAND ----------

# DBTITLE 1,movement_type
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.movement_type (
# MAGIC
# MAGIC     movement_type_id        BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each movement type record',
# MAGIC
# MAGIC     movement_type           STRING  COMMENT 'SAP movement type code (T156.BWART), e.g. 101 = GR from PO, 261 = GI to production order',
# MAGIC     sap_description         STRING  COMMENT 'SAP standard description for the movement type in English (T156T.BTEXT)',
# MAGIC     movement_type_text      STRING  COMMENT 'Mirion plain-language description of the movement type. NULL for movement types not active in NA Tech',
# MAGIC     breakdown               STRING  COMMENT 'High-level operational category: Goods Receipt | Goods Issue | Transfer | Scrap | Adjustment | Unknown'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP movement types active in NA Tech. Covers goods receipts, goods issues, transfers, scrap, and inventory adjustments. movement_type_text is a Mirion-maintained plain-language description; rows without one are excluded. Source: median_hub_captured.sap (T156, T156T).'

# COMMAND ----------

# DBTITLE 1,payment_terms
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.payment_terms (
# MAGIC
# MAGIC     payment_terms_id    BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each payment terms record',
# MAGIC
# MAGIC     payment_terms       STRING  COMMENT 'SAP payment terms key (T052.ZTERM), e.g. N030 = net 30 days, Z001 = immediate payment',
# MAGIC     description         STRING  COMMENT 'English description of the payment terms (T052U.TEXT1)',
# MAGIC     baseline_date       STRING  COMMENT 'Baseline date indicator for due date calculation (T052.ZTAG1)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP payment terms keys and their English descriptions. Used to decode ZTERM on purchase orders, vendor master, and accounts payable documents. Source: median_hub_captured.sap (T052, T052U).'

# COMMAND ----------

# DBTITLE 1,purchasing_groups
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.purchasing_groups (
# MAGIC
# MAGIC     purchasing_groups_id    BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each purchasing group record',
# MAGIC
# MAGIC     purchasing_group        STRING  COMMENT 'SAP purchasing group code (T024.EKGRP), e.g. M01, P02. Used to identify the buyer or buyer group responsible for a purchase order',
# MAGIC     buyer_name              STRING  COMMENT 'Name of the buyer or purchasing group (T024.EKNAM)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP purchasing groups and their buyer names. Used to decode EKGRP on purchase orders and purchase requisitions. Source: median_hub_captured.sap (T024).'

# COMMAND ----------

# DBTITLE 1,so_document_type
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.so_document_type (
# MAGIC
# MAGIC     so_document_type_id BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each sales document type record',
# MAGIC
# MAGIC     sales_doc_type      STRING  COMMENT 'SAP sales document type code (TVAK.AUART), e.g. ZOR = standard order, ZRE = return',
# MAGIC     description         STRING  COMMENT 'English description of the sales document type (TVAKT.BEZEI)',
# MAGIC     order_type          STRING  COMMENT 'Mirion business classification: Standard | Inquiry | Consignment | Customer Loan | Quotation | Return | Warranty/Repair | Unused'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP sales document types with Mirion business classifications. Used to decode AUART on sales orders and filter by order category. Source: median_hub_captured.sap (TVAK, TVAKT).'

# COMMAND ----------

# DBTITLE 1,so_item_category
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.so_item_category (
# MAGIC
# MAGIC     so_item_category_id BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each item category record',
# MAGIC
# MAGIC     item_category       STRING  COMMENT 'SAP sales document item category code (TVAPT.PSTYV), e.g. ZTAN = standard item, ZSER = service item',
# MAGIC     description         STRING  COMMENT 'English description of the item category (TVAPT.VTEXT)',
# MAGIC     billing_type        STRING  COMMENT 'Mirion billing classification: Standard | Billing Plan | Milestone | TBD'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP sales order item categories with Mirion billing type classifications. Used to decode PSTYV on sales order line items and filter by billing behavior. Source: median_hub_captured.sap (TVAPT).'

# COMMAND ----------

# DBTITLE 1,so_reject_codes
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.so_reject_codes (
# MAGIC
# MAGIC     so_reject_codes_id  BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each rejection code record',
# MAGIC
# MAGIC     rejection_code      STRING  COMMENT 'SAP sales order rejection reason code (TVAGT.ABGRU), e.g. 08 = customer cancel, Z5 = closed',
# MAGIC     description         STRING  COMMENT 'English description of the rejection reason (TVAGT.BEZEI)',
# MAGIC     status              STRING  COMMENT 'Mirion classification: Canceled | Closed | none'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP sales order rejection reason codes with Mirion status classifications. Used to decode ABGRU on sales order line items and identify canceled or closed lines. Source: median_hub_captured.sap (TVAGT).'

# COMMAND ----------

# DBTITLE 1,storage_locations
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.storage_locations (
# MAGIC
# MAGIC     storage_location_id BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each storage location record',
# MAGIC
# MAGIC     plant               STRING  COMMENT 'Plant code (T001L.WERKS), e.g. 4002 = Concord, 4019 = Oak Ridge, 4020 = Meriden, 4021 = Olen, 4022 = Oxfordshire',
# MAGIC     storage_location    STRING  COMMENT 'Storage location code within the plant (T001L.LGORT). Leading zeros removed for numeric codes',
# MAGIC     description         STRING  COMMENT 'Description of the storage location (T001L.LGOBE)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of storage locations per NA Tech plant. Used to decode LGORT on inventory and goods movement records. Filtered to plants 4002, 4019, 4020, 4021, 4022. Source: median_hub_captured.sap (T001L).'

# COMMAND ----------

# DBTITLE 1,delivery_blocks
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.delivery_blocks (
# MAGIC
# MAGIC     delivery_block_id   BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each delivery block record',
# MAGIC
# MAGIC     delivery_block      STRING  COMMENT 'SAP delivery block code (TVLST.LIFSP). Set on VBAK to prevent shipment processing for the order',
# MAGIC     description         STRING  COMMENT 'English description of the delivery block (TVLST.VTEXT)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP delivery block codes with English descriptions. Used to decode LIFSP on sales order headers (VBAK). Source: median_hub_captured.sap (TVLST).'

# COMMAND ----------

# DBTITLE 1,billing_blocks
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.billing_blocks (
# MAGIC
# MAGIC     billing_block_id    BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each billing block record',
# MAGIC
# MAGIC     billing_block       STRING  COMMENT 'SAP billing block code (TVFST.FAKSP). Set on VBAK or VBAP to prevent invoice creation',
# MAGIC     description         STRING  COMMENT 'English description of the billing block (TVFST.VTEXT)'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP billing block codes with English descriptions. Used to decode FAKSP on sales order headers (VBAK) and line items (VBAP). Source: median_hub_captured.sap (TVFST).'

# COMMAND ----------

# DBTITLE 1,profit_center
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.profit_center (
# MAGIC
# MAGIC     profit_center_id        BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each profit center record',
# MAGIC
# MAGIC     profit_center           STRING  COMMENT 'SAP profit center code (CEPC.PRCTR), e.g. P100520, P103620. Used on MARC, inventory, and cost object assignments',
# MAGIC     description             STRING  COMMENT 'English short text from profit center master (CEPCT.KTEXT)',
# MAGIC     lock_indicator          STRING  COMMENT 'Blank = active; X = locked or inactive (CEPC.LOCK_IND)',
# MAGIC     level_3_dept            STRING  COMMENT 'Department-level hierarchy node (level 3), e.g. INSTRUMENT.CY24, DETECTORS.CY24. Null for RMS/SIS/Corporate profit centers and those absent from the hierarchy',
# MAGIC     level_3_dept_desc       STRING  COMMENT 'Description of the level 3 department node',
# MAGIC     level_2_site            STRING  COMMENT 'Site-level hierarchy node (level 2), e.g. MERIDEN.CY24, OAKRIDGE.CY24, RMS.CY24, SIS.CY24',
# MAGIC     level_2_site_desc       STRING  COMMENT 'Description of the level 2 site node, e.g. Meriden, Oak Ridge, RMS',
# MAGIC     level_1_top             STRING  COMMENT 'Top-level hierarchy node (level 1), e.g. CNBI_CY24',
# MAGIC     level_1_top_desc        STRING  COMMENT 'Description of the top-level hierarchy node'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of SAP profit centers for the BUMN controlling area (NA Tech). Includes hierarchy context (site, department, top rollup) from the profit_center_hierarchy table where available; profit centers absent from the hierarchy have null level columns. Used to decode PRCTR on inventory, material master, and cost object records. Source: median_hub_captured.sap (CEPC, CEPCT, profit_center_hierarchy).'

# COMMAND ----------

# DBTITLE 1,cost_center
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hub_live_transformed.sap.cost_center (
# MAGIC
# MAGIC     cost_center_id          BIGINT  GENERATED ALWAYS AS IDENTITY   COMMENT 'Surrogate key — system-generated unique identifier for each cost center record',
# MAGIC
# MAGIC     cost_center             STRING      COMMENT 'SAP cost center code (CSKS.KOSTL). Leading zeros removed for numeric codes. Alphanumeric codes (e.g. C02010) kept as-is',
# MAGIC     description             STRING      COMMENT 'English description of the cost center (CSKT.KTEXT)',
# MAGIC     controlling_area        STRING      COMMENT 'Controlling area the cost center belongs to (CSKS.KOKRS), e.g. BUMN for NA Tech',
# MAGIC     responsible_person      STRING      COMMENT 'Name of the person responsible for the cost center (CSKS.VERAK)',
# MAGIC     department              STRING      COMMENT 'Department code and label assigned to the cost center (CSKS.ABTEI), e.g. 07 - FAB, 30 - Direct'
# MAGIC
# MAGIC )
# MAGIC COMMENT 'Dimension table of active SAP cost centers across all controlling areas for client 400. Filtered to DATBI = 9999-12-31 (currently active records only). Used to decode KOSTL on goods movements, purchase orders, and cost object assignments. Source: median_hub_captured.sap (CSKS, CSKT).'
