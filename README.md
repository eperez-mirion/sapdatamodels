# SAP Data Models — NA Tech

**Target catalog/schema:** `hub_live_transformed.sap`

**Source system:** `median_hub_captured.sap`

**Constants applied to all queries:**
- `MANDT = '400'` — Client filter
- `SPRAS = 'E'` — English language (on language-dependent tables)

---

## Notebooks

| Notebook | Table | Status |
|----------|-------|--------|
| [ddl.py](ddl.py) | Schema + all tables | Run first to create schema and table definitions |
| [inventory.py](inventory.py) | `inventory` | Tested — ~99.9% match with OR data |
| [purchase_document.py](purchase_document.py) | `purchase_order` | Tested — matches OR open PO data with expected filters |
| [material_reservation.py](material_reservation.py) | `material_reservation` | Needs further testing — individual lines match, row count off |
| [purchase_requisition.py](purchase_requisition.py) | `purchase_requisition` | Needs testing |
| [approved_mfg_part_list.py](approved_mfg_part_list.py) | `sap_mpn` | Stable |
| [bill_of_materials.py](bill_of_materials.py) | `sap_bom` | Stable |

---

## Table Reference

### inventory
**Mirrors:** SAP MB52
**Granularity:** One row per Material + Plant + Storage Location + Special Stock indicator

Inventory stock balances across all active stock types. Covers standard plant stock (MARD) and special stock: sales order (MSKA, indicator = E), project/WBS (MSPR, indicator = Q), and vendor subcontracting/returnable packaging (MSSL, indicator = O/V). Only records with at least one non-zero quantity are included.

> **Known gap:** Special stock W (customer consignment, MSKU) excluded — source table not accessible.
> **Open item:** Division field needs a decoder table.

| Column | Type | Description |
|--------|------|-------------|
| material_number | STRING | SAP material number (leading zeros stripped) |
| material_description | STRING | Material short text in English |
| plant | STRING | Plant code |
| profit_center | STRING | Profit center at plant level |
| division | STRING | Division (MARA.SPART) |
| storage_location | STRING | Storage location; NULL for vendor stock |
| special_stock_indicator | STRING | Blank / E / Q / O / V |
| material_type | STRING | Material type code (ROH, HALB, FERT, etc.) |
| abc_indicator | STRING | A / B / C classification |
| base_unit_of_measure | STRING | Base UOM |
| material_group | STRING | Material group / commodity code |
| valuation_class | STRING | Valuation class (BKLAS) |
| unit_price | DECIMAL(18,4) | Standard price per base unit |
| unrestricted_qty | DECIMAL(18,3) | Unrestricted-use stock |
| quality_inspection_qty | DECIMAL(18,3) | Stock in QI |
| blocked_qty | DECIMAL(18,3) | Blocked stock |
| transfer_qty | DECIMAL(18,3) | Stock in transit (standard only) |
| restricted_use_qty | DECIMAL(18,3) | Restricted-use stock (standard only) |
| returns_qty | DECIMAL(18,3) | Returns stock (standard only) |
| total_qty | DECIMAL(18,3) | Sum of all stock categories |
| total_value | DECIMAL(18,2) | total_qty × unit_price |

**SAP source tables:** MARD, MSKA, MSPR, MSSL, MARA, MARC, MAKT, MBEW

---

### purchase_order
**Mirrors:** SAP ME2M (with additions)
**Granularity:** One row per schedule line (EKKO + EKPO + EKET)

All purchase orders and stock transfer orders (STO). Expands each PO item to its delivery schedule lines. Includes a derived `po_status` field.

> **Known gap:** PO text block excluded — requires STXH/STXL aggregation not yet available.
> **Open item:** `line_creation_date` — EKPO.CREATIONDATE unpopulated in current extraction.

| Column | Type | Description |
|--------|------|-------------|
| purchasing_document_number | STRING | PO number (leading zeros stripped) |
| purchasing_document_item | STRING | PO item number |
| schedule_line_counter | STRING | Schedule line counter within item |
| vendor_account_number | STRING | Vendor number (leading zeros stripped) |
| vendor_name | STRING | Vendor name |
| material_number | STRING | Material number (leading zeros stripped) |
| material_description | STRING | Material description; falls back to free-text if no MATNR |
| po_unit_of_measure | STRING | Order unit of measure |
| po_currency | STRING | PO currency |
| plant | STRING | Receiving plant |
| material_group | STRING | Material group |
| purchasing_document_date | STRING | PO creation date (YYYYMMDD) |
| item_delivery_date | STRING | Schedule line delivery date (YYYYMMDD) |
| statistics_delivery_date | STRING | Statistical delivery date (YYYYMMDD) |
| local_currency | STRING | Company code local currency |
| po_quantity | DECIMAL(18,3) | Total ordered quantity at item level |
| scheduled_quantity | DECIMAL(18,3) | Schedule line quantity |
| gr_quantity | DECIMAL(18,3) | Goods receipt quantity |
| quantity_to_be_delivered | DECIMAL(18,3) | scheduled_quantity − gr_quantity |
| purchase_requisition_number | STRING | Originating PR number |
| price_unit | DECIMAL(18,3) | Price basis quantity |
| net_price | DECIMAL(18,4) | Net price per price unit |
| total_value | DECIMAL(18,2) | scheduled_quantity × net_price / price_unit |
| gr_indicator | STRING | GR required: X = yes |
| invoice_receipt_indicator | STRING | IR required: X = yes |
| item_category | STRING | Item category (blank / D / K) |
| account_assignment_category | STRING | Cost object type (blank / K / F / P) |
| deletion_indicator | STRING | X = deleted |
| delivery_completed_indicator | STRING | X = final delivery |
| po_status | STRING | Deleted / Delivery Complete / Fully Received / Partially Received / Open |
| purchasing_group | STRING | Buyer code |
| storage_location | STRING | Destination storage location |
| purchasing_organization | STRING | Purchasing organization |

**SAP source tables:** EKKO, EKPO, EKET, LFA1, T001, MAKT

---

### material_reservation
**Mirrors:** SAP MB25
**Granularity:** One row per reservation item (RSNUM + RSPOS)

Material reservations and dependent requirements. Deleted reservation lines (XLOEK = X) are excluded.

> **Needs further testing:** Individual lines match OR data; overall count differs.

| Column | Type | Description |
|--------|------|-------------|
| reservation_number | STRING | Reservation number |
| reservation_item | STRING | Item within reservation |
| order_number | STRING | Associated production/maintenance order (leading zeros stripped) |
| material_number | STRING | Material number (leading zeros stripped) |
| material_description | STRING | Material short text |
| plant | STRING | Plant |
| storage_location | STRING | Issue storage location |
| special_stock_indicator | STRING | Special stock type (E / Q / O) |
| reservation_type | STRING | Record type |
| account_assignment_category | STRING | Cost object type (K / F / P / A) |
| movement_type | STRING | Goods movement type for issue |
| requirement_date | STRING | Required by date (YYYYMMDD) |
| base_unit_of_measure | STRING | Base UOM |
| requirement_qty | DECIMAL(13,3) | Total required quantity |
| withdrawn_qty | DECIMAL(13,3) | Already issued quantity |
| difference_qty | DECIMAL(13,3) | Open quantity (requirement − withdrawn) |
| final_issue_indicator | STRING | X = final issue posted |

**SAP source tables:** RESB, MAKT

---

### purchase_requisition
**Mirrors:** SAP ME5A
**Granularity:** One row per PR item (EBAN)

Purchase requisition items with linked PO number, net price, and total GR quantity.

> **Needs testing.**

| Column | Type | Description |
|--------|------|-------------|
| purchase_requisition_number | STRING | PR number (leading zeros stripped) |
| purchase_requisition_item | STRING | Item within PR |
| material_number | STRING | Material number (leading zeros stripped) |
| material_description | STRING | Material description; falls back to free-text if no MATNR |
| purchasing_group | STRING | Buyer code |
| material_group | STRING | Material group |
| processing_status | STRING | Processing status (N / B / etc.) |
| release_status | STRING | Approval status |
| release_indicator | STRING | Approval levels granted |
| purchasing_document_number | STRING | Linked PO number |
| vendor_account_number | STRING | Preferred/assigned vendor |
| pr_closed | STRING | X = closed |
| plant | STRING | Requiring plant |
| requisition_date | STRING | PR creation date (YYYYMMDD) |
| release_date | STRING | Approval date (YYYYMMDD) |
| item_delivery_date | STRING | Requested delivery date (YYYYMMDD) |
| unit_of_measure | STRING | Base UOM |
| currency | STRING | Price currency |
| planned_delivery_time_days | DECIMAL(5,0) | Lead time in days |
| pr_quantity | DECIMAL(18,3) | Requested quantity |
| pr_price | DECIMAL(18,4) | Estimated price per price unit |
| price_unit | DECIMAL(18,3) | Price basis quantity |
| committed_quantity | DECIMAL(18,3) | Committed in planning |
| ordered_quantity | DECIMAL(18,3) | Converted to PO |
| net_price_po | DECIMAL(18,4) | Net price from linked PO |
| gr_quantity | DECIMAL(18,3) | Total GR quantity on linked PO item |

**SAP source tables:** EBAN, MAKT, EKPO, EKET

---

### sap_mpn
**Granularity:** One row per approved manufacturer per internal material per plant

Approved Manufacturer Parts List — maps internal Mirion materials to their approved external manufacturer part numbers. Rows with NULL plant are excluded.

| Column | Type | Description |
|--------|------|-------------|
| MPN_Number | STRING | Manufacturer part number in SAP (leading zeros stripped) |
| Material_Number | STRING | Internal Mirion material number |
| Material_Description | STRING | Internal material description |
| Manufacturer_Name | STRING | Manufacturer name |
| Manufacturer_Number | STRING | SAP vendor number for the manufacturer |
| Manufacturer_Material_Number | STRING | Manufacturer's own part number |
| Plant | STRING | Plant where approval applies |
| Valid_From | STRING | Effective date (YYYYMMDD) |
| Valid_To | STRING | Expiry date (YYYYMMDD) |

**SAP source tables:** AMPL, MARA, MAKT, LFA1

---

### sap_bom
**Granularity:** One row per component at each level of the BOM hierarchy

Multi-level BOM explosion up to 10 levels deep. Four deduplication layers are applied: alternative BOM selection (lowest alt number), revision-letter dedup (highest letter per base number), same-qty position dedup (latest valid_from), and item-number dedup (highest item node / STLKN).

BOM usage type is propagated through explosion — a production BOM (usage = 1) only expands sub-assemblies that also have a production BOM, preventing cross-usage contamination.

| Column | Type | Description |
|--------|------|-------------|
| top_material | STRING | Top-level assembly at the root of the explosion |
| top_plant | STRING | Plant of the top-level assembly |
| bom_level | INT | Depth in hierarchy (1 = direct child) |
| parent_material | STRING | Immediate parent assembly material number |
| parent_description | STRING | Parent material description |
| component_material | STRING | Component material number (leading zeros stripped) |
| component_description | STRING | Component material description |
| plant | STRING | Plant for this BOM |
| bom_usage | STRING | Usage type (1 = Production, 2 = Engineering, 5 = Sales) |
| bom_number | STRING | BOM document number |
| alt_bom | STRING | Alternative BOM number (lowest retained) |
| bom_category | STRING | BOM category (M = material BOM) |
| item_number | STRING | Item position within the BOM |
| qty | DECIMAL(18,3) | Required quantity per parent |
| uom | STRING | Unit of measure |
| item_category | STRING | Item category (L = stock, N = non-stock, D = document) |
| valid_from | STRING | Effective date for this revision (YYYYMMDD) |

**SAP source tables:** MAST, STKO, STAS, STPO, MAKT

---

## Open Items

| Item | Affects | Notes |
|------|---------|-------|
| MSKU table access | inventory | Special stock W (customer consignment) excluded |
| STXH / STXL access | purchase_order | Required for PO text block |
| Division decoder | inventory | MARA.SPART needs mapping table |
| line_creation_date | purchase_order | EKPO.CREATIONDATE unpopulated in extraction |
| material_reservation count | material_reservation | Row count does not match OR; needs investigation |
