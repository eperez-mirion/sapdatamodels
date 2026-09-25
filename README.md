# SAP Data Models — NA Tech

**Target catalog/schema:** `hub_live_transformed.sap`

**Source system:** `median_hub_captured.sap`

**Constants applied to all queries:**
- `MANDT = '400'` — Client filter
- `SPRAS = 'E'` — English language (on language-dependent tables)

---

## Notebooks

| Notebook | Table | Description | Status |
|----------|-------|-------------|--------|
| [ddl.py](ddl.py) | Schema + all tables | Creates the hub_live_transformed.sap schema and all table definitions | Run first |
| [inventory.py](inventory.py) | `inventory` | Stock balances by material, plant, storage location, and special stock type | Tested — ~99.9% match with OR data |
| [purchase_order.py](purchase_order.py) | `purchase_order` | Purchase order schedule lines by vendor, material, and schedule line | Tested — matches OR open PO data |
| [stock_transfer.py](stock_transfer.py) | `stock_transfer` | Internal stock transfer order lines by material and plant | Tested (DS) |
| [material_reservation.py](material_reservation.py) | `material_reservation` | Material reservations and dependent requirements by reservation line | Needs further testing — row count off |
| [purchase_requisition.py](purchase_requisition.py) | `purchase_requisition` | Purchase requisition items with linked PO and GR quantities | Needs testing |
| [approved_mfg_part_list.py](approved_mfg_part_list.py) | `approved_mfg_part_list` | Approved manufacturer parts by material and plant | Stable |
| [bill_of_materials.py](bill_of_materials.py) | `bill_of_materials` | Multi-level BOM explosion by material assembly hierarchy | Stable |
| [material_details.py](material_details.py) | `material_details` | Material master and plant planning data by material and plant | New — needs testing |
| [material_usage.py](material_usage.py) | `material_usage` | Full goods movement history by material document line item | New — needs testing |
| [vendor_master.py](vendor_master.py) | `vendor_master` | Vendor address, contact, and purchasing org data by vendor | New — needs testing |
| [material_last_movement.py](material_last_movement.py) | `material_last_movement` | Most recent goods movement date by material and plant | New — needs testing |
| [vendor_otd.py](vendor_otd.py) | `vendor_otd` | Vendor on-time delivery scored by PO schedule line | New — needs testing |
| **— Dimension Tables —** | | | |
| [material_class.py](material_class.py) | `material_class` | Dimension table: material class by material type and procurement type | New — needs testing |
| [mirion_customer_numbers.py](mirion_customer_numbers.py) | `mirion_customer_numbers` | Dimension table: Mirion internal customer accounts with standardized site labels | New — needs testing |
| [mirion_vendor_numbers.py](mirion_vendor_numbers.py) | `mirion_vendor_numbers` | Dimension table: Mirion internal vendor accounts with standardized site labels | New — needs testing |
| [movement_type.py](movement_type.py) | `movement_type` | Dimension table: movement type codes with plain-language descriptions and breakdown categories | New — needs testing |
| [payment_terms.py](payment_terms.py) | `payment_terms` | Dimension table: payment terms keys with English descriptions | New — needs testing |
| [purchasing_groups.py](purchasing_groups.py) | `purchasing_groups` | Dimension table: purchasing group codes and buyer names | New — needs testing |
| [so_document_type.py](so_document_type.py) | `so_document_type` | Dimension table: sales order document type codes with Mirion order type classification | New — needs testing |
| [so_item_category.py](so_item_category.py) | `so_item_category` | Dimension table: sales order item category codes with Mirion billing type classification | New — needs testing |
| [so_reject_codes.py](so_reject_codes.py) | `so_reject_codes` | Dimension table: sales order rejection reason codes with Mirion status classification | New — needs testing |
| [storage_locations.py](storage_locations.py) | `storage_locations` | Dimension table: storage location codes and descriptions per NA Tech plant | New — needs testing |
| [delivery_blocks.py](delivery_blocks.py) | `delivery_blocks` | Dimension table: sales order delivery block codes with descriptions | New — needs testing |
| [billing_blocks.py](billing_blocks.py) | `billing_blocks` | Dimension table: sales order billing block codes with descriptions | New — needs testing |
| [profit_center.py](profit_center.py) | `profit_center` | Dimension table: profit centers with site, department, and top-level hierarchy rollup | New — needs testing |
| [cost_center.py](cost_center.py) | `cost_center` | Dimension table: cost centers with controlling area, validity periods, responsible person, and department | New — needs testing |

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
**Mirrors:** SAP ME2M
**Granularity:** One row per schedule line (EKKO + EKPO + EKET)
**Document range:** 0004000000–0004999999 (standard POs only)

Standard purchase orders expanded to delivery schedule lines. Includes a derived `po_status` field and `purchasing_document_type` (BSART) for future merging with `stock_transfer`.

> **Known gap:** PO text block excluded — requires STXH/STXL aggregation not yet available.
> **Open item:** `line_creation_date` — EKPO.CREATIONDATE unpopulated in current extraction.

| Column | Type | Description |
|--------|------|-------------|
| purchasing_document_number | STRING | PO number (leading zeros stripped) |
| purchasing_document_item | STRING | PO item number |
| schedule_line_counter | STRING | Schedule line counter within item |
| purchasing_document_type | STRING | SAP document type (e.g. NB = standard PO) |
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

### stock_transfer
**Mirrors:** SAP ME2M (filtered to STO range)
**Granularity:** One row per schedule line (EKKO + EKPO + EKET)
**Document range:** 0003000000–0003999999 (stock transfers only)

Internal plant-to-plant stock transfer orders. No external vendor — LFA1 not joined. Includes `receiving_plant` (EKPO.WERKS), `supplying_plant` (EKPO.RESWK), and `purchasing_document_type` (BSART) for future merging with `purchase_order`.

| Column | Type | Description |
|--------|------|-------------|
| purchasing_document_number | STRING | STO document number (leading zeros stripped) |
| purchasing_document_item | STRING | STO item number |
| schedule_line_counter | STRING | Schedule line counter |
| purchasing_document_type | STRING | SAP document type (e.g. UB = stock transfer) |
| material_number | STRING | Material number (leading zeros stripped) |
| material_description | STRING | Material short text |
| po_unit_of_measure | STRING | Unit of measure |
| receiving_plant | STRING | Plant receiving the stock (EKPO.WERKS) |
| supplying_plant | STRING | Plant issuing the stock (EKPO.RESWK) |
| material_group | STRING | Material group |
| purchasing_document_date | STRING | STO creation date (YYYYMMDD) |
| item_delivery_date | STRING | Schedule line delivery date (YYYYMMDD) |
| statistics_delivery_date | STRING | Statistical delivery date (YYYYMMDD) |
| local_currency | STRING | Company code local currency |
| po_quantity | DECIMAL(18,3) | Total ordered quantity at item level |
| scheduled_quantity | DECIMAL(18,3) | Schedule line quantity |
| gr_quantity | DECIMAL(18,3) | Goods receipt quantity |
| quantity_to_be_delivered | DECIMAL(18,3) | scheduled_quantity − gr_quantity |
| price_unit | DECIMAL(18,3) | Price basis quantity |
| net_price | DECIMAL(18,4) | Net price per price unit |
| total_value | DECIMAL(18,2) | scheduled_quantity × net_price / price_unit |
| gr_indicator | STRING | GR required: X = yes |
| item_category | STRING | Item category |
| deletion_indicator | STRING | X = deleted |
| delivery_completed_indicator | STRING | X = final delivery |
| sto_status | STRING | Deleted / Delivery Complete / Fully Received / Partially Received / Open |
| purchasing_group | STRING | Buyer code |
| storage_location | STRING | Destination storage location |
| purchasing_organization | STRING | Purchasing organization |

**SAP source tables:** EKKO, EKPO, EKET, T001, MAKT

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

### approved_mfg_part_list
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

### bill_of_materials
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

### material_details
**Granularity:** One row per material + plant (MARC)

Material master combined with plant planning parameters and valuation data. Covers general material attributes (type, group, weight, dimensions) alongside plant-level MRP settings (type, controller, lot sizing, procurement type) and standard/moving-average pricing.

| Column | Type | Description |
|--------|------|-------------|
| material_number | STRING | SAP material number (leading zeros stripped) |
| material_description | STRING | Material short text in English |
| plant | STRING | Plant |
| material_type | STRING | Material type code (ROH, HALB, FERT, HAWA, etc.) |
| material_group | STRING | Material group / commodity code |
| base_unit_of_measure | STRING | Base unit of measure |
| division | STRING | Division (MARA.SPART) |
| old_material_number | STRING | Legacy / predecessor material number |
| creation_date | STRING | Material master creation date (YYYYMMDD) |
| net_weight | DECIMAL(18,3) | Net weight per base unit |
| gross_weight | DECIMAL(18,3) | Gross weight per base unit |
| weight_unit | STRING | Unit of weight (KG, G, LB, etc.) |
| volume | DECIMAL(18,3) | Volume per base unit |
| volume_unit | STRING | Unit of volume (L, ML, CM3, etc.) |
| manufacturer_part_number | STRING | Manufacturer own part number (MARA.MFRPN) |
| profit_center | STRING | Profit center at plant level |
| mrp_type | STRING | MRP planning type (PD, VB, ND, MO, etc.) |
| mrp_controller | STRING | MRP controller / planner code |
| lot_sizing_procedure | STRING | Lot sizing procedure (EX, FX, HB, etc.) |
| procurement_type | STRING | E = in-house, F = external, X = both |
| special_procurement_type | STRING | Special procurement key (subcontracting, phantom, etc.) |
| plant_material_status | STRING | Plant-level material status; restricts transactions when set |
| abc_indicator | STRING | ABC classification at plant level |
| purchasing_group | STRING | Purchasing group at plant level |
| planned_delivery_time_days | DECIMAL(5,0) | Planned delivery time in calendar days |
| gr_processing_time_days | DECIMAL(5,0) | GR processing time in workdays |
| safety_stock_qty | DECIMAL(18,3) | Safety stock level |
| reorder_point | DECIMAL(18,3) | Reorder point quantity |
| maximum_stock_level | DECIMAL(18,3) | Maximum stock level |
| valuation_class | STRING | Valuation class (MBEW.BKLAS) |
| price_control | STRING | S = standard price, V = moving average |
| standard_price | DECIMAL(18,4) | Standard price per price unit |
| moving_average_price | DECIMAL(18,4) | Moving average price per price unit |
| price_unit | DECIMAL(18,3) | Price unit for valuation prices |

**SAP source tables:** MARC, MARA, MAKT, MBEW

---

### material_usage
**Mirrors:** SAP MB51
**Granularity:** One row per material document item (MSEG)

Full goods movement history across all movement types and all dates. No filters are applied beyond client — use `movement_type` and `posting_date` to slice in downstream queries or reports.

Common movement types: 101 = GR for PO, 261 = GI for production order, 201 = GI for cost center, 311/312 = plant-to-plant transfer, 501 = receipt without reference.

| Column | Type | Description |
|--------|------|-------------|
| posting_date | STRING | Date posted to inventory and accounting (YYYYMMDD) |
| document_date | STRING | Date on the source document (YYYYMMDD) |
| material_document_number | STRING | Material document number |
| material_document_year | STRING | Fiscal year of the document |
| material_document_item | STRING | Line item within the document |
| movement_type | STRING | SAP movement type code |
| debit_credit_indicator | STRING | S = stock increase, H = stock decrease |
| material_number | STRING | SAP material number (leading zeros stripped) |
| material_description | STRING | Material short text in English |
| material_type | STRING | Material type code (ROH, HALB, FERT, etc.) |
| material_group | STRING | Material group / commodity code |
| plant | STRING | Plant where the movement occurred |
| storage_location | STRING | Storage location |
| special_stock_indicator | STRING | Blank = standard, E = sales order, Q = project |
| quantity | DECIMAL(18,3) | Quantity moved |
| unit_of_measure | STRING | Unit of measure for the quantity |
| amount_local_currency | DECIMAL(18,2) | Value in company code local currency |
| order_number | STRING | Production/maintenance/internal order (leading zeros stripped) |
| purchase_order_number | STRING | Associated PO number (leading zeros stripped) |
| purchase_order_item | STRING | PO item number (leading zeros stripped) |
| cost_center | STRING | Cost center charged on goods issue |
| reservation_number | STRING | Reservation fulfilled by this movement (leading zeros stripped) |
| created_by | STRING | SAP username who posted the document |

**SAP source tables:** MSEG, MKPF, MAKT, MARA

---

### vendor_master
**Granularity:** One row per vendor + purchasing organization (LFA1 × LFM1)

Vendor address, contact info, email, and purchasing org data in a single flat table. Replaces both the standalone vendor master and vendor email DS reports. Each site maintains its own vendor records in LFM1, so a vendor set up across multiple purchasing orgs appears once per org. Vendors not set up in any purchasing org appear once with NULL org fields. Email is sourced from SAP address management (ADR6) via the address key on LFA1, deduplicated to the default address.

| Column | Type | Description |
|--------|------|-------------|
| vendor_account_number | STRING | SAP vendor account number (leading zeros stripped) |
| vendor_name | STRING | Primary vendor name |
| vendor_name_2 | STRING | Secondary name line |
| account_group | STRING | Vendor account group (KTOKK) |
| industry | STRING | Industry sector key |
| country | STRING | Country key |
| region | STRING | Region / state |
| city | STRING | City |
| postal_code | STRING | Postal / ZIP code |
| street_address | STRING | Street and house number |
| telephone | STRING | Primary telephone |
| fax | STRING | Fax number |
| tax_number_1 | STRING | Tax number 1 (e.g. EIN in US) |
| tax_number_2 | STRING | Tax number 2 (e.g. VAT number) |
| posting_block | STRING | X = all postings blocked |
| central_deletion_flag | STRING | X = vendor marked for deletion |
| email | STRING | Primary email from SAP address management; NULL if none on file |
| purchasing_organization | STRING | Purchasing org this row applies to — one row per org the vendor is set up in |
| payment_terms | STRING | Payment terms key (e.g. N030 = net 30) |
| order_currency | STRING | Default PO currency |
| incoterms | STRING | Incoterms code (EXW, FOB, CIF, etc.) |
| incoterms_description | STRING | Incoterms location or description |
| minimum_order_value | DECIMAL(18,2) | Minimum order value in order currency |
| gr_based_iv_indicator | STRING | X = invoice requires GR before posting |
| purchasing_block | STRING | Purchasing-org-level block flag |

**SAP source tables:** LFA1, ADR6, LFM1

---

### material_last_movement
**Granularity:** One row per material + plant (most recent movement only)

Most recent goods movement per material per plant. Useful for slow-moving inventory analysis — join to `inventory` on material_number + plant to flag stock with no recent activity. `days_since_last_movement` is calculated at notebook run time so it stays current on each refresh.

> **Note:** `days_since_last_movement` reflects the age as of the last notebook run, not the current date. Schedule regular refreshes to keep it accurate.

| Column | Type | Description |
|--------|------|-------------|
| material_number | STRING | SAP material number (leading zeros stripped) |
| material_description | STRING | Material short text in English |
| material_type | STRING | Material type code (ROH, HALB, FERT, etc.) |
| material_group | STRING | Material group / commodity code |
| plant | STRING | Plant |
| last_movement_date | STRING | Posting date of the most recent goods movement (YYYYMMDD) |
| last_movement_type | STRING | SAP movement type of the last movement |
| last_document_number | STRING | Material document number of the last movement |
| days_since_last_movement | INT | Days since last_movement_date as of the last notebook run |

**SAP source tables:** MSEG, MKPF, MAKT, MARA

---

### vendor_otd
**Mirrors:** SAP ME2M + GR history
**Granularity:** One row per PO delivery schedule line (EKET)
**Document range:** 0004000000–0004999999 (standard POs only; STOs excluded)

Vendor on-time delivery by PO schedule line. EKBE GR events are matched to EKET schedule lines using a **cumulative quantity threshold** approach to correctly handle partial deliveries.

Schedule lines are sorted by due date ascending and assigned a running cumulative scheduled quantity (`cum_sched_qty`). GR events are filtered to positive receipts only (BEWTP='E', SHKZG='S'), sorted by posting date ascending, and assigned a running cumulative received quantity (`cum_gr_qty`). Each schedule line is matched to the first GR event where `cum_gr_qty >= cum_sched_qty` — the **completion event** for that line.

A schedule line scores as Open or Overdue until its full scheduled quantity has been cumulatively received. Partial deliveries do not score the line. The `gr_date` is the posting date of the completing GR, and `days_early_late` is positive when the vendor delivered early and negative when late.

Note: EKBE.ETENS (the SAP schedule line link field) is unpopulated in this extraction, so cumulative matching is used as the best available approach.

| Column | Type | Description |
|--------|------|-------------|
| purchasing_document_number | STRING | PO number (leading zeros stripped) |
| purchasing_document_item | STRING | PO item number (leading zeros stripped) |
| schedule_line_counter | STRING | Schedule line counter |
| purchasing_document_type | STRING | SAP document type (e.g. NB) |
| vendor_account_number | STRING | Vendor number (leading zeros stripped) |
| vendor_name | STRING | Vendor name |
| material_number | STRING | Material number (leading zeros stripped) |
| material_description | STRING | Material description |
| material_group | STRING | Material group |
| plant | STRING | Receiving plant |
| purchasing_group | STRING | Buyer code |
| purchasing_organization | STRING | Purchasing organization |
| purchasing_document_date | STRING | PO creation date (YYYYMMDD) |
| scheduled_delivery_date | STRING | Requested delivery date (YYYYMMDD) |
| statistics_delivery_date | STRING | Statistical delivery date (YYYYMMDD) |
| scheduled_quantity | DECIMAL(18,3) | Schedule line quantity (EKET.MENGE) |
| gr_quantity | DECIMAL(18,3) | GR quantity for this schedule line as maintained by SAP (EKET.WEMNG) |
| open_quantity | DECIMAL(18,3) | scheduled_quantity − gr_quantity |
| gr_date | STRING | Posting date of the rank-matched GR for this line (YYYYMMDD); NULL if no matching GR |
| gr_document_number | STRING | Accounting document number of the matched GR; NULL if no matching GR |
| delivered_on_time | BOOLEAN | True = gr_date on or before scheduled date; False = late; NULL = not yet received |
| days_early_late | INT | Positive = early, negative = late, NULL = open |
| otd_status | STRING | On Time \| Late \| Overdue \| Open |

**SAP source tables:** EKET, EKKO, EKPO, EKBE, LFA1, MAKT

---

## Dimension Tables

### material_class
**Mirrors:** Custom Mirion classification (no direct SAP equivalent)
**Granularity:** One row per material type + procurement type combination

Maps every combination of SAP material type (MARA.MTART) and procurement type (MARC.BESKZ) to a Mirion business classification. Built by cross joining distinct MTART values from MARA with distinct BESKZ values from MARC. Material types ZONT, ZHER, and HERS are excluded as inactive/legacy. Use this table to classify materials in reports without repeating the CASE logic.

| Column | Type | Description |
|--------|------|-------------|
| material_type | STRING | SAP material type code (MARA.MTART) |
| procurement_type | STRING | SAP procurement type code (MARC.BESKZ): F = external, E = in-house, X = both, blank = not set |
| material_class | STRING | Mirion classification: Tradable/Resale \| Raw \| Semi-FG \| FG \| Operating/Packing Supplies \| Service \| Non-Stock Material \| Unknown |

**SAP source tables:** MARA, MARC

---

### mirion_customer_numbers
**Mirrors:** Custom Mirion filter (no direct SAP equivalent)
**Granularity:** One row per Mirion-affiliated customer account (KNA1)

SAP customer accounts whose name contains Mirion, Canberra, Sun Nuclear, or Capintec — the internal intercompany customer accounts used across NA Tech sites. Deleted (LOEVM), blocked (SPERR), BLK3 test accounts, and French entity KUNNRs are excluded. `short_name` maps each KUNNR to a standardized Mirion site label for use in reports; accounts not in the mapping show TBD and should be reviewed.

| Column | Type | Description |
|--------|------|-------------|
| customer_number | STRING | SAP customer account number (leading zeros stripped) |
| customer_name | STRING | Customer name as stored in SAP |
| city | STRING | City from customer master |
| short_name | STRING | Mirion standardized site label (e.g. MIRION (MERIDEN)); TBD if not mapped |

**SAP source tables:** KNA1

---

### mirion_vendor_numbers
**Mirrors:** Custom Mirion filter (no direct SAP equivalent)
**Granularity:** One row per Mirion-affiliated vendor account (LFA1)

SAP vendor accounts whose name contains Mirion, Canberra, or Capintec — the internal intercompany vendor accounts used across NA Tech sites. `short_name` maps each LIFNR to a standardized Mirion site label; accounts not in the mapping show TBD and should be reviewed. Note: LIFNR values are a mix of numeric (e.g. 0000103998) and alphanumeric (e.g. V4020, R103998) — leading zeros are stripped only from fully numeric accounts.

| Column | Type | Description |
|--------|------|-------------|
| vendor_number | STRING | SAP vendor account number; leading zeros stripped for numeric accounts, alphanumeric kept as-is |
| vendor_name | STRING | Vendor name as stored in SAP |
| country | STRING | Country key from vendor master |
| short_name | STRING | Mirion standardized site label (e.g. MIRION (MERIDEN)); TBD if not mapped |

**SAP source tables:** LFA1

---

### movement_type
**Mirrors:** SAP T156 / T156T
**Granularity:** One row per movement type code (BWART), NA Tech active types only

SAP movement type reference table combining the standard SAP description (T156T.BTEXT) with Mirion plain-language descriptions and a high-level breakdown category. Only movement types with a Mirion description are included — these represent the types active in NA Tech. To include all SAP types, remove the `movement_type_text IS NOT NULL` filter in the notebook.

| Column | Type | Description |
|--------|------|-------------|
| movement_type | STRING | SAP movement type code (e.g. 101, 261) |
| sap_description | STRING | SAP standard English description from T156T |
| movement_type_text | STRING | Mirion plain-language description |
| breakdown | STRING | Goods Receipt \| Goods Issue \| Transfer \| Scrap \| Adjustment |

**SAP source tables:** T156, T156T

---

### payment_terms
**Mirrors:** SAP T052 / T052U
**Granularity:** One row per payment terms key (ZTERM)

Payment terms reference table for decoding ZTERM on purchase orders, vendor master records, and AP documents. Joins the configuration table (T052) with the English language description (T052U).

| Column | Type | Description |
|--------|------|-------------|
| payment_terms | STRING | SAP payment terms key (e.g. N030, Z001) |
| description | STRING | English description of the payment terms |
| baseline_date | STRING | Baseline date indicator for due date calculation |

**SAP source tables:** T052, T052U

---

### purchasing_groups
**Mirrors:** SAP T024
**Granularity:** One row per purchasing group (EKGRP)

Simple lookup table mapping purchasing group codes to buyer names. Used to decode EKGRP on purchase orders and purchase requisitions.

| Column | Type | Description |
|--------|------|-------------|
| purchasing_group | STRING | SAP purchasing group code (e.g. M01, P02) |
| buyer_name | STRING | Name of the buyer or purchasing group |

**SAP source tables:** T024

---

### so_document_type
**Mirrors:** SAP TVAK / TVAKT
**Granularity:** One row per sales document type (AUART)

Sales order document type reference table combining the SAP description with a Mirion `order_type` classification (Standard, Inquiry, Consignment, Customer Loan, Quotation, Return, Warranty/Repair). Document types not in the classification default to Unused.

| Column | Type | Description |
|--------|------|-------------|
| sales_doc_type | STRING | SAP sales document type code (e.g. ZOR, ZRE) |
| description | STRING | English description from TVAKT |
| order_type | STRING | Mirion classification: Standard \| Inquiry \| Consignment \| Customer Loan \| Quotation \| Return \| Warranty/Repair \| Unused |

**SAP source tables:** TVAK, TVAKT

---

### so_item_category
**Mirrors:** SAP TVAPT
**Granularity:** One row per sales document item category (PSTYV)

Sales order item category reference table. TVAPT contains both the code and the English description in a single table (filtered to SPRAS = 'E'), so no join is required. `billing_type` classifies each item category into Standard, Billing Plan, Milestone, or TBD.

| Column | Type | Description |
|--------|------|-------------|
| item_category | STRING | SAP item category code (e.g. ZTAN, ZSER, DLP) |
| description | STRING | English description from TVAPT |
| billing_type | STRING | Mirion classification: Standard \| Billing Plan \| Milestone \| TBD |

**SAP source tables:** TVAPT

---

### so_reject_codes
**Mirrors:** SAP TVAGT
**Granularity:** One row per rejection reason code (ABGRU)

Sales order rejection reason reference table. TVAGT carries both the code and English description in a single table. `status` classifies each rejection code as Canceled, Closed, or none — used to identify and exclude rejected/canceled lines in sales reporting.

| Column | Type | Description |
|--------|------|-------------|
| rejection_code | STRING | SAP rejection reason code (e.g. 08, 20, Z5) |
| description | STRING | English description from TVAGT |
| status | STRING | Mirion classification: Canceled \| Closed \| none |

**SAP source tables:** TVAGT

---

### storage_locations
**Mirrors:** SAP T001L
**Granularity:** One row per plant + storage location (WERKS + LGORT)

Storage location reference table for NA Tech plants. T001L is the SAP storage location master — one row per plant/storage location combination with a plain-language description. Filtered to plants 4002 (Concord), 4019 (Oak Ridge), 4020 (Meriden), 4021 (Olen), and 4022 (Oxfordshire).

| Column | Type | Description |
|--------|------|-------------|
| plant | STRING | Plant code (e.g. 4002, 4019) |
| storage_location | STRING | Storage location code within the plant; leading zeros removed for numeric codes |
| description | STRING | Description of the storage location from T001L.LGOBE |

**SAP source tables:** T001L

---

### delivery_blocks
**Mirrors:** SAP TVLST
**Granularity:** One row per delivery block code (LIFSP)

Delivery block reference table. A delivery block set on a sales order header (`VBAK.LIFSP`) prevents the order from being processed for shipment. Common blocks include credit holds, customer request holds, and quality checks. This table decodes those codes into plain-language descriptions.

| Column | Type | Description |
|--------|------|-------------|
| delivery_block | STRING | SAP delivery block code (e.g. 01, 02, ZD) |
| description | STRING | English description from TVLST |

**SAP source tables:** TVLST

---

### billing_blocks
**Mirrors:** SAP TVFST
**Granularity:** One row per billing block code (FAKSP)

Billing block reference table. A billing block set on a sales order header (`VBAK.FAKSP`) or line item (`VBAP.FAKSP`) prevents invoice creation for the order or item. Common uses include pending approval, disputed pricing, and incomplete documentation. This table decodes those codes into plain-language descriptions.

| Column | Type | Description |
|--------|------|-------------|
| billing_block | STRING | SAP billing block code (e.g. 01, 08, ZB) |
| description | STRING | English description from TVFST |

**SAP source tables:** TVFST

---

### profit_center
**Mirrors:** SAP CEPC + CEPCT + profit_center_hierarchy
**Granularity:** One row per active profit center (PRCTR) in BUMN controlling area

Profit center dimension with hierarchy context. CEPC is the authoritative source for all active profit centers — the hierarchy table is left joined to provide site and department groupings where available. The `profit_center_hierarchy` table has variable depth: Meriden and Oak Ridge profit centers sit at level 4 (with a department tier), while RMS, SIS, and Corporate profit centers are leaf nodes at level 3 (no department tier, `level_3_dept` is null). Profit centers absent from the hierarchy altogether will have all level columns null.

| Column | Type | Description |
|--------|------|-------------|
| profit_center | STRING | SAP profit center code (e.g. P100520, P103620) |
| description | STRING | English short text from CEPCT |
| lock_indicator | STRING | Blank = active; X = locked |
| level_3_dept | STRING | Department grouping node (null for RMS/SIS/Corporate and hierarchy gaps) |
| level_3_dept_desc | STRING | Department description |
| level_2_site | STRING | Site grouping node (e.g. MERIDEN.CY24, OAKRIDGE.CY24, RMS.CY24) |
| level_2_site_desc | STRING | Site description |
| level_1_top | STRING | Top-level hierarchy node |
| level_1_top_desc | STRING | Top-level description |

**SAP source tables:** CEPC, CEPCT, profit_center_hierarchy

---

### cost_center
**Mirrors:** SAP CSKS + CSKT
**Granularity:** One row per cost center per validity period (KOSTL + KOKRS + DATBI)

Cost center dimension for all controlling areas in client 400. Filtered to active records only (`DATBI = '9999-12-31'`). CSKT is joined on `KOSTL + KOKRS + DATBI` to avoid cross-product duplicates from date-ranged rows.

Note on `cost_center` field: SAP stores KOSTL as a 10-character padded field. Numeric codes like `0000035123` are stripped to `35123`; alphanumeric codes like `C02010` are kept as-is.

| Column | Type | Description |
|--------|------|-------------|
| cost_center | STRING | Cost center code (leading zeros stripped for numeric codes) |
| description | STRING | English description from CSKT |
| controlling_area | STRING | Controlling area (e.g. BUMN for NA Tech) |
| responsible_person | STRING | Person responsible from CSKS.VERAK |
| department | STRING | Department label from CSKS.ABTEI |

**SAP source tables:** CSKS, CSKT

---

## Open Items

| Item | Affects | Notes |
|------|---------|-------|
| MSKU table access | inventory | Special stock W (customer consignment) excluded |
| STXH / STXL access | purchase_order | Required for PO text block |
| Division decoder | inventory | MARA.SPART needs mapping table |
| line_creation_date | purchase_order | EKPO.CREATIONDATE unpopulated in extraction |
| material_reservation count | material_reservation | Row count does not match OR; needs investigation |
| cumulative match accuracy | vendor_otd | Cumulative qty matching assumes GRs flow in posting-date order; backdated receipts may shift completion event assignment. Validate OTD percentages against ME2M during testing |
