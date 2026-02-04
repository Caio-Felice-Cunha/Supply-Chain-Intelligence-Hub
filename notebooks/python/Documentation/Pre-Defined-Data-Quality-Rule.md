# Pre-Defined Data Quality Rules
## Suppliers Table
✓ supplier_id_unique - Supplier ID must be unique (CRITICAL)

✓ reliability_score_range - Score between 0-100 (CRITICAL)

✓ lead_time_positive - Lead time must be positive (WARNING)

## Products Table
✓ product_id_unique - Product ID must be unique (CRITICAL)

✓ unit_cost_positive - Unit cost must be positive (CRITICAL)

✓ reorder_level_valid - Reorder level non-negative (WARNING)

✓ product_name_not_null - Product name required (CRITICAL)

## Inventory Table
✓ quantity_on_hand_valid - Quantity ≥ 0 (CRITICAL)

✓ quantity_reserved_valid - Reserved ≥ 0 (CRITICAL)

✓ reserved_not_exceed_onhand - Reserved ≤ on hand (CRITICAL)

## Orders Table
✓ order_quantity_positive - Quantity > 0 (CRITICAL)

✓ order_cost_positive - Cost ≥ 0 (CRITICAL)

✓ delivery_dates_logical - Delivery date > order date (WARNING)

## Sales Table
✓ quantity_sold_positive - Quantity > 0 (CRITICAL)

✓ revenue_positive - Revenue ≥ 0 (CRITICAL)

✓ revenue_quantity_consistency - Revenue/quantity > 0 (WARNING)