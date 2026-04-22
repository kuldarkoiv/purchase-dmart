-- Views ja indeksid (käivita pärast ETL-i)
-- Tabelid loob etl.py automaatselt, vaated tuleb käsitsi taastada

CREATE INDEX IF NOT EXISTS idx_pol_supplier        ON purchase_dmart.purchase_order_lines (supplier_id);
CREATE INDEX IF NOT EXISTS idx_pol_due_date        ON purchase_dmart.purchase_order_lines (order_due_date);
CREATE INDEX IF NOT EXISTS idx_pol_is_overdue      ON purchase_dmart.purchase_order_lines (is_overdue);
CREATE INDEX IF NOT EXISTS idx_pol_contract        ON purchase_dmart.purchase_order_lines (purchase_contract_id);
CREATE INDEX IF NOT EXISTS idx_pol_article_group   ON purchase_dmart.purchase_order_lines (article_group_id);

CREATE INDEX IF NOT EXISTS idx_pmp_order_line      ON purchase_dmart.purchase_material_products (order_line_id);
CREATE INDEX IF NOT EXISTS idx_pmp_status          ON purchase_dmart.purchase_material_products (material_status);
CREATE INDEX IF NOT EXISTS idx_pmp_supplier        ON purchase_dmart.purchase_material_products (supplier_id);
CREATE INDEX IF NOT EXISTS idx_pmp_due             ON purchase_dmart.purchase_material_products (order_due_date);
CREATE INDEX IF NOT EXISTS idx_pmp_sales_contract  ON purchase_dmart.purchase_material_products (sales_contract_id);
CREATE INDEX IF NOT EXISTS idx_pmp_waybill         ON purchase_dmart.purchase_material_products (waybill_id);
CREATE INDEX IF NOT EXISTS idx_pmp_article_group   ON purchase_dmart.purchase_material_products (article_group_id);
CREATE INDEX IF NOT EXISTS idx_pmp_arrival         ON purchase_dmart.purchase_material_products (effective_arrival_date);

CREATE OR REPLACE VIEW purchase_dmart.v_free_stock_by_article AS
SELECT
    species_name,
    grade_name,
    treatment_name,
    spec_height,
    spec_width,
    spec_length_min,
    spec_length,
    cert_name,
    supplier_name,
    purchase_contract_number,
    COUNT(*)                        AS free_packs,
    STRING_AGG(DISTINCT product_comment, ' | ')
        FILTER (WHERE product_comment IS NOT NULL AND product_comment <> '')
                                    AS planner_comments
FROM purchase_dmart.purchase_material_products
WHERE material_status = 'vaba'
GROUP BY
    species_name, grade_name, treatment_name, spec_height, spec_width,
    spec_length_min, spec_length, cert_name, supplier_name,
    purchase_contract_number
ORDER BY article_group, species_name, grade_name;

GRANT SELECT ON purchase_dmart.v_free_stock_by_article TO doadmin;

CREATE OR REPLACE VIEW purchase_dmart.v_overdue_orders AS
SELECT
    order_line_id,
    purchase_contract_number,
    supplier_name,
    article_group,
    article_name,
    species_name,
    height, width, length,
    order_due_date,
    days_since_due,
    ordered_packs,
    received_packs,
    pending_packs,
    fulfillment_pct,
    order_comment
FROM purchase_dmart.purchase_order_lines
WHERE is_overdue = 1
ORDER BY days_since_due DESC;

GRANT SELECT ON purchase_dmart.v_overdue_orders TO doadmin;

CREATE OR REPLACE VIEW purchase_dmart.v_supplier_delivery_performance AS
SELECT
    supplier_id,
    supplier_name,
    COUNT(*)                                    AS total_received_packs,
    SUM(CASE WHEN is_on_time = 1 THEN 1 ELSE 0 END)
                                                AS on_time_packs,
    SUM(CASE WHEN is_on_time = 0 THEN 1 ELSE 0 END)
                                                AS late_packs,
    ROUND(
        100.0 * SUM(CASE WHEN is_on_time = 1 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    )                                           AS on_time_pct,
    ROUND(AVG(days_late::NUMERIC), 1)           AS avg_days_late,
    MAX(days_late)                              AS max_days_late
FROM purchase_dmart.purchase_material_products
WHERE days_late IS NOT NULL
GROUP BY supplier_id, supplier_name
ORDER BY on_time_pct ASC;

GRANT SELECT ON purchase_dmart.v_supplier_delivery_performance TO doadmin;

CREATE OR REPLACE VIEW purchase_dmart.v_material_journey AS
SELECT
    pmp.product_id,
    pmp.product_label,
    pmp.purchase_contract_number,
    pmp.supplier_name,
    pmp.article_group,
    pmp.article_name,
    pmp.species_name,
    pmp.grade_name,
    pmp.spec_height,
    pmp.spec_width,
    pmp.spec_length,
    pmp.order_due_date,
    pmp.effective_arrival_date,
    pmp.days_late,
    pmp.is_on_time,
    pmp.material_status,
    pmp.consumed_at,
    pmp.wrote_off_date,
    pmp.sales_contract_number,
    pmp.customer_name,
    pmp.sales_delivery_due,
    pmp.product_comment,
    pmp.order_line_comment,
    pol.is_overdue                              AS order_is_overdue,
    pol.days_since_due                          AS order_days_since_due,
    pol.fulfillment_pct                         AS order_fulfillment_pct
FROM purchase_dmart.purchase_material_products pmp
LEFT JOIN purchase_dmart.purchase_order_lines pol
    ON pmp.order_line_id = pol.order_line_id;

GRANT SELECT ON purchase_dmart.v_material_journey TO doadmin;
