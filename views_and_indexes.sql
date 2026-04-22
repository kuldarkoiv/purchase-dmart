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
    pmp.species_name,
    pmp.grade_name,
    pmp.treatment_name,
    pmp.spec_height,
    pmp.spec_width,
    pmp.spec_length_min,
    pmp.spec_length,
    pmp.cert_name,
    pmp.supplier_name,
    pmp.purchase_contract_number,
    COUNT(*)                        AS free_packs,
    STRING_AGG(DISTINCT pmp.product_comment, ' | ')
        FILTER (WHERE pmp.product_comment IS NOT NULL AND pmp.product_comment <> '')
                                    AS planner_comments,
    STRING_AGG(DISTINCT pol.marking_comments, ' | ')
        FILTER (WHERE pol.marking_comments IS NOT NULL AND pol.marking_comments <> '')
                                    AS marking_comments
FROM purchase_dmart.purchase_material_products pmp
LEFT JOIN purchase_dmart.purchase_order_lines pol
    ON pmp.order_line_id = pol.order_line_id
WHERE pmp.material_status = 'vaba'
GROUP BY
    pmp.species_name, pmp.grade_name, pmp.treatment_name, pmp.spec_height, pmp.spec_width,
    pmp.spec_length_min, pmp.spec_length, pmp.cert_name, pmp.supplier_name,
    pmp.purchase_contract_number
ORDER BY pmp.species_name, pmp.grade_name;

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

-- =============================================================================
-- v_material_availability
-- Müügi ühtne vaade: vaba laovaru + tulevikus saabuvad kogused
-- Üks rida = üks dimensioon/liik/kvaliteet/leping kombinatsioon
-- segment = 'vaba_ladu' (laos) | 'tulemas' (tellitud, pole veel kohale jõudnud)
-- m3 arvutus: spec_height_mm * spec_width_mm / 1_000_000 * pack_meters
-- =============================================================================
CREATE OR REPLACE VIEW purchase_dmart.v_material_availability AS
WITH free_stock AS (
    SELECT
        pmp.species_name,
        pmp.grade_name,
        pmp.treatment_name,
        pmp.spec_height                                     AS height,
        pmp.spec_width                                      AS width,
        pmp.spec_length_min                                 AS length_min,
        pmp.spec_length                                     AS length,
        pmp.cert_name,
        pmp.supplier_name,
        pmp.purchase_contract_number,
        pmp.purchase_contract_archived,
        pmp.order_due_date,
        pol.marking_comments,
        COUNT(*)                                            AS packs,
        ROUND(SUM(
            pmp.spec_height * pmp.spec_width * pmp.spec_length / 1000000000.0
        )::NUMERIC, 3)                                      AS m3
    FROM purchase_dmart.purchase_material_products pmp
    LEFT JOIN purchase_dmart.purchase_order_lines pol
        ON pmp.order_line_id = pol.order_line_id
    WHERE pmp.material_status = 'vaba'
    GROUP BY
        pmp.species_name, pmp.grade_name, pmp.treatment_name,
        pmp.spec_height, pmp.spec_width, pmp.spec_length_min, pmp.spec_length,
        pmp.cert_name, pmp.supplier_name, pmp.purchase_contract_number,
        pmp.purchase_contract_archived, pmp.order_due_date, pol.marking_comments),
pending AS (
    SELECT
        pol.species_name,
        pol.grade_name,
        pol.treatment_name,
        pol.height,
        pol.width,
        pol.length_min,
        pol.length,
        pol.cert_name,
        pol.supplier_name,
        pol.purchase_contract_number,
        pol.purchase_contract_archived,
        pol.order_due_date,
        pol.marking_comments,
        pol.pending_packs                                   AS packs,
        ROUND((pol.height * pol.width / 1000000.0 * pol.pending_meters)::NUMERIC, 3) AS m3
    FROM purchase_dmart.purchase_order_lines pol
    WHERE pol.pending_meters > 0
      AND pol.is_finished IS NOT TRUE
      AND pol.is_active = 1
)
SELECT 'vaba_ladu'  AS segment,
    species_name, grade_name, treatment_name,
    height, width, length_min, length, cert_name,
    supplier_name, purchase_contract_number, purchase_contract_archived,
    order_due_date, marking_comments, packs::NUMERIC AS packs, m3
FROM free_stock
UNION ALL
SELECT 'tulemas'    AS segment,
    species_name, grade_name, treatment_name,
    height, width, length_min, length, cert_name,
    supplier_name, purchase_contract_number, purchase_contract_archived,
    order_due_date, marking_comments,
    packs::TEXT::NUMERIC AS packs,
    m3
FROM pending
ORDER BY species_name, grade_name, height, width, length, segment;

GRANT SELECT ON purchase_dmart.v_material_availability TO doadmin;
