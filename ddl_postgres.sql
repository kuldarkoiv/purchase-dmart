-- =============================================================================
-- ddl_postgres.sql
-- PostgreSQL dmart skeem: ostetud materjalide teekond ja laoseis
-- Allikas: MSSQL woodpecker_dev (ETL via etl.py)
-- =============================================================================

-- Katkesta idle ühendused (vabastan lukud mis jäid eelmistest katsetest)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state = 'idle'
  AND query_start < NOW() - INTERVAL '1 minute';

-- Lock timeout - kui 15 sekundiga ei saa lukku, anna viga (ära jää ootama)
SET lock_timeout = '15s';

-- Schema
CREATE SCHEMA IF NOT EXISTS purchase_dmart;

GRANT USAGE ON SCHEMA purchase_dmart TO doadmin;

-- =============================================================================
-- TABEL 1: purchase_order_lines
-- Ostutellimuste read koos täitmise seisu ja tähtaegsuse infoga.
-- Üks rida = üks tarneliin (contract_delivery, purchase lepingust).
-- =============================================================================

DROP TABLE IF EXISTS purchase_dmart.purchase_order_lines CASCADE;

CREATE TABLE purchase_dmart.purchase_order_lines (

    -- Primaaravõti
    order_line_id               INT             NOT NULL,

    -- Ostutellimuse rida
    order_line_number           INT,
    order_due_date              DATE,
    order_due_seller            DATE,
    ordered_meters              NUMERIC(12,3),
    ordered_packs               INT,
    ordered_volume              NUMERIC(12,4),
    ordered_sqm                 NUMERIC(12,3),
    ordered_pieces              INT,
    order_mark                  TEXT,
    order_comment               TEXT,            -- planeerija kommentaar real
    is_finished                 BOOLEAN,
    production_due_date         DATE,
    notification_due            TIMESTAMP,

    -- Ostuleping
    purchase_contract_id        INT,
    purchase_contract_number    TEXT,
    purchase_order_number       TEXT,
    contract_sent_date          DATE,

    -- Tarnija
    supplier_id                 INT,
    supplier_name               TEXT,

    -- Kauba spetsifikatsioon
    contract_goods_id           INT,
    goods_row_number            INT,
    height                      NUMERIC(8,2),
    width                       NUMERIC(8,2),
    length_min                  NUMERIC(8,2),
    length                      NUMERIC(8,2),
    article_group_id            INT,
    article_group               TEXT,
    article_name                TEXT,
    article_type_id             INT,
    species_name                TEXT,
    grade_name                  TEXT,
    treatment_name              TEXT,
    cert_name                   TEXT,
    quality                     TEXT,
    material_spec               TEXT,

    -- Täitmise seis
    received_packs              INT,
    received_meters             NUMERIC(12,3),
    stock_packs                 INT,
    stock_meters                NUMERIC(12,3),
    shipped_packs               INT,
    shipped_meters              NUMERIC(12,3),
    pending_packs               INT,            -- pole veel kohale jõudnud
    pending_meters              NUMERIC(12,3),
    balance_packs               INT,
    balance_meters              NUMERIC(12,3),
    fulfillment_pct             NUMERIC(10,1),  -- täitmise %

    -- Tähtaegsus
    snapshot_date               TIMESTAMP,
    is_overdue                  BOOLEAN,        -- kas praegune hetk on pärast tähtaega ja pole täidetud
    days_since_due              INT,            -- positiivne = ületähtaegne
    first_arrival_date          DATE,           -- millal esimene pakk saabus
    last_arrival_date           DATE,
    days_first_arrival_vs_due   INT,            -- esimese saabumise hilinejmine (positiivne = hilines)

    -- Arved
    invoice_count               INT,
    first_invoice_date          DATE,
    last_invoice_date           DATE,

    -- Aktiivsus
    is_active                   BOOLEAN,

    -- ETL metaandmed
    etl_loaded_at               TIMESTAMP       NOT NULL DEFAULT NOW(),

    PRIMARY KEY (order_line_id)
);

COMMENT ON TABLE purchase_dmart.purchase_order_lines IS
    'Ostutellimuste read koos täitmise seisu ja tähtaegsusega. '
    'is_overdue=true tähendab, et tähtaeg on möödunud aga materjal pole täielikult kohale jõudnud.';

COMMENT ON COLUMN purchase_dmart.purchase_order_lines.order_comment IS
    'Planeerija kommentaar ostutellimuse real (nt tootmise juhised või märkused)';
COMMENT ON COLUMN purchase_dmart.purchase_order_lines.days_first_arrival_vs_due IS
    'Positiivne = materjal saabus pärast tähtaega (hilinenult). Negatiivne = saabus varakult.';

GRANT SELECT ON purchase_dmart.purchase_order_lines TO doadmin;


-- =============================================================================
-- TABEL 2: purchase_material_products
-- Individuaalsed toote pakid - ostetud materjali teekond laos.
-- Üks rida = üks pakk (product) mis on seotud ostutellimuse reaga.
-- =============================================================================

DROP TABLE IF EXISTS purchase_dmart.purchase_material_products CASCADE;

CREATE TABLE purchase_dmart.purchase_material_products (

    -- Primaaravõti
    product_id                  INT             NOT NULL,

    -- Pakk identifikaator
    product_label               TEXT,           -- füüsiline märgis pakil (number1)
    product_number2             BIGINT,
    product_number_original     TEXT,

    -- Link ostutellimusele
    order_line_id               INT,            -- FK -> purchase_order_lines.order_line_id
    order_line_number           INT,
    order_due_date              DATE,           -- koopeeritud paremaks pärimiseks
    order_due_seller            DATE,
    order_mark                  TEXT,
    order_line_comment          TEXT,           -- planeerija kommentaar ostutellimuse real

    -- Ostuleping
    purchase_contract_id        INT,
    purchase_contract_number    TEXT,
    purchase_order_number       TEXT,
    purchase_contract_date      DATE,

    -- Tarnija
    supplier_id                 INT,
    supplier_name               TEXT,

    -- Tegelikud mõõdud (mõõdetud pakil)
    actual_height               NUMERIC(8,2),
    actual_width                NUMERIC(8,2),
    -- Nominaalmõõdud kontraktilt
    spec_height                 NUMERIC(8,2),
    spec_width                  NUMERIC(8,2),
    spec_length_min             NUMERIC(8,2),
    spec_length                 NUMERIC(8,2),

    -- Spetsifikatsioon
    article_id                  INT,
    article_group_id            INT,
    article_group               TEXT,
    article_name                TEXT,
    article_type_id             INT,
    species_name                TEXT,
    grade_name                  TEXT,
    treatment_name              TEXT,
    cert_name                   TEXT,

    -- Saateleht/ostuarve
    waybill_id                  INT,
    waybill_number              TEXT,
    waybill_seller_number       TEXT,
    waybill_sent_date           DATE,
    waybill_arrival_date        DATE,           -- tegelik saabumiskuupäev (PÕHILINE)
    effective_arrival_date      DATE,           -- arrival_date või sentdate (fallback)

    -- Tähtaegsuse analüüs (pakitasemel)
    days_late                   INT,            -- positiivne = hilines vs order_due_date
    is_on_time                  BOOLEAN,        -- true = saabus õigeaegselt

    -- Materjali staatus (PÕHILINE TEEKOND)
    -- 'vaba' | 'reserveeritud' | 'tarbitud' | 'ekspedeeritud' | 'maha_kantud'
    material_status             TEXT            NOT NULL,
    is_reserved_bron            BOOLEAN,        -- käsitsi reserveeritud (bron flag)
    is_allocated                BOOLEAN,        -- seotud müügilepinguga
    is_consumed                 BOOLEAN,        -- tarbitud tootmises (actual_used IS NOT NULL)
    consumed_at                 TIMESTAMP,      -- millal tarbiti
    is_shipped                  BOOLEAN,        -- ekspedeeritud (shipment_id IS NOT NULL)
    is_wrote_off                BOOLEAN,        -- maha kantud (kadu/häving)
    wrote_off_date              TIMESTAMP,
    is_done                     BOOLEAN,
    is_finished_good            BOOLEAN,        -- peaks olema false ostetud materjalide puhul
    received_at                 TIMESTAMP,      -- süsteemi sisestamise aeg

    -- Tootmise seos
    processing_work_received    INT,            -- tootmistöö, mis lõi selle pakk
    processing_work_consumed    INT,            -- tootmistöö, kuhu pakk sisendina läks
    processing_step_made        INT,
    processing_step_used        INT,
    parent_product_id           INT,            -- kui pakk on splititud suuremast

    -- Müügilepingu seos
    sales_contract_id           INT,
    sales_contract_number       TEXT,
    sales_order_number          TEXT,
    customer_id                 INT,
    customer_name               TEXT,
    sales_delivery_id           INT,
    sales_delivery_due          DATE,           -- millal kliendile vaja
    sales_delivery_number       INT,

    -- Kommentaarid
    product_comment             TEXT,           -- pakitaseme planeerija kommentaar
    order_line_comment_full     TEXT,           -- ostutellimuse rea kommentaar (täis)

    -- Tehniline
    shipment_id                 INT,
    purchase_invoice_id         INT,
    purchase_invoice_row_id     INT,
    product_created_at          TIMESTAMP,
    product_modified_at         TIMESTAMP,

    -- ETL metaandmed
    etl_loaded_at               TIMESTAMP       NOT NULL DEFAULT NOW(),

    PRIMARY KEY (product_id)
);

COMMENT ON TABLE purchase_dmart.purchase_material_products IS
    'Ostetud materjalide individuaalsed pakid koos teekonnaga. '
    'material_status näitab pakki praegust seisundit: '
    'vaba (laos vaba), reserveeritud (müügilepingusse kinni pandud), '
    'tarbitud (tootmises ära kasutatud), ekspedeeritud (kliendile saadetud), '
    'maha_kantud (kadu/häving).';

COMMENT ON COLUMN purchase_dmart.purchase_material_products.days_late IS
    'Positiivne = pakk saabus pärast ostutellimuse tähtaega (hilinenult). '
    'Negatiivne = saabus varakult. NULL = pole veel saabunud.';

COMMENT ON COLUMN purchase_dmart.purchase_material_products.material_status IS
    'vaba | reserveeritud | tarbitud | ekspedeeritud | maha_kantud';

COMMENT ON COLUMN purchase_dmart.purchase_material_products.product_comment IS
    'Planeerija märkus konkreetsel pakil (nt kasutusotstare, erand, märge)';

GRANT SELECT ON purchase_dmart.purchase_material_products TO doadmin;


-- =============================================================================
-- INDEKSID (pärimise kiirendamiseks)
-- =============================================================================

CREATE INDEX idx_pol_supplier        ON purchase_dmart.purchase_order_lines (supplier_id);
CREATE INDEX idx_pol_due_date        ON purchase_dmart.purchase_order_lines (order_due_date);
CREATE INDEX idx_pol_is_overdue      ON purchase_dmart.purchase_order_lines (is_overdue);
CREATE INDEX idx_pol_contract        ON purchase_dmart.purchase_order_lines (purchase_contract_id);
CREATE INDEX idx_pol_article_group   ON purchase_dmart.purchase_order_lines (article_group_id);

CREATE INDEX idx_pmp_order_line      ON purchase_dmart.purchase_material_products (order_line_id);
CREATE INDEX idx_pmp_status          ON purchase_dmart.purchase_material_products (material_status);
CREATE INDEX idx_pmp_supplier        ON purchase_dmart.purchase_material_products (supplier_id);
CREATE INDEX idx_pmp_due             ON purchase_dmart.purchase_material_products (order_due_date);
CREATE INDEX idx_pmp_sales_contract  ON purchase_dmart.purchase_material_products (sales_contract_id);
CREATE INDEX idx_pmp_waybill         ON purchase_dmart.purchase_material_products (waybill_id);
CREATE INDEX idx_pmp_article_group   ON purchase_dmart.purchase_material_products (article_group_id);
CREATE INDEX idx_pmp_arrival         ON purchase_dmart.purchase_material_products (effective_arrival_date);


-- =============================================================================
-- VAATED (views) - korduvkasutatavad päringud
-- =============================================================================

-- Vaade 1: Vaba laovaru artiklite kaupa
CREATE OR REPLACE VIEW purchase_dmart.v_free_stock_by_article AS
SELECT
    article_group_id,
    article_group,
    article_name,
    species_name,
    grade_name,
    treatment_name,
    spec_height,
    spec_width,
    spec_length_min,
    spec_length,
    cert_name,
    supplier_name,
    COUNT(*)                        AS free_packs,
    -- meters pole pakitasemel - kui vaja, lisa lähtebaasi põhjal
    STRING_AGG(DISTINCT product_comment, ' | ')
        FILTER (WHERE product_comment IS NOT NULL AND product_comment <> '')
                                    AS planner_comments
FROM purchase_dmart.purchase_material_products
WHERE material_status = 'vaba'
GROUP BY
    article_group_id, article_group, article_name, species_name,
    grade_name, treatment_name, spec_height, spec_width,
    spec_length_min, spec_length, cert_name, supplier_name
ORDER BY article_group, species_name, grade_name;

COMMENT ON VIEW purchase_dmart.v_free_stock_by_article IS
    'Vaba laovaru artiklite kaupa - pakid, mis pole reserveeritud, tarbitud ega ekspedeeritud.';

GRANT SELECT ON purchase_dmart.v_free_stock_by_article TO doadmin;


-- Vaade 2: Ületähtaegsed ostutellimused
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
WHERE is_overdue = TRUE
ORDER BY days_since_due DESC;

COMMENT ON VIEW purchase_dmart.v_overdue_orders IS
    'Ostutellimuste read, mille tähtaeg on möödunud ja materjalid pole täielikult kohale jõudnud.';

GRANT SELECT ON purchase_dmart.v_overdue_orders TO doadmin;


-- Vaade 3: Tarnija täptidelineanalüüs (on-time delivery rate tarnija kaupa)
CREATE OR REPLACE VIEW purchase_dmart.v_supplier_delivery_performance AS
SELECT
    supplier_id,
    supplier_name,
    COUNT(*)                                    AS total_received_packs,
    SUM(CASE WHEN is_on_time = TRUE THEN 1 ELSE 0 END)
                                                AS on_time_packs,
    SUM(CASE WHEN is_on_time = FALSE THEN 1 ELSE 0 END)
                                                AS late_packs,
    ROUND(
        100.0 * SUM(CASE WHEN is_on_time = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    )                                           AS on_time_pct,
    ROUND(AVG(days_late::NUMERIC), 1)           AS avg_days_late,
    MAX(days_late)                              AS max_days_late
FROM purchase_dmart.purchase_material_products
WHERE days_late IS NOT NULL   -- ainult pakid, mille kohta saabumisinfo olemas
GROUP BY supplier_id, supplier_name
ORDER BY on_time_pct ASC;     -- kõige halvemad üleval

COMMENT ON VIEW purchase_dmart.v_supplier_delivery_performance IS
    'Tarnijate tarneetäpsuse statistika - kui suur % pakkidest saabus õigeaegselt '
    'võrrelduna ostutellimuse due kuupäevaga.';

GRANT SELECT ON purchase_dmart.v_supplier_delivery_performance TO doadmin;


-- Vaade 4: Materjali teekond (täismaatriks)
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
    -- Müügilepingu seos
    pmp.sales_contract_number,
    pmp.customer_name,
    pmp.sales_delivery_due,
    -- Kommentaarid
    pmp.product_comment,
    pmp.order_line_comment,
    -- Ostutellimuse tähtaegsus koondülevaade
    pol.is_overdue                              AS order_is_overdue,
    pol.days_since_due                          AS order_days_since_due,
    pol.fulfillment_pct                         AS order_fulfillment_pct
FROM purchase_dmart.purchase_material_products pmp
LEFT JOIN purchase_dmart.purchase_order_lines pol
    ON pmp.order_line_id = pol.order_line_id;

COMMENT ON VIEW purchase_dmart.v_material_journey IS
    'Täielik materjali teekond: ostutellimus -> saabumine -> staatus (vaba/reserveeritud/tarbitud jne)';

GRANT SELECT ON purchase_dmart.v_material_journey TO doadmin;
