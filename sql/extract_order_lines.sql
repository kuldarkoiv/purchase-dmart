-- =============================================================================
-- extract_order_lines.sql
-- MSSQL -> PostgreSQL ETL: kõik ostutellimuste read koos täitmise seisuga.
-- Üks rida = üks ostutellimuse tarneliin (contract_delivery, purchase=1).
-- Sisaldab: tellitud kogus, vastu võetud, laos, allesjäänud, tähtaegsus.
-- NB! See päring näitab KA tellimuseid, mille materjal pole veel kohale jõudnud.
-- =============================================================================

SELECT

    -- -------------------------------------------------------------------------
    -- OSTUTELLIMUSE RIDA
    -- -------------------------------------------------------------------------
    cd.id                               AS order_line_id,
    cd.delivery_number                  AS order_line_number,
    cd.due                              AS order_due_date,
    cd.due_seller                       AS order_due_seller,
    cd.meters                           AS ordered_meters,
    cd.packs                            AS ordered_packs,
    cd.volume                           AS ordered_volume,
    cd.square_meters                    AS ordered_sqm,
    cd.pieces                           AS ordered_pieces,
    cd.mark                             AS order_mark,
    cd.comment                          AS order_comment,          -- planeerija kommentaar real
    cd.finished                         AS is_finished,            -- käsitsi märgitud lõpetatuks
    cd.production_date                  AS production_due_date,    -- millal valmis peab olema
    cd.notification_time                AS notification_due,        -- teavitamise tähtaeg

    -- -------------------------------------------------------------------------
    -- OSTULEPING
    -- -------------------------------------------------------------------------
    c.id                                AS purchase_contract_id,
    ISNULL(c.number_prefix, '') + CAST(c.number AS NVARCHAR)
                                        AS purchase_contract_number,
    c.order_number                      AS purchase_order_number,
    c.sentdate                          AS contract_sent_date,

    -- -------------------------------------------------------------------------
    -- TARNIJA
    -- -------------------------------------------------------------------------
    a_sup.id                            AS supplier_id,
    a_sup.name                          AS supplier_name,

    -- -------------------------------------------------------------------------
    -- KAUBA SPETSIFIKATSIOON
    -- -------------------------------------------------------------------------
    cg.id                               AS contract_goods_id,
    cg.row_number                       AS goods_row_number,
    cg.height,
    cg.width,
    COALESCE(cg.length_min, cg.length) AS length_min,
    cg.length,
    art.article_group_id,
    art.article_group,
    COALESCE(art.article_base, art.article_der) AS article_name,
    art.type_id                         AS article_type_id,
    sp.name                             AS species_name,
    g.name                              AS grade_name,
    tr.name                             AS treatment_name,
    ev_cert.name                        AS cert_name,
    cg.quality,
    cg.material_spec,

    -- -------------------------------------------------------------------------
    -- TÄITMISE SEIS (vastu võetud kogused)
    -- -------------------------------------------------------------------------
    -- Vastu võetud (done = saabunud lattu)
    ISNULL(cdf.packs_done, 0)           AS received_packs,
    ISNULL(cdf.meters_done, 0)          AS received_meters,
    -- Laos (done - shipped)
    ISNULL(cdf.packs_stock, 0)          AS stock_packs,
    ISNULL(cdf.meters_stock, 0)         AS stock_meters,
    -- Ekspedeeritud (lahkunud laost)
    ISNULL(cdf.packs_shipped, 0)        AS shipped_packs,
    ISNULL(cdf.meters_shipped, 0)       AS shipped_meters,
    -- Allesjäänud (veel pole kohale jõudnud)
    cd.packs - ISNULL(cdf.packs_done, 0)    AS pending_packs,
    cd.meters - ISNULL(cdf.meters_done, 0)  AS pending_meters,
    -- Balance (tellitud - ekspedeeritud)
    cd.packs - ISNULL(cdf.packs_shipped, 0)     AS balance_packs,
    cd.meters - ISNULL(cdf.meters_shipped, 0)   AS balance_meters,

    -- -------------------------------------------------------------------------
    -- TÄITMISE PROTSENT (kiire ülevaade)
    -- -------------------------------------------------------------------------
    CASE
        WHEN cd.meters > 0
        THEN ROUND(CAST(100.0 * ISNULL(cdf.meters_done, 0) / cd.meters AS DECIMAL(8,1)), 1)
        ELSE NULL
    END                                 AS fulfillment_pct,

    -- -------------------------------------------------------------------------
    -- TÄHTAEGSUSE ANALÜÜS (tarnerea tasemel)
    -- -------------------------------------------------------------------------
    GETDATE()                           AS snapshot_date,
    -- Kas tarneliin on praeguseks tänasega võrreldes ületähtaegne?
    CASE
        WHEN cd.finished = 1 THEN 0                         -- lõpetatud, pole ületähtaegne
        WHEN cd.due IS NULL THEN NULL                        -- tähtaeg puudub
        WHEN cd.due < GETDATE()
             AND cd.meters > ISNULL(cdf.meters_done, 0) THEN 1
        ELSE 0
    END                                 AS is_overdue,       -- 1 = ületähtaegne tellimus

    -- Päevade arv tähtajast (positiivne = hilines/ületähtaegne)
    CASE
        WHEN cd.due IS NOT NULL
        THEN DATEDIFF(day, cd.due, GETDATE())
        ELSE NULL
    END                                 AS days_since_due,

    -- Esimese saatelehe saabumiskuupäev (millal esimene pakk kohale jõudis)
    first_arrivals.first_arrival_date,
    first_arrivals.last_arrival_date,
    -- Kui palju aega kulus: due -> esimene pakk kohale
    CASE
        WHEN first_arrivals.first_arrival_date IS NOT NULL AND cd.due IS NOT NULL
        THEN DATEDIFF(day, cd.due, first_arrivals.first_arrival_date)
        ELSE NULL
    END                                 AS days_first_arrival_vs_due,

    -- -------------------------------------------------------------------------
    -- ARVED (invoice info)
    -- -------------------------------------------------------------------------
    inv_agg.invoice_count,
    inv_agg.last_invoice_date,
    inv_agg.first_invoice_date,

    -- -------------------------------------------------------------------------
    -- AKTIIVSUS
    -- -------------------------------------------------------------------------
    CASE
        WHEN cd.finished = 1 THEN 0
        WHEN (cd.packs - ISNULL(cdf.packs_shipped, 0) - ISNULL(cdf.packs_stock, 0)) <= 0 THEN 0
        ELSE 1
    END                                 AS is_active

FROM dbo.contract_delivery cd

INNER JOIN dbo.contract_goods cg
    ON cd.contract_goods_id = cg.id

INNER JOIN dbo.contract c
    ON cg.contract_id = c.id

-- Ainult ostulepingud (direction/purchase flag)
INNER JOIN (
    SELECT cd2.id
    FROM views.contract_delivery_list cd2
    WHERE cd2.purchase = 1
) AS purchase_filter
    ON cd.id = purchase_filter.id

INNER JOIN dbo.account a_sup
    ON c.account_id_supplier = a_sup.id

-- Täitmisandmed (packs_done, meters_done jms)
LEFT JOIN views.contract_row_filled_base_purchase cdf
    ON cd.id = cdf.contract_delivery_id

LEFT JOIN dbo.species sp
    ON cg.species_id = sp.id

LEFT JOIN dbo.grade g
    ON cg.grade_id = g.id

LEFT JOIN dbo.treatment tr
    ON cg.treatment_id = tr.id

LEFT JOIN webrock.wr_enum_value ev_cert
    ON cg.cert_id = ev_cert.id

LEFT JOIN views.article_dyn art
    ON cg.article_id = art.id

-- Esimese/viimase saabumiskuupäev (läbi product -> purchase_waybill)
LEFT JOIN (
    SELECT
        p2.purchase_contract_row_id,
        MIN(COALESCE(pw2.arrival_date, pw2.sentdate)) AS first_arrival_date,
        MAX(COALESCE(pw2.arrival_date, pw2.sentdate)) AS last_arrival_date
    FROM dbo.product p2
    INNER JOIN dbo.purchase_waybill pw2
        ON p2.purchase_waybill_id = pw2.id
    WHERE p2.purchase_contract_row_id IS NOT NULL
    GROUP BY p2.purchase_contract_row_id
) AS first_arrivals
    ON cd.id = first_arrivals.purchase_contract_row_id

-- Arvete koondinfo
LEFT JOIN (
    SELECT
        cgr.contract_goods_id,
        COUNT(*) AS invoice_count,
        MIN(cgr.invoice_date) AS first_invoice_date,
        MAX(cgr.invoice_date) AS last_invoice_date
    FROM views.contract_goods_report_invoice cgr
    GROUP BY cgr.contract_goods_id
) AS inv_agg
    ON cg.id = inv_agg.contract_goods_id

WHERE cd.archived = 0
