-- =============================================================================
-- extract_received_products.sql
-- MSSQL -> PostgreSQL ETL: vastu võetud ostetud materjalid (pakid/rullid/jms)
-- Üks rida = üks toote pakk (product), mis on seotud ostutellimuse reaga.
-- Sisaldab: ostutellimuse info, saatelehe info, tähtaegsus, praegune staatus,
--           müügilepingu seos, planeerija kommentaar.
-- =============================================================================

SELECT

    -- -------------------------------------------------------------------------
    -- TOOTE (PAKK) IDENTIFIKAATOR
    -- -------------------------------------------------------------------------
    p.id                                AS product_id,
    p.number1                           AS product_label,       -- füüsiline märgis pakil
    p.number2                           AS product_number2,
    p.number_original                   AS product_number_original,

    -- -------------------------------------------------------------------------
    -- OSTULEPINGU RIDA (CONTRACT_DELIVERY - purchase tellimus)
    -- -------------------------------------------------------------------------
    cd_pur.id                           AS order_line_id,
    cd_pur.delivery_number              AS order_line_number,
    cd_pur.due                          AS order_due_date,       -- lubatud tarnekuupäev
    cd_pur.due_seller                   AS order_due_seller,     -- müüja-poolne lubad tähtaeg
    cd_pur.meters                       AS order_meters,
    cd_pur.packs                        AS order_packs,
    cd_pur.mark                         AS order_mark,
    cd_pur.finished                     AS order_finished,       -- tellimus märgitud lõpetatuks
    cd_pur.comment                      AS order_line_comment,   -- planeerija kommentaar real
    cdl_pur.price_m3                    AS price_m3,               -- toorme hind €/m3

    -- -------------------------------------------------------------------------
    -- OSTULEPING (CONTRACT - purchase)
    -- -------------------------------------------------------------------------
    c_pur.id                            AS purchase_contract_id,
    ISNULL(c_pur.number_prefix, '') + CAST(c_pur.number AS NVARCHAR) 
                                        AS purchase_contract_number,
    c_pur.order_number                  AS purchase_order_number,
    c_pur.sentdate                      AS purchase_contract_date,
    c_pur.archived                      AS purchase_contract_archived,

    -- -------------------------------------------------------------------------
    -- TARNIJA (SUPPLIER)
    -- -------------------------------------------------------------------------
    a_sup.id                            AS supplier_id,
    a_sup.name                          AS supplier_name,

    -- -------------------------------------------------------------------------
    -- KAUBA SPETSIFIKATSIOON (DIMENSIONS, SPECIES, ARTICLE)
    -- -------------------------------------------------------------------------
    -- Müügimõõdud kontraktilt (nominaal)
    cg.height                           AS spec_height,
    cg.width                            AS spec_width,
    COALESCE(cg.length_min, cg.length) AS spec_length_min,
    cg.length                           AS spec_length,
    -- Tegelikud ostumõõdud (mõõdetud pakil)
    p.height_purchase                   AS actual_height,
    p.width_purchase                    AS actual_width,
    -- Artikkel / tootegrupp
    art.id                              AS article_id,
    art.article_group_id,
    art.article_group,
    COALESCE(art.article_base, art.article_der) AS article_name,
    art.type_id                         AS article_type_id,
    -- Liik, kvaliteet, töötlus, sert
    sp.name                             AS species_name,
    g.name                              AS grade_name,
    tr.name                             AS treatment_name,
    ev_cert.name                        AS cert_name,

    -- -------------------------------------------------------------------------
    -- SAATELEHT / OSTUARVE (PURCHASE_WAYBILL)
    -- -------------------------------------------------------------------------
    pw.id                               AS waybill_id,
    ISNULL(pw.number_prefix, '') 
        + ISNULL(CAST(pw.number AS NVARCHAR), '') 
        + ISNULL(pw.number_suffix, '') AS waybill_number,
    pw.number_seller                    AS waybill_seller_number,
    pw.sentdate                         AS waybill_sent_date,    -- dokumendi väljaandmise kuupäev
    pw.arrival_date                     AS waybill_arrival_date, -- tegelik saabumiskuupäev *** PÕHILINE ***
    COALESCE(pw.arrival_date, pw.sentdate)
                                        AS effective_arrival_date,

    -- -------------------------------------------------------------------------
    -- TÄHTAEGSUSE ANALÜÜS
    -- -------------------------------------------------------------------------
    -- Positiivne = hilines, negatiivne = tuli varakult, 0 = täpselt õigel ajal
    DATEDIFF(day, cd_pur.due, COALESCE(pw.arrival_date, pw.sentdate))
                                        AS days_late,
    CASE
        WHEN COALESCE(pw.arrival_date, pw.sentdate) IS NULL THEN NULL  -- pole veel saabunud
        WHEN DATEDIFF(day, cd_pur.due, COALESCE(pw.arrival_date, pw.sentdate)) <= 0 THEN 1
        ELSE 0
    END                                 AS is_on_time,           -- 1 = õigeaegne, 0 = hilines

    -- -------------------------------------------------------------------------
    -- MATERJALI PRAEGUNE STAATUS (TEEKOND)
    -- -------------------------------------------------------------------------
    p.bron                              AS is_reserved_bron,     -- kasutaja poolt kinni pandud
    p.wrote_off                         AS is_wrote_off,         -- maha kantud (kadu/häving)
    p.wrote_off_date_c                  AS wrote_off_date,
    CASE WHEN p.actual_used IS NOT NULL    THEN 1 ELSE 0 END
                                        AS is_consumed,          -- tarbitud tootmises
    p.actual_used                       AS consumed_at,          -- millal tarbiti
    CASE WHEN p.shipment_id IS NOT NULL   THEN 1 ELSE 0 END
                                        AS is_shipped,           -- ekspedeeritud kliendile
    CASE WHEN p.contract_delivery_id IS NOT NULL OR p.bron = 1 THEN 1 ELSE 0 END
                                        AS is_allocated,         -- reserveeritud müügilepingusse
    p.actual_creation                   AS received_at,          -- süsteemi sisestamise aeg
    p.actual_finished                   AS finished_at,          -- töötluse lõpp (kui oli töötlus)
    p.done                              AS is_done,
    p.fn                                AS is_finished_good,     -- 1 = valmistoode (ei tohiks olla purchased material puhul)

    -- Koondstaatus (prioriteet: maha kantud > tarbitud > ekspedeeritud > reserveeritud > vaba)
    CASE
        WHEN p.wrote_off = 1                                            THEN 'maha_kantud'
        WHEN p.actual_used IS NOT NULL                                  THEN 'tarbitud'
        WHEN p.shipment_id IS NOT NULL                                  THEN 'ekspedeeritud'
        WHEN p.contract_delivery_id IS NOT NULL OR p.bron = 1           THEN 'reserveeritud'
        ELSE                                                                 'vaba'
    END                                 AS material_status,

    -- -------------------------------------------------------------------------
    -- TOOTMISE SEOS (kuidas materjali kasutati)
    -- -------------------------------------------------------------------------
    p.processing_work_id_in             AS processing_work_received,  -- tootmistöö, mis lõi selle pakk (nt lõikus)
    p.processing_work_id_out            AS processing_work_consumed,  -- tootmistöö, kuhu pakk anti (nt liimimine)
    p.processing_step_id_made          AS processing_step_made,
    p.processing_step_id_used          AS processing_step_used,
    p.product_id_parent                 AS parent_product_id,         -- vanem-pakk (kui splititud)

    -- -------------------------------------------------------------------------
    -- MÜÜGILEPINGU SEOS (kui materjal on reserveeritud/kasutatud)
    -- -------------------------------------------------------------------------
    c_sal.id                            AS sales_contract_id,
    ISNULL(c_sal.number_prefix, '') + CAST(c_sal.number AS NVARCHAR)
                                        AS sales_contract_number,
    c_sal.order_number                  AS sales_order_number,
    a_cust.id                           AS customer_id,
    a_cust.name                         AS customer_name,
    cd_sal.id                           AS sales_delivery_id,
    cd_sal.due                          AS sales_delivery_due,        -- millal kliendile vaja
    cd_sal.delivery_number              AS sales_delivery_number,

    -- -------------------------------------------------------------------------
    -- KOMMENTAARID (planeerija märkused)
    -- -------------------------------------------------------------------------
    p.comment                           AS product_comment,           -- pakitaseme kommentaar
    cd_pur.comment                      AS order_line_comment_full,   -- ostutellimuse rea kommentaar

    -- -------------------------------------------------------------------------
    -- TEHNILINE AUDIT
    -- -------------------------------------------------------------------------
    p.wr_created                        AS product_created_at,
    p.wr_modified                       AS product_modified_at,
    p.shipment_id,
    p.invoice_id                        AS purchase_invoice_id,
    p.invoice_row_id                    AS purchase_invoice_row_id

FROM dbo.product p

-- Ostulepingu rida (MIKS see pakk osteti)
INNER JOIN dbo.contract_delivery cd_pur
    ON p.purchase_contract_row_id = cd_pur.id

-- Lepingukaup (spetsifikatsioon)
INNER JOIN dbo.contract_goods cg
    ON cd_pur.contract_goods_id = cg.id

-- Ostuleping
INNER JOIN dbo.contract c_pur
    ON cg.contract_id = c_pur.id

-- Tarnija
INNER JOIN dbo.account a_sup
    ON c_pur.account_id_supplier = a_sup.id

-- Saateleht/ostuarve (millal saabuvs)
LEFT JOIN dbo.purchase_waybill pw
    ON p.purchase_waybill_id = pw.id

-- Liik, kvaliteet, töötlus, sert
LEFT JOIN dbo.species sp
    ON p.species_id = sp.id

LEFT JOIN dbo.grade g
    ON p.grade_id = g.id

LEFT JOIN dbo.treatment tr
    ON p.treatment_id = tr.id

LEFT JOIN webrock.wr_enum_value ev_cert
    ON p.cert_id = ev_cert.id

-- Artikkel
LEFT JOIN views.article_dyn art
    ON p.article_id = art.id

-- Müügilepingu seos
LEFT JOIN dbo.contract c_sal
    ON p.contract_id = c_sal.id

LEFT JOIN dbo.account a_cust
    ON c_sal.account_id = a_cust.id

LEFT JOIN dbo.contract_delivery cd_sal
    ON p.contract_delivery_id = cd_sal.id

-- price_m3 ostutellimuse realt (views.contract_delivery_list kaudu)
LEFT JOIN views.contract_delivery_list cdl_pur
    ON cd_pur.id = cdl_pur.id

WHERE
    -- Ainult ostetud materjal (mitte ise toodetud)
    p.purchase_contract_row_id IS NOT NULL
    -- Välista maha kantud (wrote_off) vanad kirjed? - KOMMENTEERI LAHTI KUI SOOVID KA NEID
    -- AND p.wrote_off = 0
