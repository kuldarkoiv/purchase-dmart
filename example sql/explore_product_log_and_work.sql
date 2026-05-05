-- =============================================================================
-- Uurimis-SQL: product_log ja processing_work struktuur
-- Kasuta SSMS-is andmebaasis woodpecker_dev
-- Eesmärk: mõista, kas product_log / processing_work annab infot
--          pakkide tegeliku staatuse kohta (nt töös aga veel mitte tarbitud)
-- =============================================================================

-- 1) product_log kõik kirjed product_id=60262 jaoks
--    → vaata, mis description/action väärtused on, mis veerud on täidetud
SELECT *
FROM dbo.product_log
WHERE product_id = 60262
ORDER BY id;

-- =============================================================================

-- 2) product_log veergude NULL-analüüs (kogu tabel, ~1000 rea sample)
--    → näitab, mis veerud on sisuliselt alati NULL (pole väärtust lisada)
SELECT
    column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN col_val IS NULL THEN 1 ELSE 0 END) AS null_count,
    CAST(100.0 * SUM(CASE WHEN col_val IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,1)) AS null_pct
FROM (
    SELECT 'id'          AS column_name, CAST(id AS NVARCHAR(MAX))          AS col_val FROM dbo.product_log
    UNION ALL SELECT 'product_id',     CAST(product_id AS NVARCHAR(MAX))     FROM dbo.product_log
    UNION ALL SELECT 'description',    CAST(description AS NVARCHAR(MAX))    FROM dbo.product_log
    UNION ALL SELECT 'user_id',        CAST(user_id AS NVARCHAR(MAX))        FROM dbo.product_log
    UNION ALL SELECT 'wr_created',     CAST(wr_created AS NVARCHAR(MAX))     FROM dbo.product_log
) t
GROUP BY column_name
ORDER BY null_pct ASC;
-- NB: kui veergude nimed ei klapi, asenda ülal õigetega (vt päris veergude nimesid käsust 1)

-- =============================================================================

-- 3) processing_work nr 545 detailid
--    → vaata kõiki välju, mis on kasulik ETL-i lisada
SELECT *
FROM dbo.processing_work
WHERE id = 545;

-- =============================================================================

-- 4) processing_work veergude NULL-analüüs
--    → käivita pärast seda, kui tead täpseid veergude nimesid (käsust 3)
SELECT
    column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN col_val IS NULL THEN 1 ELSE 0 END) AS null_count,
    CAST(100.0 * SUM(CASE WHEN col_val IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,1)) AS null_pct
FROM (
    SELECT 'id'           AS column_name, CAST(id AS NVARCHAR(MAX))         AS col_val FROM dbo.processing_work
    UNION ALL SELECT 'number',           CAST(number AS NVARCHAR(MAX))      FROM dbo.processing_work
    UNION ALL SELECT 'description',      CAST(description AS NVARCHAR(MAX)) FROM dbo.processing_work
    UNION ALL SELECT 'status',           CAST(status AS NVARCHAR(MAX))      FROM dbo.processing_work  -- või status_id
    UNION ALL SELECT 'finished',         CAST(finished AS NVARCHAR(MAX))    FROM dbo.processing_work
    UNION ALL SELECT 'wr_created',       CAST(wr_created AS NVARCHAR(MAX))  FROM dbo.processing_work
) t
GROUP BY column_name
ORDER BY null_pct ASC;

-- =============================================================================

-- 5) Kui product_log sisaldab "added to work" tüüpi kirjeid:
--    → kas on eraldi veerg (nt processing_work_id) või on see description tekstis?
--    → kas ühel pakil saab olla mitu "added to work" kirjet?
SELECT
    description,
    COUNT(*) AS cnt
FROM dbo.product_log
WHERE description LIKE '%work%'
   OR description LIKE '%töö%'
GROUP BY description
ORDER BY cnt DESC;

-- =============================================================================

-- 6) product_id=60262 praegune seis product tabelis
--    → kontrolli, miks material_status = 'vaba' (mitte 'tarbitud')
SELECT
    p.id,
    p.actual_used,          -- peaks olema NOT NULL kui tarbitud
    p.wrote_off,            -- 1 = maha kantud
    p.shipment_id,          -- NOT NULL = ekspedeeritud
    p.contract_delivery_id, -- NOT NULL = müügilepingusse reserveeritud
    p.bron,                 -- 1 = kasutaja käsitsi broneeritud
    p.processing_work_id_in,    -- tootmistöö, mis lõi pakki
    p.processing_work_id_out,   -- tootmistöö, kuhu pakk anti (peaks olema täidetud kui töös)
    p.processing_step_id_made,
    p.processing_step_id_used
FROM dbo.product p
WHERE p.id = 60262;
