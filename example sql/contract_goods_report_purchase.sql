SELECT        cg.id, cg.contract_id, c.number_full AS contract, cg.row_number, cg.height, cg.width, cg.height2_fixed AS height2, cg.width2_fixed AS width2, cg.height3_fixed AS height3, cg.width3_fixed AS width3, cg.length_min, cg.length, 
                         art.article_group_id, art.article_group, t.name AS treatment, cert.name AS cert, icount.invoices, cg.status_to_customer, c.account_id, cr.volume, cr.volume2, cr.volume3, cr.meters, cr.meters_shipped, cr.meters_balance, 
                         cr.meters_stock, cr.meters_shipped * cg.volume_coef AS volume_shipped, cr.meters_balance * cg.volume_coef AS volume_balance, cr.meters_stock * cg.volume_coef AS volume_stock, 
                         cr.meters_shipped * cg.volume2_coef AS volume2_shipped, cr.meters_balance * cg.volume2_coef AS volume2_balance, cr.meters_stock * cg.volume2_coef AS volume2_stock, 
                         cr.meters_shipped * cg.volume3_coef AS volume3_shipped, cr.meters_balance * cg.volume3_coef AS volume3_balance, cr.meters_stock * cg.volume3_coef AS volume3_stock, cr.from_date, cr.to_date, icount.last_invoice_date, 
                         COALESCE (icount.last_invoice_date, cr.from_date) AS last_invoice_or_start, a.name AS account, s.name AS supplier, c.direction, c.archived
FROM            views.contract_goods_s AS cg INNER JOIN
                             (SELECT        cd.contract_goods_id, MIN(cd.due) AS from_date, MAX(cd.due) AS to_date, COALESCE (SUM(cd.volume), 0) AS volume, COALESCE (SUM(cd.volume2), 0) AS volume2, COALESCE (SUM(cd.volume3), 0) AS volume3, 
                                                         COALESCE (SUM(cd.meters), 0) AS meters, SUM(ISNULL(cdf.meters_shipped, 0)) AS meters_shipped, SUM(cd.meters - ISNULL(cdf.meters_shipped, 0)) AS meters_balance, SUM(ISNULL(cdf.meters_stock, 0)) 
                                                         AS meters_stock
                               FROM            views.contract_delivery_list AS cd WITH (NOLOCK) LEFT OUTER JOIN
                                                         views.contract_row_filled_base_purchase AS cdf ON cd.id = cdf.contract_delivery_id
                               WHERE        (cd.purchase = 1)
                               GROUP BY cd.contract_goods_id) AS cr ON cr.contract_goods_id = cg.id INNER JOIN
                         views.contract_s AS c ON cg.contract_id = c.id INNER JOIN
                         dbo.account AS a ON c.account_id = a.id INNER JOIN
                         dbo.account AS s ON c.account_id_supplier = s.id LEFT OUTER JOIN
                             (SELECT        COUNT(*) AS invoices, MAX(invoice_date) AS last_invoice_date, contract_goods_id
                               FROM            views.contract_goods_report_invoice
                               GROUP BY contract_goods_id) AS icount ON cg.id = icount.contract_goods_id LEFT OUTER JOIN
                         dbo.treatment AS t ON cg.treatment_id = t.id LEFT OUTER JOIN
                         webrock.wr_enum_value AS cert ON cg.cert_id = cert.id LEFT OUTER JOIN
                         views.article_dyn AS art ON cg.article_id = art.id