DROP TABLE IF EXISTS data.journal_slow_update;
CREATE TEMPORARY TABLE data.journal_slow_update LIKE data.sku;
REPLACE INTO data.journal_slow_update
SELECT 
    c.id_catalog_config AS `ID Catalog Config`,
    s.sku AS `SKU Simple`,
    c.sku AS `SKU Config`,
    CAST(PREG_REPLACE("/[\r\n]+/", " ", c.name) AS CHAR) AS `SKU Name`,
    s.status AS `Status`,
    s.created_at AS `Created At`,
    s.updated_at AS `Updated At`,
    b.name AS `Brand`,
    CASE
        WHEN c.supplier_name IS NOT NULL THEN c.supplier_name
        ELSE s.supplier_simple
    END AS `Seller Name`,
    CASE
        WHEN spl.type = 'supplier' THEN 'Retail'
        WHEN stype.name = 'Dropshipping' THEN 'MP'
        WHEN stype.name = 'Own Warehouse' THEN 'FBL'        
        WHEN stype.name = 'Cross docking' THEN 'Cross Docking'
        ELSE NULL
    END AS `SKU Type`,
    ct.`name` AS `Primary Category`,
    cat.`Cat 1` AS `Category 1`,
    cat.`Cat 2` AS `Category 2`,
    cat.`Cat 3` AS `Category 3`,
    cat.`Cat 4` AS `Category 4`,
    cat.`Cat 5` AS `Category 5`,
    IF((c.marketplace_parent_sku IS NOT NULL AND c.marketplace_parent_sku != '')
            OR (c.marketplace_children_skus IS NOT NULL AND c.marketplace_children_skus != ''),
        IF(c.marketplace_parent_sku IS NULL OR c.marketplace_parent_sku = '', c.sku, c.marketplace_parent_sku),
        NULL) AS `Parent SKU`,
    CASE
        WHEN
            (c.marketplace_parent_sku IS NOT NULL AND c.marketplace_parent_sku != '')
            OR (c.marketplace_children_skus IS NOT NULL AND c.marketplace_children_skus != '')
        THEN
            'YES'
        ELSE 'NO'
    END AS `Is Multisourced`,
    CAST(SUBSTRING_INDEX(CONVERT( PREG_REPLACE('/,/',
                        '',
                        PREG_REPLACE('/([^0-9.,]+)/', 'x', c.product_measures)) USING UTF8),
                'x',
                1)
        AS DECIMAL (14 , 2 )) * CASE
        WHEN c.product_measures like '%cm%' THEN 10
        ELSE 1
    END AS `BOB Product Height`,
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONVERT( PREG_REPLACE('/,/',
                                '',
                                PREG_REPLACE('/([^0-9.,]+)/', 'x', c.product_measures)) USING UTF8),
                        'x',
                        2),
                'x',
                - 1)
        AS DECIMAL (14 , 2 )) * CASE
        WHEN c.product_measures like '%cm%' THEN 10
        ELSE 1
    END AS `BOB Product Length`,
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONVERT( PREG_REPLACE('/,/',
                                '',
                                PREG_REPLACE('/([^0-9.,]+)/', 'x', c.product_measures)) USING UTF8),
                        'x',
                        3),
                'x',
                - 1)
        AS DECIMAL (14 , 2 )) * CASE
        WHEN c.product_measures like '%cm%' THEN 10
        ELSE 1
    END AS `BOB Product Width`,
    CAST(SUBSTRING_INDEX(CONVERT( PREG_REPLACE('/,/',
                        '',
                        PREG_REPLACE('/([^0-9.,]+)/', 'x', c.product_weight)) USING UTF8),
                'x',
                1)
        AS DECIMAL (14 , 6 )) / CASE
        WHEN c.product_weight like '%g%'  THEN 0.001
        ELSE 1
    END AS `BOB Product Height`,
    cspu.height * 10 AS `OMS Height`,
    cspu.length * 10 AS `OMS Length`,
    cspu.width * 10 AS `OMS Width`,
    cspu.weight AS `OMS Weight`,
    c.package_height * 10 AS `BOB Package Height`,
    c.package_length * 10 AS `BOB Package Length`,
    c.package_width * 10 AS `BOB Package Width`,
    c.package_weight AS `BOB Package Weight`,
    c.color AS `Colour`,
    s.price AS `Price`,
    s.price AS `Retail Price`,
    s.special_price AS `Special Price`,
    s.special_from_date AS `Special Price From Date`,
    s.special_to_date AS `Special Price To Date`,
    st.quantity AS `Stock`,
    st.updated_at AS `Stock Updated At`,
	'NO' AS `Visibility`,
    (SELECT 
            SUM(o3.`Paid Price`)
        FROM
            data.oms o3
        WHERE
            s.sku = o3.sku
                AND o3.`Item Status` IN ('delivered' , 'replaced')
                AND o3.`Item Status Last Update` > NOW() - INTERVAL 7 DAY
        GROUP BY o3.sku) AS `NMV L7D`,
    (SELECT 
            SUM(o3.`Paid Price`)
        FROM
            data.oms o3
        WHERE
            s.sku = o3.sku
                AND o3.`Item Status` IN ('delivered' , 'replaced')
                AND o3.`Item Status Last Update` > NOW() - INTERVAL 30 DAY
        GROUP BY o3.sku) AS `NMV LM`,
    (SELECT 
            COUNT(o3.`BOB Sales Order Item`)
        FROM
            data.oms o3
        WHERE
            s.sku = o3.sku
                AND o3.`Outbound Logistics - Shipped Status` IN ('shipped')
                AND o3.`Item Status Last Update` > NOW() - INTERVAL 7 DAY
        GROUP BY o3.sku) AS `Items Sold L7D`,
    (SELECT 
            COUNT(o3.`BOB Sales Order Item`)
        FROM
            data.oms o3
        WHERE
            s.sku = o3.sku
                AND o3.`Outbound Logistics - Shipped Status` IN ('shipped')
                AND o3.`Item Status Last Update` > NOW() - INTERVAL 30 DAY
        GROUP BY o3.sku) AS `Items Sold LM`,
    grp.name AS `Return Policy`,
    NULL AS `Origin City`,
    CAST(PREG_REPLACE("/[\r\n]+/", " ", c.description) AS CHAR) AS `Description`,
    CAST(PREG_REPLACE("/[\r\n]+/", " ", c.package_content) AS CHAR) AS `Package Content`,
    CAST(PREG_REPLACE("/[\r\n]+/", " ", c.product_warranty) AS CHAR) AS `Warranty`,
    stype.name AS `Shipment Type`,
    seller.short_code AS short_code,
    0 AS Flag,
	spl.id_supplier src_id
FROM
    bob_live.catalog_simple s
        LEFT JOIN
    bob_live.catalog_config c ON c.id_catalog_config = s.fk_catalog_config
        LEFT JOIN
    bob_live.catalog_attribute_option_global_return_policies grp ON grp.id_catalog_attribute_option_global_return_policies = c.fk_catalog_attribute_option_global_return_policies
        LEFT JOIN
    bob_live.catalog_brand b ON b.id_catalog_brand = c.fk_catalog_brand
        LEFT JOIN
    staging.category cat ON cat.cat_id = c.primary_category
        LEFT JOIN
    bob_live.catalog_category ct ON ct.id_catalog_category = c.primary_category
        LEFT JOIN
    bob_live.catalog_source so ON so.fk_catalog_simple = s.id_catalog_simple
        LEFT JOIN
    bob_live.supplier spl ON spl.id_supplier = so.fk_supplier
	    LEFT JOIN
    asc_live.seller seller ON spl.id_supplier = seller.src_id
        LEFT JOIN
    bob_live.catalog_shipment_type stype ON stype.id_catalog_shipment_type = so.fk_catalog_shipment_type
        LEFT JOIN
    bob_live.catalog_stock st ON st.fk_catalog_source = so.id_catalog_source
        LEFT JOIN
    bob_live.warehouse w ON st.fk_warehouse = w.id_warehouse
        LEFT JOIN
    oms_live.ims_product ip ON ip.sku = s.sku
        LEFT JOIN
    bob_live.catalog_simple_package_unit cspu ON cspu.fk_catalog_simple = s.id_catalog_simple
WHERE
	s.updated_at BETWEEN NOW() - INTERVAL @start_hours HOUR AND NOW() - INTERVAL @end_hours HOUR 	 
	OR s.created_at BETWEEN NOW() - INTERVAL @start_hours HOUR AND NOW() - INTERVAL @end_hours HOUR  	
	OR c.created_at BETWEEN NOW() - INTERVAL @start_hours HOUR AND NOW() - INTERVAL @end_hours HOUR
	OR c.updated_at BETWEEN NOW() - INTERVAL @start_hours HOUR AND NOW() - INTERVAL @end_hours HOUR;
    
DELETE FROM data.journal_slow_update WHERE `SKU Simple` = '' OR `SKU Simple` IS NULL;


USE `data`;
UPDATE (SELECT 
        sku,
            IFNULL(SUM(quantity), 0) AS quantity,
            COUNT(*) AS source_count
    FROM
        (SELECT 
        cs.sku,
            CAST(MID(MAX(CONCAT(cstock.updated_at, quantity)), 20)
                AS DECIMAL) AS quantity,
            MAX(cstock.updated_at) AS updated_at,
            cstock.fk_catalog_source,
            cstock.fk_warehouse
    FROM
        bob_live.catalog_stock cstock
    LEFT JOIN bob_live.catalog_source csource ON cstock.fk_catalog_source = csource.id_catalog_source
    LEFT JOIN bob_live.catalog_simple cs ON csource.fk_catalog_simple = cs.id_catalog_simple
    WHERE
        csource.status_source = 'active'
    GROUP BY fk_catalog_source , fk_warehouse) abc
    GROUP BY sku) def
        LEFT JOIN
    data.journal_slow_update sku ON def.sku = sku.`SKU Simple` 
SET 
    sku.`Stock` = def.quantity;




-- UPDATE data.journal_slow_update sku
-- SET 
--     sku.`Stock` = IFNULL((SELECT 
--             SUM(quantity)
--         FROM
--             bob_live.catalog_simple c_simple
--                 LEFT JOIN
--             bob_live.catalog_source c_source ON c_source.fk_catalog_simple = c_simple.id_catalog_simple
--                 LEFT JOIN
--             bob_live.catalog_stock c_stock ON c_stock.fk_catalog_source = c_source.id_catalog_source
--         WHERE
--             c_simple.sku = sku.`SKU Simple`), 0);

-- UPDATE data.journal_slow_update o
--         LEFT JOIN
--     bob_live.supplier ss ON ss.name = o.`Seller Name`
--         LEFT JOIN
--     oms_live.ims_supplier is1 ON is1.name = o.`Seller Name`
--         LEFT JOIN
--     bob_live.supplier_address sa ON sa.fk_supplier = ss.id_supplier
--         LEFT JOIN
--     bob_live.country_region cr ON cr.id_country_region = sa.fk_country_region
--         LEFT JOIN
--     bob_live.supplier_address sa2 ON sa2.fk_supplier = is1.bob_id_supplier
--         LEFT JOIN
--     bob_live.country_region cr2 ON cr2.id_country_region = sa2.fk_country_region
--         LEFT JOIN
--     bob_live.catalog_simple c_simple ON c_simple.sku = o.`SKU Simple`
--         LEFT JOIN
--     bob_live.catalog_source c_source ON c_source.fk_catalog_simple = c_simple.id_catalog_simple
--         LEFT JOIN
--     bob_live.catalog_stock c_stock ON c_stock.fk_catalog_source = c_source.id_catalog_source
--         LEFT JOIN
--     bob_live.warehouse w ON w.id_warehouse = c_stock.fk_warehouse 
-- SET 
--     o.`Origin City` = CASE
--         WHEN
--             o.`Shipment Type` = 'Dropshipping'
--         THEN
--             IFNULL(IF(cr.name = 'Hồ Chí Minh',
--                         'HCMC',
--                         cr.name),
--                     IF(cr2.name = 'Hồ Chí Minh',
--                         'HCMC',
--                         cr2.name))
--         WHEN
--             o.`Shipment Type` IN ('Own Warehouse' , 'Cross docking')
--                 AND w.name IN ('HCM Warehouse' , NULL,
--                 'Default Warehouse1',
--                 'Ninja Warehouse HCM')
--         THEN
--             'HCMC'
--         WHEN
--             o.`Shipment Type` IN ('Own Warehouse' , 'Cross docking')
--                 AND w.name IN ('Hanoi' , 'Ninja WH HN')
--         THEN
--             'Hà Nội'
--         ELSE IFNULL(IF(cr.name = 'Hồ Chí Minh',
--                     'HCMC',
--                     cr.name),
--                 IF(cr2.name = 'Hồ Chí Minh',
--                     'HCMC',
--                     cr2.name))
--     END
-- WHERE
--     `Origin City` IS NULL
--         AND (cr.name IS NOT NULL
--         OR cr2.name IS NOT NULL);

       
-- Update Visibility of all SKUs

UPDATE bob_live.catalog_simple cs 
        LEFT JOIN
    data.journal_slow_update sku ON cs.sku = sku.`SKU Simple`
        LEFT JOIN
    bob_live.catalog_config cc ON cc.id_catalog_config = sku.`ID Catalog Config`
        LEFT JOIN
    bob_live.catalog_brand b ON b.id_catalog_brand = cc.fk_catalog_brand 
SET 
    sku.`Visibility` = IF((SELECT 
                SUM(IF(cc.status = 'active'
                            AND cs.status = 'active'
                            AND csource.status_source = 'active'
                            AND cc.pet_status = 'creation,edited,images'
                            AND spl.status = 'active'
                            AND b.status = 'active'
                            AND cc.pet_approved = 1,
                        1,
                        0))
            FROM
                bob_live.catalog_source csource
                    LEFT JOIN
                bob_live.supplier spl ON spl.id_supplier = csource.fk_supplier
            WHERE
                csource.fk_catalog_simple = cs.id_catalog_simple) > 0,
        'YES',
        'NO');
        
REPLACE INTO data.sku SELECT * FROM data.journal_slow_update;
DROP TABLE IF EXISTS data.journal_slow_update;
        
