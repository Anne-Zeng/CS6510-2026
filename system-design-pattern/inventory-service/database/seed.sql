-- 功能：与 Catalog 演示数据使用相同 SKU；名称为本服务的显示快照。
INSERT INTO inventory(sku,name,current_stock)
SELECT 'SKU-' || lpad(n::text,6,'0'), 'Store Item ' || lpad(n::text,6,'0'), 1000
FROM generate_series(1,20) n
ON CONFLICT(sku) DO NOTHING;
