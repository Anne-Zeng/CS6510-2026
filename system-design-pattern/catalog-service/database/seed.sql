-- 准备演示商品，默认 20 个 SKU。
INSERT INTO catalog_items(sku, name, price)
SELECT 'SKU-' || lpad(n::text, 6, '0'),
       'Store Item ' || lpad(n::text, 6, '0'),
       (100 + n % 1000)::numeric / 100
FROM generate_series(1, 20) n
ON CONFLICT (sku) DO NOTHING;
