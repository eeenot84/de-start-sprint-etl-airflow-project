CREATE TABLE IF NOT EXISTS mart.d_item (
    item_id INTEGER PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    category_id INTEGER,
    category_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO mart.d_item (
    item_id,
    item_name,
    category_id,
    category_name
)
SELECT DISTINCT
    product_id as item_id,
    product_name as item_name,
    category_id,
    category_name
FROM staging.user_order_log
WHERE product_id IS NOT NULL
ON CONFLICT (item_id) 
DO UPDATE SET
    item_name = EXCLUDED.item_name,
    category_id = EXCLUDED.category_id,
    category_name = EXCLUDED.category_name,
    updated_at = CURRENT_TIMESTAMP;

ANALYZE mart.d_item;
