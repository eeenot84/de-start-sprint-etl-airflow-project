CREATE TABLE IF NOT EXISTS mart.f_sales (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    product_id INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'shipped',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'mart' 
        AND table_name = 'f_sales' 
        AND column_name = 'status'
    ) THEN
        ALTER TABLE mart.f_sales ADD COLUMN status VARCHAR(20) DEFAULT 'shipped';
        UPDATE mart.f_sales SET status = 'shipped' WHERE status IS NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_f_sales_date_time ON mart.f_sales(date_time);
CREATE INDEX IF NOT EXISTS idx_f_sales_customer_id ON mart.f_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_f_sales_product_id ON mart.f_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_f_sales_status ON mart.f_sales(status);

INSERT INTO mart.f_sales (
    date_time,
    product_id,
    price,
    quantity,
    customer_id,
    status
)
SELECT 
    date_time,
    product_id,
    price,
    quantity,
    customer_id,
    COALESCE(status, 'shipped') as status
FROM staging.user_order_log
WHERE date_time::date = '{{ params.date }}'::date
ON CONFLICT DO NOTHING;

ANALYZE mart.f_sales;
