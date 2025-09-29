CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    id SERIAL PRIMARY KEY,
    new_customers_count INTEGER DEFAULT 0,
    returning_customers_count INTEGER DEFAULT 0,
    refunded_customer_count INTEGER DEFAULT 0,
    period_name VARCHAR(20) NOT NULL,
    period_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    new_customers_revenue DECIMAL(15,2) DEFAULT 0,
    returning_customers_revenue DECIMAL(15,2) DEFAULT 0,
    customers_refunded INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(period_name, period_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_retention_period ON mart.f_customer_retention(period_name, period_id);
CREATE INDEX IF NOT EXISTS idx_customer_retention_item ON mart.f_customer_retention(item_id);

GRANT ALL ON TABLE mart.f_customer_retention TO airflow;
