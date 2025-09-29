CREATE TABLE IF NOT EXISTS mart.d_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_surname VARCHAR(255),
    customer_primary_email VARCHAR(255),
    customer_primary_phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO mart.d_customer (
    customer_id,
    customer_name,
    customer_surname,
    customer_primary_email,
    customer_primary_phone
)
SELECT DISTINCT
    customer_id,
    customer_name,
    customer_surname,
    customer_primary_email,
    customer_primary_phone
FROM staging.customer_research
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_id) 
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    customer_surname = EXCLUDED.customer_surname,
    customer_primary_email = EXCLUDED.customer_primary_email,
    customer_primary_phone = EXCLUDED.customer_primary_phone,
    updated_at = CURRENT_TIMESTAMP;

ANALYZE mart.d_customer;
