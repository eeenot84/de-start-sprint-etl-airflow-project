CREATE TABLE IF NOT EXISTS mart.d_city (
    city_id INTEGER PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL,
    country VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO mart.d_city (
    city_id,
    city_name,
    country
)
SELECT DISTINCT
    city_id,
    city_name,
    country
FROM staging.customer_research
WHERE city_id IS NOT NULL
ON CONFLICT (city_id) 
DO UPDATE SET
    city_name = EXCLUDED.city_name,
    country = EXCLUDED.country,
    updated_at = CURRENT_TIMESTAMP;

ANALYZE mart.d_city;
