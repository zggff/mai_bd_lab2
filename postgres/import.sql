CREATE TABLE raw_data (
    id INT,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INT,
    customer_email VARCHAR,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price DECIMAL,
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price DECIMAL,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR,
    pet_category VARCHAR,
    product_weight DECIMAL,
    product_color VARCHAR,
    product_size VARCHAR,
    product_brand VARCHAR,
    product_material VARCHAR,
    product_description TEXT,
    product_rating DECIMAL,
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR
);

CREATE TEMP TABLE staging_table AS SELECT * FROM raw_data WITH NO DATA;

DO $$
DECLARE
    i INT;
    offset_val INT;
    file_path TEXT;
BEGIN
    FOR i IN 0..9 LOOP
        offset_val := i * 1000;
        file_path := '/data/MOCK_DATA (' || i || ').csv';

        TRUNCATE staging_table;

        EXECUTE format('COPY staging_table FROM %L WITH (FORMAT csv, HEADER true)', file_path);

        UPDATE staging_table SET 
            id = id + offset_val, 
            sale_customer_id = sale_customer_id + offset_val, 
            sale_seller_id = sale_seller_id + offset_val, 
            sale_product_id = sale_product_id + offset_val;
        
        INSERT INTO raw_data SELECT * FROM staging_table;
        
        RAISE NOTICE 'Processed file %', file_path;
    END LOOP;
END $$;
