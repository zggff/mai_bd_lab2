CREATE TABLE raw_data (
    sale_id SERIAL PRIMARY KEY,
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

DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..9 LOOP
        EXECUTE format('
            COPY raw_data (
                id, customer_first_name, customer_last_name, customer_age,
                customer_email, customer_country, customer_postal_code,
                customer_pet_type, customer_pet_name, customer_pet_breed,
                seller_first_name, seller_last_name, seller_email,
                seller_country, seller_postal_code, product_name,
                product_category, product_price, product_quantity, sale_date,
                sale_customer_id, sale_seller_id, sale_product_id, sale_quantity,
                sale_total_price, store_name, store_location, store_city,
                store_state, store_country, store_phone, store_email,
                pet_category, product_weight, product_color, product_size,
                product_brand, product_material, product_description,
                product_rating, product_reviews, product_release_date,
                product_expiry_date, supplier_name, supplier_contact,
                supplier_email, supplier_phone, supplier_address,
                supplier_city, supplier_country
            )
            FROM ''/docker-entrypoint-initdb.d/raw_data/MOCK_DATA (%1$s).csv''
            DELIMITER '',''
            CSV HEADER;
        ', i);
    END LOOP;
END $$;
