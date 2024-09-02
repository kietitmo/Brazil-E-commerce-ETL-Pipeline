-- Cấp quyền cho user để có thể sử dụng LOAD DATA INFILE
GRANT FILE ON *.* TO 'myuser'@'%';
FLUSH PRIVILEGES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id VARCHAR(255) NULL,
    order_item_id VARCHAR(255) NULL,
    product_id VARCHAR(255) NULL,
    seller_id VARCHAR(255) NULL,
    shipping_limit_date DATETIME NULL,
    price FLOAT NULL,
    freight_value FLOAT NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id VARCHAR(255) NULL,
    customer_unique_id VARCHAR(255) NULL,
    customer_zip_code_prefix VARCHAR(255) NULL,
    customer_city VARCHAR(255) NULL,
    customer_state VARCHAR(255) NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(255) NULL,
    geolocation_lat FLOAT NULL,
    geolocation_lng FLOAT NULL,
    geolocation_city VARCHAR(255) NULL,
    geolocation_state VARCHAR(255) NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id VARCHAR(255) NULL,
    payment_sequential FLOAT NULL,
    payment_type VARCHAR(255) NULL,
    payment_installments INT NULL,
    payment_value FLOAT NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id VARCHAR(255) NULL,
    order_id VARCHAR(255) NULL,
    review_score INT NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message TEXT NULL,
    review_creation_date DATETIME NULL,
    review_answer_timestamp DATETIME NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id VARCHAR(255) NULL,
    customer_id VARCHAR(255) NULL,
    order_status VARCHAR(255) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id VARCHAR(255) NULL,
    product_category_name VARCHAR(255) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_products_dataset.csv'
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id VARCHAR(255) NULL,
    seller_zip_code_prefix VARCHAR(255) NULL,
    seller_city VARCHAR(255) NULL,
    seller_state VARCHAR(255) NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Tạo bảng nếu chưa tồn tại và nhập dữ liệu từ file CSV
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name VARCHAR(255) NULL,
    product_category_name_english VARCHAR(255) NULL
);

-- Nhập dữ liệu từ file CSV
LOAD DATA INFILE '/var/lib/mysql-files/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
