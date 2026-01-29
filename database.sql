-- =====================================================
-- STAR SCHEMA FOR SALES DATA WAREHOUSE (MySQL Version)
-- =====================================================

-- Drop tables in correct order (fact table first, then dimensions, then temp)
DROP TABLE IF EXISTS FACT_SALES;
DROP TABLE IF EXISTS DIM_PRODUCT;
DROP TABLE IF EXISTS DIM_STORE;
DROP TABLE IF EXISTS DIM_SUPPLIER;
DROP TABLE IF EXISTS DIM_DATE;
DROP TABLE IF EXISTS DIM_CUSTOMER;
DROP TABLE IF EXISTS temp_master;
DROP TABLE IF EXISTS temp_transactions;
DROP TABLE IF EXISTS temp_customer;

-- =====================================================
-- TEMPORARY TABLES (to load raw CSV data)
-- =====================================================

-- Temp table for customer_master_data.csv
CREATE TABLE temp_customer (
    row_index INT,
    customer_id INT,
    gender VARCHAR(10),
    age VARCHAR(20),
    occupation INT,
    city_category VARCHAR(10),
    stay_years VARCHAR(10),
    marital_status INT
);

-- Temp table for product_master_data.csv
CREATE TABLE temp_master (
    row_index INT,
    product_id VARCHAR(20),
    product_category VARCHAR(50),
    price DECIMAL(10,2),
    store_id INT,
    supplier_id INT,
    store_name VARCHAR(100),
    supplier_name VARCHAR(100)
);

-- Temp table for transactional_data.csv
CREATE TABLE temp_transactions (
    row_index INT,
    order_id INT,
    customer_id INT,
    product_id VARCHAR(30),
    quantity INT,
    full_date DATE
);

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

CREATE TABLE DIM_CUSTOMER (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    gender VARCHAR(10),
    age VARCHAR(20),
    occupation VARCHAR(50),
    city_category VARCHAR(10),
    stay_years VARCHAR(10),
    marital_status VARCHAR(10)
);

CREATE TABLE DIM_PRODUCT (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(20),
    product_category VARCHAR(50),
    price DECIMAL(10,2)
);

CREATE TABLE DIM_STORE (
    store_key INT AUTO_INCREMENT PRIMARY KEY,
    store_id INT,
    store_name VARCHAR(100)
);

CREATE TABLE DIM_SUPPLIER (
    supplier_key INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT,
    supplier_name VARCHAR(100)
);

CREATE TABLE DIM_DATE (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    weekday_flag VARCHAR(10)
);

-- =====================================================
-- FACT TABLE
-- =====================================================

CREATE TABLE FACT_SALES (
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_key INT,
    product_key INT,
    store_key INT,
    supplier_key INT,
    date_key INT,
    quantity INT,
    revenue DECIMAL(12,2),
    FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMER(customer_key),
    FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key),
    FOREIGN KEY (store_key) REFERENCES DIM_STORE(store_key),
    FOREIGN KEY (supplier_key) REFERENCES DIM_SUPPLIER(supplier_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
);

-- =====================================================
-- LOAD DATA INTO TEMP TABLES
-- (Run load_data.py script OR use these LOAD DATA statements)
-- =====================================================

-- Option 1: Use Python script (recommended)
-- Run: python load_data.py

-- Option 2: Use LOAD DATA LOCAL INFILE (may require enabling local_infile)
/*
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/customer_master_data.csv'
INTO TABLE temp_customer
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, customer_id, gender, age, occupation, city_category, stay_years, marital_status);

LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/product_master_data.csv'
INTO TABLE temp_master
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, product_id, product_category, price, store_id, supplier_id, store_name, supplier_name);

LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/transactional_data.csv'
INTO TABLE temp_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, order_id, customer_id, product_id, quantity, full_date);
*/

-- =====================================================
-- POPULATE DIMENSION TABLES FROM TEMP TABLES
-- =====================================================

-- Populate DIM_CUSTOMER
INSERT INTO DIM_CUSTOMER (customer_id, gender, age, occupation, city_category, stay_years, marital_status)
SELECT DISTINCT customer_id, gender, age, occupation, city_category, stay_years, 
       CASE WHEN marital_status = 1 THEN 'Married' ELSE 'Single' END
FROM temp_customer;

-- Populate DIM_PRODUCT
INSERT INTO DIM_PRODUCT (product_id, product_category, price)
SELECT DISTINCT product_id, product_category, price
FROM temp_master;

-- Populate DIM_STORE
INSERT INTO DIM_STORE (store_id, store_name)
SELECT DISTINCT store_id, store_name
FROM temp_master;

-- Populate DIM_SUPPLIER
INSERT INTO DIM_SUPPLIER (supplier_id, supplier_name)
SELECT DISTINCT supplier_id, supplier_name
FROM temp_master;

-- Populate DIM_DATE
INSERT INTO DIM_DATE (full_date, day, month, quarter, year, weekday_flag)
SELECT DISTINCT 
    t.full_date,
    DAY(t.full_date),
    MONTH(t.full_date),
    QUARTER(t.full_date),
    YEAR(t.full_date),
    CASE WHEN DAYOFWEEK(t.full_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END
FROM temp_transactions t
WHERE t.full_date IS NOT NULL;

-- =====================================================
-- POPULATE FACT TABLE
-- =====================================================

INSERT INTO FACT_SALES (customer_key, product_key, store_key, supplier_key, date_key, quantity, revenue)
SELECT 
    dc.customer_key,
    dp.product_key,
    ds.store_key,
    dsu.supplier_key,
    dd.date_key,
    t.quantity,
    t.quantity * dp.price AS revenue
FROM temp_transactions t
JOIN DIM_CUSTOMER dc ON dc.customer_id = t.customer_id
JOIN DIM_PRODUCT dp ON dp.product_id = t.product_id
JOIN temp_master m ON m.product_id = t.product_id
JOIN DIM_STORE ds ON ds.store_id = m.store_id
JOIN DIM_SUPPLIER dsu ON dsu.supplier_id = m.supplier_id
JOIN DIM_DATE dd ON dd.full_date = t.full_date;

-- =====================================================
-- ANALYTICAL QUERIES
-- =====================================================

-- 1. Top 5 products by revenue with time breakdown
SELECT dd.year, dd.month, dd.weekday_flag, dp.product_id, dp.product_category, SUM(fs.revenue) AS total_revenue
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.weekday_flag, dp.product_id, dp.product_category
ORDER BY total_revenue DESC
LIMIT 5;

-- 2. Purchase amount by demographics
SELECT dc.gender, dc.age, dc.city_category,
       SUM(fs.revenue) AS purchase_amount
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
GROUP BY dc.gender, dc.age, dc.city_category;

-- 3. Sales by occupation and product category
SELECT dc.occupation, dp.product_category,
       SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
GROUP BY dc.occupation, dp.product_category;

-- 4. Quarterly purchase by gender and age (2015)
SELECT dd.quarter, dc.gender, dc.age,
       SUM(fs.revenue) AS purchase_amount
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
WHERE dd.year = 2015
GROUP BY dd.quarter, dc.gender, dc.age;

-- 5. Top 5 occupation-category combinations by sales
SELECT dc.occupation, dp.product_category, SUM(fs.revenue) AS sales
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
GROUP BY dc.occupation, dp.product_category
ORDER BY sales DESC
LIMIT 5;

-- 6. Purchase by city and marital status (2017, after July)
SELECT dd.year, dd.month, dc.city_category, dc.marital_status, SUM(fs.revenue) AS purchase_amount
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
WHERE dd.year = 2017 AND dd.month > 7
GROUP BY dc.city_category, dc.marital_status, dd.year, dd.month;

-- 7. Average purchase by stay years and gender
SELECT dc.stay_years, dc.gender, AVG(fs.revenue) AS avg_purchase_amount
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
GROUP BY dc.stay_years, dc.gender;

-- 8. Top 5 city-category combinations by purchase
SELECT dc.city_category, dp.product_category, SUM(fs.revenue) AS purchase_amount
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
GROUP BY dc.city_category, dp.product_category
ORDER BY purchase_amount DESC
LIMIT 5;

-- 9. Monthly growth by product category (2016)
WITH monthly_growth AS (
    SELECT dd.month, dp.product_category, SUM(fs.revenue) AS total_sales
    FROM FACT_SALES fs
    JOIN DIM_DATE dd ON fs.date_key = dd.date_key
    JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
    WHERE dd.year = 2016
    GROUP BY dd.month, dp.product_category
)
SELECT 
    curr.month,
    curr.product_category,
    curr.total_sales AS current_sales,
    prev.total_sales AS previous_sales,
    ROUND((curr.total_sales - prev.total_sales) * 100 / prev.total_sales, 2) AS growth_percentage
FROM monthly_growth curr
LEFT JOIN monthly_growth prev ON curr.product_category = prev.product_category 
    AND curr.month = prev.month + 1
ORDER BY curr.product_category, curr.month;

-- 10. Sales by age group and weekday/weekend
SELECT dc.age, dd.weekday_flag, SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_CUSTOMER dc ON fs.customer_key = dc.customer_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
GROUP BY dc.age, dd.weekday_flag;

-- 11. Top 5 products by revenue (2016)
SELECT dd.month, dd.weekday_flag, dp.product_id, dp.product_category, SUM(fs.revenue) AS total_revenue
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
WHERE dd.year = 2016
GROUP BY dd.month, dd.weekday_flag, dp.product_id, dp.product_category
ORDER BY total_revenue DESC
LIMIT 5;

-- 12. Store quarterly revenue (2017)
SELECT ds.store_name, dd.quarter, SUM(fs.revenue) AS revenue
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.store_key = ds.store_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
WHERE dd.year = 2017
GROUP BY ds.store_name, dd.quarter;

-- 13. Store-supplier-category breakdown
SELECT ds.store_name, dsu.supplier_name, dp.product_category, SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.store_key = ds.store_key
JOIN DIM_SUPPLIER dsu ON fs.supplier_key = dsu.supplier_key
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
GROUP BY ds.store_name, dsu.supplier_name, dp.product_category;

-- 14. Seasonal analysis by product
SELECT dp.product_id, dp.product_category, 
    CASE
        WHEN dd.month IN (3, 4, 5) THEN 'Spring'
        WHEN dd.month IN (6, 7, 8) THEN 'Summer'
        WHEN dd.month IN (9, 10, 11) THEN 'Fall'
        WHEN dd.month IN (12, 1, 2) THEN 'Winter' 
    END AS season,
    dd.quarter, SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
GROUP BY dp.product_id, dp.product_category, dd.month, dd.quarter
ORDER BY dp.product_id, season, dd.month;

-- 15. Store-supplier monthly growth
WITH monthly_sale AS (
    SELECT ds.store_name, dsu.supplier_name, dd.month, SUM(fs.revenue) AS total_sales
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.store_key = ds.store_key
    JOIN DIM_SUPPLIER dsu ON fs.supplier_key = dsu.supplier_key
    JOIN DIM_DATE dd ON fs.date_key = dd.date_key
    GROUP BY ds.store_name, dsu.supplier_name, dd.month
)
SELECT 
    curr.store_name,
    curr.supplier_name,
    curr.month,
    prev.month AS prev_month,
    curr.total_sales AS current_monthly_total_sales,
    prev.total_sales AS previous_month_total_sales,
    (curr.total_sales - prev.total_sales) AS difference,
    ROUND((curr.total_sales - prev.total_sales) * 100 / prev.total_sales, 2) AS percentage
FROM monthly_sale curr
LEFT JOIN monthly_sale prev ON curr.store_name = prev.store_name
    AND curr.supplier_name = prev.supplier_name 
    AND curr.month = prev.month + 1
ORDER BY curr.store_name, curr.supplier_name, curr.month, prev.month;

-- 16. Products frequently bought together
SELECT t1.product_id AS product_1, t2.product_id AS product_2, COUNT(*) AS product_bought_together
FROM temp_transactions t1
JOIN temp_transactions t2 ON t1.order_id = t2.order_id AND t1.product_id < t2.product_id
GROUP BY t1.product_id, t2.product_id
ORDER BY product_bought_together DESC
LIMIT 5;

-- 17. Hierarchical sales rollup (store > supplier > category)
SELECT ds.store_name, dsu.supplier_name, dp.product_category, SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.store_key = ds.store_key
JOIN DIM_SUPPLIER dsu ON fs.supplier_key = dsu.supplier_key
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
GROUP BY ds.store_name, dsu.supplier_name, dp.product_category WITH ROLLUP;

-- 18. Half-yearly revenue comparison by product
SELECT dp.product_id, dp.product_category,
    SUM(CASE WHEN dd.month BETWEEN 1 AND 6 THEN fs.revenue ELSE 0 END) AS first_half_revenue,
    SUM(CASE WHEN dd.month BETWEEN 7 AND 12 THEN fs.revenue ELSE 0 END) AS second_half_revenue,
    SUM(fs.quantity) AS total_quantity
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
GROUP BY dp.product_id, dp.product_category;

-- 19. Daily sales analysis with flag
WITH daily_sales AS (
    SELECT dp.product_id, SUM(fs.revenue) AS total_sales,
        COUNT(DISTINCT fs.date_key) AS no_of_days,
        ROUND(SUM(fs.revenue) / COUNT(DISTINCT fs.date_key), 3) AS avg_daily_sales 
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.product_key = dp.product_key
    JOIN DIM_DATE dd ON fs.date_key = dd.date_key
    GROUP BY dp.product_id
    ORDER BY dp.product_id ASC
)
SELECT
    d.product_id,
    d.total_sales,
    d.avg_daily_sales,
    2 * d.avg_daily_sales AS twice_daily_avg_sales,
    CASE WHEN d.total_sales > 2 * d.avg_daily_sales THEN 1 ELSE 0 END AS flag
FROM daily_sales d
ORDER BY d.product_id;

-- 20. Store quarterly sales view
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT ds.store_name, dd.year, dd.quarter, SUM(fs.revenue) AS total_sales
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.store_key = ds.store_key
JOIN DIM_DATE dd ON fs.date_key = dd.date_key
GROUP BY ds.store_name, dd.year, dd.quarter
ORDER BY ds.store_name;
