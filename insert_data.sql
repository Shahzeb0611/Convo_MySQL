-- =====================================================
-- INSERT DATA FROM CSV FILES INTO TABLES
-- =====================================================
-- Using LOAD DATA LOCAL INFILE to avoid --secure-file-priv restriction
-- =====================================================

-- IMPORTANT: You may need to enable local_infile first
-- Run this command before loading data:
SET GLOBAL local_infile = 1;

-- =====================================================
-- INSERT INTO CUSTOMER MASTER TABLE
-- =====================================================
LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/customer_master_data.csv'
INTO TABLE customer_master
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status);

-- =====================================================
-- INSERT INTO PRODUCT MASTER TABLE
-- =====================================================
LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/product_master_data.csv'
INTO TABLE product_master
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, Product_ID, Product_Category, price, storeID, supplierID, storeName, supplierName);

-- =====================================================
-- INSERT INTO TRANSACTIONAL DATA TABLE
-- =====================================================
LOAD DATA LOCAL INFILE 'E:/CONVO/Misc/Curso_MySQL/29-1-26/data/transactional_data.csv'
INTO TABLE transactional_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(row_index, orderID, Customer_ID, Product_ID, quantity, order_date);

-- =====================================================
-- VERIFY DATA LOADED CORRECTLY
-- =====================================================
SELECT 'customer_master' AS table_name, COUNT(*) AS row_count FROM customer_master
UNION ALL
SELECT 'product_master' AS table_name, COUNT(*) AS row_count FROM product_master
UNION ALL
SELECT 'transactional_data' AS table_name, COUNT(*) AS row_count FROM transactional_data;
