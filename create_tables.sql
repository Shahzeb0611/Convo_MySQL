-- =====================================================
-- CREATE TABLES FOR CUSTOMER, PRODUCT, AND TRANSACTIONS
-- =====================================================

-- Drop tables if they exist (in correct order due to foreign key constraints)
DROP TABLE IF EXISTS transactional_data;
DROP TABLE IF EXISTS product_master;
DROP TABLE IF EXISTS customer_master;

-- =====================================================
-- TABLE 1: CUSTOMER MASTER DATA
-- =====================================================
CREATE TABLE customer_master (
    row_index INT,
    Customer_ID INT PRIMARY KEY,
    Gender CHAR(1),
    Age VARCHAR(10),
    Occupation INT,
    City_Category CHAR(1),
    Stay_In_Current_City_Years VARCHAR(5),
    Marital_Status TINYINT
);

-- =====================================================
-- TABLE 2: PRODUCT MASTER DATA
-- =====================================================
CREATE TABLE product_master (
    row_index INT,
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category VARCHAR(50),
    price DECIMAL(10, 2),
    storeID INT,
    supplierID INT,
    storeName VARCHAR(100),
    supplierName VARCHAR(100)
);

-- =====================================================
-- TABLE 3: TRANSACTIONAL DATA
-- =====================================================
CREATE TABLE transactional_data (
    row_index INT,
    orderID INT PRIMARY KEY,
    Customer_ID INT,
    Product_ID VARCHAR(20),
    quantity INT,
    order_date DATE,
    FOREIGN KEY (Customer_ID) REFERENCES customer_master(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES product_master(Product_ID)
);
