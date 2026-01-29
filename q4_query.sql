-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend (2020)

-- Main detailed breakdown
SELECT 
    c.Gender,
    c.Age,
    QUARTER(t.order_date) AS quarter,
    COUNT(DISTINCT c.Customer_ID) AS unique_customers,
    COUNT(DISTINCT t.orderID) AS total_orders,
    SUM(t.quantity) AS total_quantity,
    ROUND(SUM(t.quantity * p.price), 2) AS total_purchase,
    ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction
FROM transactional_data t
JOIN customer_master c ON t.Customer_ID = c.Customer_ID
JOIN product_master p ON t.Product_ID = p.Product_ID
WHERE YEAR(t.order_date) = 2020
GROUP BY c.Gender, c.Age, QUARTER(t.order_date)
ORDER BY c.Gender, 
    CASE c.Age 
        WHEN '0-17' THEN 1 
        WHEN '18-25' THEN 2 
        WHEN '26-35' THEN 3 
        WHEN '36-45' THEN 4 
        WHEN '46-50' THEN 5 
        WHEN '51-55' THEN 6 
        WHEN '55+' THEN 7 
    END,
    quarter;
