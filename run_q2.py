"""
Q2: Customer Demographics by Purchase Amount with City Category Breakdown
Analyzes total purchase amounts by gender and age, detailed by city category.
"""

import mysql.connector

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'project_test'
}

def run_query():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Main query: Purchase amount by gender, age, and city category
    query = """
    SELECT 
        c.Gender,
        c.Age,
        c.City_Category,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_purchase_amount,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_purchase_per_transaction
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Gender, c.Age, c.City_Category
    ORDER BY c.Gender, c.Age, c.City_Category
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 120)
    print("Q2: CUSTOMER DEMOGRAPHICS BY PURCHASE AMOUNT WITH CITY CATEGORY BREAKDOWN")
    print("=" * 120)
    
    print(f"\n{'Gender':<8} {'Age':<10} {'City':<6} {'Orders':>10} {'Qty':>10} {'Total Purchase':>18} {'Avg/Transaction':>18}")
    print(f"{'-'*8} {'-'*10} {'-'*6} {'-'*10} {'-'*10} {'-'*18} {'-'*18}")
    
    current_gender = None
    for row in results:
        gender, age, city, orders, qty, total, avg_per = row
        
        if gender != current_gender:
            current_gender = gender
            if gender == 'F':
                print(f"\n--- FEMALE ---")
            else:
                print(f"\n--- MALE ---")
        
        print(f"{gender:<8} {age:<10} {city:<6} {orders:>10,} {qty:>10,} ${total:>17,.2f} ${avg_per:>17,.2f}")
    
    # Summary by Gender
    print("\n\n" + "=" * 120)
    print("SUMMARY BY GENDER")
    print("=" * 120)
    
    gender_query = """
    SELECT 
        c.Gender,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Gender
    """
    
    cur.execute(gender_query)
    print(f"\n{'Gender':<10} {'Customers':>12} {'Orders':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*10} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    for row in cur.fetchall():
        gender, customers, orders, total, avg_trans, avg_cust = row
        g_label = "Female" if gender == 'F' else "Male"
        print(f"{g_label:<10} {customers:>12,} {orders:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Summary by Age
    print("\n\n" + "=" * 120)
    print("SUMMARY BY AGE GROUP")
    print("=" * 120)
    
    age_query = """
    SELECT 
        c.Age,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Age
    ORDER BY 
        CASE c.Age 
            WHEN '0-17' THEN 1 
            WHEN '18-25' THEN 2 
            WHEN '26-35' THEN 3 
            WHEN '36-45' THEN 4 
            WHEN '46-50' THEN 5 
            WHEN '51-55' THEN 6 
            WHEN '55+' THEN 7 
            ELSE 8 
        END
    """
    
    cur.execute(age_query)
    print(f"\n{'Age Group':<12} {'Customers':>12} {'Orders':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    for row in cur.fetchall():
        age, customers, orders, total, avg_trans, avg_cust = row
        print(f"{age:<12} {customers:>12,} {orders:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Summary by City Category
    print("\n\n" + "=" * 120)
    print("SUMMARY BY CITY CATEGORY")
    print("=" * 120)
    
    city_query = """
    SELECT 
        c.City_Category,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.City_Category
    ORDER BY c.City_Category
    """
    
    cur.execute(city_query)
    print(f"\n{'City':<10} {'Customers':>12} {'Orders':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*10} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    for row in cur.fetchall():
        city, customers, orders, total, avg_trans, avg_cust = row
        print(f"City {city:<5} {customers:>12,} {orders:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Cross-tab: Gender x City
    print("\n\n" + "=" * 120)
    print("GENDER x CITY CATEGORY CROSS-TAB")
    print("=" * 120)
    
    cross_query = """
    SELECT 
        c.Gender,
        c.City_Category,
        SUM(t.quantity * p.price) AS total_purchase
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Gender, c.City_Category
    ORDER BY c.Gender, c.City_Category
    """
    
    cur.execute(cross_query)
    cross_results = cur.fetchall()
    
    # Build pivot table
    data = {}
    for gender, city, total in cross_results:
        if gender not in data:
            data[gender] = {}
        data[gender][city] = total
    
    print(f"\n{'Gender':<10} {'City A':>18} {'City B':>18} {'City C':>18} {'Total':>18}")
    print(f"{'-'*10} {'-'*18} {'-'*18} {'-'*18} {'-'*18}")
    
    for gender in ['F', 'M']:
        g_label = "Female" if gender == 'F' else "Male"
        city_a = data.get(gender, {}).get('A', 0)
        city_b = data.get(gender, {}).get('B', 0)
        city_c = data.get(gender, {}).get('C', 0)
        total = city_a + city_b + city_c
        print(f"{g_label:<10} ${city_a:>17,.2f} ${city_b:>17,.2f} ${city_c:>17,.2f} ${total:>17,.2f}")
    
    print("\n" + "=" * 120)
    conn.close()

if __name__ == "__main__":
    run_query()
