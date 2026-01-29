"""
Q7: Average Purchase Amount by Stay Duration and Gender
Calculates the average purchase amount based on years stayed in the city and gender.
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
    
    # Main query: Average purchase by stay duration and gender
    query = """
    SELECT 
        c.Stay_In_Current_City_Years AS stay_duration,
        c.Gender,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Stay_In_Current_City_Years, c.Gender
    ORDER BY 
        CASE c.Stay_In_Current_City_Years 
            WHEN '0' THEN 1 
            WHEN '1' THEN 2 
            WHEN '2' THEN 3 
            WHEN '3' THEN 4 
            WHEN '4+' THEN 5 
            ELSE 6 
        END,
        c.Gender
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 140)
    print("Q7: AVERAGE PURCHASE AMOUNT BY STAY DURATION AND GENDER")
    print("=" * 140)
    
    print(f"\n{'Stay Duration':<15} {'Gender':<10} {'Customers':>12} {'Orders':>12} {'Quantity':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*15} {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    
    current_stay = None
    for row in results:
        stay, gender, customers, orders, qty, total, avg_trans, avg_cust = row
        
        if stay != current_stay:
            current_stay = stay
            if stay != results[0][0]:  # Not first group
                print()  # Add blank line between groups
        
        gender_label = "Female" if gender == 'F' else "Male"
        stay_label = f"{stay} year(s)" if stay != '4+' else "4+ years"
        print(f"{stay_label:<15} {gender_label:<10} {customers:>12,} {orders:>12,} {qty:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Summary by Stay Duration only
    print("\n\n" + "=" * 140)
    print("SUMMARY BY STAY DURATION (ALL GENDERS)")
    print("=" * 140)
    
    stay_query = """
    SELECT 
        c.Stay_In_Current_City_Years AS stay_duration,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Stay_In_Current_City_Years
    ORDER BY 
        CASE c.Stay_In_Current_City_Years 
            WHEN '0' THEN 1 
            WHEN '1' THEN 2 
            WHEN '2' THEN 3 
            WHEN '3' THEN 4 
            WHEN '4+' THEN 5 
            ELSE 6 
        END
    """
    
    cur.execute(stay_query)
    print(f"\n{'Stay Duration':<15} {'Customers':>12} {'Orders':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*15} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    
    for row in cur.fetchall():
        stay, customers, orders, total, avg_trans, avg_cust = row
        stay_label = f"{stay} year(s)" if stay != '4+' else "4+ years"
        print(f"{stay_label:<15} {customers:>12,} {orders:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Summary by Gender only
    print("\n\n" + "=" * 140)
    print("SUMMARY BY GENDER (ALL STAY DURATIONS)")
    print("=" * 140)
    
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
    print(f"\n{'Gender':<12} {'Customers':>12} {'Orders':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    
    for row in cur.fetchall():
        gender, customers, orders, total, avg_trans, avg_cust = row
        gender_label = "Female" if gender == 'F' else "Male"
        print(f"{gender_label:<12} {customers:>12,} {orders:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Cross-tab: Stay Duration vs Gender (Average per Customer)
    print("\n\n" + "=" * 140)
    print("CROSS-TAB: STAY DURATION vs GENDER (AVERAGE PURCHASE PER CUSTOMER)")
    print("=" * 140)
    
    crosstab_query = """
    SELECT 
        c.Stay_In_Current_City_Years AS stay_duration,
        c.Gender,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Stay_In_Current_City_Years, c.Gender
    ORDER BY 
        CASE c.Stay_In_Current_City_Years 
            WHEN '0' THEN 1 
            WHEN '1' THEN 2 
            WHEN '2' THEN 3 
            WHEN '3' THEN 4 
            WHEN '4+' THEN 5 
            ELSE 6 
        END,
        c.Gender
    """
    
    cur.execute(crosstab_query)
    cross_results = cur.fetchall()
    
    # Build pivot table
    data = {}
    for stay, gender, avg_cust in cross_results:
        if stay not in data:
            data[stay] = {}
        data[stay][gender] = avg_cust
    
    # Order for display
    stay_order = ['0', '1', '2', '3', '4+']
    
    print(f"\n{'Stay Duration':<15} {'Female':>18} {'Male':>18} {'Difference':>18} {'Higher Spender':>18}")
    print(f"{'-'*15} {'-'*18} {'-'*18} {'-'*18} {'-'*18}")
    
    for stay in stay_order:
        if stay in data:
            female = float(data[stay].get('F', 0))
            male = float(data[stay].get('M', 0))
            diff = abs(male - female)
            higher = "Male" if male > female else "Female" if female > male else "Equal"
            stay_label = f"{stay} year(s)" if stay != '4+' else "4+ years"
            print(f"{stay_label:<15} ${female:>17,.2f} ${male:>17,.2f} ${diff:>17,.2f} {higher:>18}")
    
    # Insight: Who spends more on average?
    print("\n\n" + "=" * 140)
    print("INSIGHTS: SPENDING PATTERNS")
    print("=" * 140)
    
    insight_query = """
    SELECT 
        c.Stay_In_Current_City_Years AS stay_duration,
        COUNT(DISTINCT c.Customer_ID) AS customers,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Stay_In_Current_City_Years
    ORDER BY avg_per_customer DESC
    LIMIT 1
    """
    
    cur.execute(insight_query)
    top_stay = cur.fetchone()
    print(f"\n• Highest avg spending by stay duration: {top_stay[0]} year(s) - ${top_stay[2]:,.2f} per customer")
    
    insight_query2 = """
    SELECT 
        c.Gender,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Gender
    ORDER BY avg_per_customer DESC
    LIMIT 1
    """
    
    cur.execute(insight_query2)
    top_gender = cur.fetchone()
    gender_label = "Female" if top_gender[0] == 'F' else "Male"
    print(f"• Highest avg spending by gender: {gender_label} - ${top_gender[1]:,.2f} per customer")
    
    print("\n" + "=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
