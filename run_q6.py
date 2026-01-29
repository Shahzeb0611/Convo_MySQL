"""
Q6: City Category Performance by Marital Status with Monthly Breakdown
Assesses purchase amounts by city category and marital status over the past 6 months.
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
    
    # First, get the date range in the data
    cur.execute("SELECT MIN(order_date), MAX(order_date) FROM transactional_data")
    min_date, max_date = cur.fetchone()
    print(f"Data range: {min_date} to {max_date}")
    print(f"Analyzing last 6 months from the most recent date in data...\n")
    
    # Main query: Purchase amounts by city category and marital status for last 6 months
    query = """
    WITH DateRange AS (
        SELECT 
            MAX(order_date) AS max_date,
            DATE_SUB(MAX(order_date), INTERVAL 6 MONTH) AS start_date
        FROM transactional_data
    ),
    MonthlyData AS (
        SELECT 
            c.City_Category,
            CASE WHEN c.Marital_Status = 0 THEN 'Single' ELSE 'Married' END AS marital_status,
            DATE_FORMAT(t.order_date, '%Y-%m') AS month_year,
            MONTH(t.order_date) AS month_num,
            YEAR(t.order_date) AS year_num,
            COUNT(DISTINCT t.orderID) AS total_orders,
            COUNT(DISTINCT c.Customer_ID) AS unique_customers,
            SUM(t.quantity) AS total_quantity,
            SUM(t.quantity * p.price) AS total_purchase
        FROM transactional_data t
        JOIN customer_master c ON t.Customer_ID = c.Customer_ID
        JOIN product_master p ON t.Product_ID = p.Product_ID
        CROSS JOIN DateRange d
        WHERE t.order_date > d.start_date
        GROUP BY c.City_Category, c.Marital_Status, DATE_FORMAT(t.order_date, '%Y-%m'),
                 MONTH(t.order_date), YEAR(t.order_date)
    )
    SELECT 
        City_Category,
        marital_status,
        month_year,
        total_orders,
        unique_customers,
        total_quantity,
        total_purchase
    FROM MonthlyData
    ORDER BY City_Category, marital_status, year_num, month_num
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 140)
    print("Q6: CITY CATEGORY PERFORMANCE BY MARITAL STATUS - MONTHLY BREAKDOWN (LAST 6 MONTHS)")
    print("=" * 140)
    
    current_city = None
    current_marital = None
    
    for row in results:
        city, marital, month, orders, customers, qty, purchase = row
        
        if city != current_city:
            current_city = city
            current_marital = None
            print(f"\n{'='*140}")
            print(f"CITY CATEGORY: {city}")
            print(f"{'='*140}")
        
        if marital != current_marital:
            current_marital = marital
            print(f"\n  --- {marital.upper()} ---")
            print(f"  {'Month':<12} {'Orders':>12} {'Customers':>12} {'Quantity':>12} {'Total Purchase':>20}")
            print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*20}")
        
        print(f"  {month:<12} {orders:>12,} {customers:>12,} {qty:>12,} ${purchase:>19,.2f}")
    
    # Summary by City and Marital Status (totals)
    print("\n\n" + "=" * 140)
    print("SUMMARY: TOTAL BY CITY CATEGORY AND MARITAL STATUS (LAST 6 MONTHS)")
    print("=" * 140)
    
    summary_query = """
    WITH DateRange AS (
        SELECT 
            MAX(order_date) AS max_date,
            DATE_SUB(MAX(order_date), INTERVAL 6 MONTH) AS start_date
        FROM transactional_data
    )
    SELECT 
        c.City_Category,
        CASE WHEN c.Marital_Status = 0 THEN 'Single' ELSE 'Married' END AS marital_status,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_purchase,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    CROSS JOIN DateRange d
    WHERE t.order_date > d.start_date
    GROUP BY c.City_Category, c.Marital_Status
    ORDER BY c.City_Category, c.Marital_Status
    """
    
    cur.execute(summary_query)
    print(f"\n{'City':<8} {'Status':<10} {'Customers':>12} {'Orders':>12} {'Quantity':>12} {'Total Purchase':>18} {'Avg/Trans':>14} {'Avg/Cust':>14}")
    print(f"{'-'*8} {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*14}")
    
    for row in cur.fetchall():
        city, marital, customers, orders, qty, total, avg_trans, avg_cust = row
        print(f"{city:<8} {marital:<10} {customers:>12,} {orders:>12,} {qty:>12,} ${total:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>13,.2f}")
    
    # Cross-tab: City vs Marital Status
    print("\n\n" + "=" * 140)
    print("CROSS-TAB: CITY CATEGORY vs MARITAL STATUS (TOTAL PURCHASE)")
    print("=" * 140)
    
    crosstab_query = """
    WITH DateRange AS (
        SELECT DATE_SUB(MAX(order_date), INTERVAL 6 MONTH) AS start_date
        FROM transactional_data
    )
    SELECT 
        c.City_Category,
        CASE WHEN c.Marital_Status = 0 THEN 'Single' ELSE 'Married' END AS marital_status,
        SUM(t.quantity * p.price) AS total_purchase
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    CROSS JOIN DateRange d
    WHERE t.order_date > d.start_date
    GROUP BY c.City_Category, c.Marital_Status
    ORDER BY c.City_Category, c.Marital_Status
    """
    
    cur.execute(crosstab_query)
    cross_results = cur.fetchall()
    
    # Build pivot table
    data = {}
    for city, marital, total in cross_results:
        if city not in data:
            data[city] = {}
        data[city][marital] = total
    
    print(f"\n{'City':<10} {'Single':>20} {'Married':>20} {'Total':>20} {'% Single':>12} {'% Married':>12}")
    print(f"{'-'*10} {'-'*20} {'-'*20} {'-'*20} {'-'*12} {'-'*12}")
    
    grand_single = 0
    grand_married = 0
    
    for city in sorted(data.keys()):
        single = float(data[city].get('Single', 0))
        married = float(data[city].get('Married', 0))
        total = single + married
        pct_single = (single / total * 100) if total > 0 else 0
        pct_married = (married / total * 100) if total > 0 else 0
        grand_single += single
        grand_married += married
        print(f"City {city:<5} ${single:>19,.2f} ${married:>19,.2f} ${total:>19,.2f} {pct_single:>11.1f}% {pct_married:>11.1f}%")
    
    grand_total = grand_single + grand_married
    print(f"{'-'*10} {'-'*20} {'-'*20} {'-'*20} {'-'*12} {'-'*12}")
    print(f"{'TOTAL':<10} ${grand_single:>19,.2f} ${grand_married:>19,.2f} ${grand_total:>19,.2f} {(grand_single/grand_total*100):>11.1f}% {(grand_married/grand_total*100):>11.1f}%")
    
    print("\n" + "=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
