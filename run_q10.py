"""
Q10: Weekend vs. Weekday Sales by Age Group
Compares total sales by age group for weekends versus weekdays in the current year.
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
    
    # Get the most recent year in data
    cur.execute("SELECT MAX(YEAR(order_date)) FROM transactional_data")
    current_year = cur.fetchone()[0]
    print(f"Analyzing year: {current_year}\n")
    
    # Main query: Weekend vs Weekday sales by age group
    query = """
    SELECT 
        c.Age,
        CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY c.Age, CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END
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
        END,
        day_type DESC
    """
    
    cur.execute(query, (current_year,))
    results = cur.fetchall()
    
    print("=" * 140)
    print(f"Q10: WEEKEND VS WEEKDAY SALES BY AGE GROUP ({current_year})")
    print("=" * 140)
    
    print(f"\n{'Age Group':<12} {'Day Type':<12} {'Customers':>12} {'Orders':>12} {'Quantity':>12} {'Total Sales':>18} {'Avg/Trans':>15}")
    print(f"{'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*15}")
    
    current_age = None
    for row in results:
        age, day_type, customers, orders, qty, sales, avg_trans = row
        
        if age != current_age:
            if current_age is not None:
                print()  # Blank line between age groups
            current_age = age
        
        print(f"{age:<12} {day_type:<12} {customers:>12,} {orders:>12,} {qty:>12,} ${sales:>17,.2f} ${avg_trans:>14,.2f}")
    
    # Cross-tab comparison
    print("\n\n" + "=" * 140)
    print("CROSS-TAB: WEEKEND VS WEEKDAY COMPARISON BY AGE GROUP")
    print("=" * 140)
    
    crosstab_query = """
    SELECT 
        c.Age,
        SUM(CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN t.quantity * p.price ELSE 0 END) AS weekend_sales,
        SUM(CASE WHEN DAYOFWEEK(t.order_date) NOT IN (1, 7) THEN t.quantity * p.price ELSE 0 END) AS weekday_sales,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
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
    
    cur.execute(crosstab_query, (current_year,))
    
    print(f"\n{'Age Group':<12} {'Weekend Sales':>20} {'Weekday Sales':>20} {'Total':>20} {'Weekend %':>12} {'Weekday %':>12} {'Higher':>12}")
    print(f"{'-'*12} {'-'*20} {'-'*20} {'-'*20} {'-'*12} {'-'*12} {'-'*12}")
    
    grand_weekend = 0
    grand_weekday = 0
    
    for row in cur.fetchall():
        age, weekend, weekday, total = row
        weekend = float(weekend)
        weekday = float(weekday)
        total = float(total)
        
        weekend_pct = weekend / total * 100 if total > 0 else 0
        weekday_pct = weekday / total * 100 if total > 0 else 0
        higher = "Weekend" if weekend > weekday else "Weekday" if weekday > weekend else "Equal"
        
        grand_weekend += weekend
        grand_weekday += weekday
        
        print(f"{age:<12} ${weekend:>19,.2f} ${weekday:>19,.2f} ${total:>19,.2f} {weekend_pct:>11.1f}% {weekday_pct:>11.1f}% {higher:>12}")
    
    grand_total = grand_weekend + grand_weekday
    print(f"{'-'*12} {'-'*20} {'-'*20} {'-'*20} {'-'*12} {'-'*12}")
    print(f"{'TOTAL':<12} ${grand_weekend:>19,.2f} ${grand_weekday:>19,.2f} ${grand_total:>19,.2f} {(grand_weekend/grand_total*100):>11.1f}% {(grand_weekday/grand_total*100):>11.1f}%")
    
    # Insights
    print("\n\n" + "=" * 140)
    print("INSIGHTS: SHOPPING PATTERNS BY AGE GROUP")
    print("=" * 140)
    
    insight_query = """
    WITH AgeDayStats AS (
        SELECT 
            c.Age,
            CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            SUM(t.quantity * p.price) AS total_sales
        FROM transactional_data t
        JOIN customer_master c ON t.Customer_ID = c.Customer_ID
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY c.Age, CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END
    )
    SELECT Age, day_type, total_sales
    FROM AgeDayStats
    ORDER BY total_sales DESC
    LIMIT 3
    """
    
    cur.execute(insight_query, (current_year,))
    print("\nTop 3 Age Group + Day Type Combinations by Sales:")
    for i, (age, day_type, sales) in enumerate(cur.fetchall(), 1):
        print(f"  {i}. {age} on {day_type}s: ${sales:,.2f}")
    
    print("\n" + "=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
