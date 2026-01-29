"""
Q11: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
Find the top 5 products that generated the highest revenue, separated by weekday and weekend
sales, with results grouped by month for a specified year.
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
    
    # Get available years
    cur.execute("SELECT DISTINCT YEAR(order_date) FROM transactional_data ORDER BY 1")
    years = [r[0] for r in cur.fetchall()]
    print(f"Available years: {years}")
    target_year = years[-1] if years else 2016
    print(f"Analyzing year: {target_year}\n")
    
    # Main query: Top 5 products by revenue for weekday/weekend with monthly drill-down
    query = """
    WITH ProductRevenue AS (
        SELECT 
            MONTH(t.order_date) AS month,
            DATE_FORMAT(t.order_date, '%%Y-%%m') AS month_year,
            CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            p.Product_ID,
            p.Product_Category,
            SUM(t.quantity * p.price) AS total_revenue,
            SUM(t.quantity) AS total_quantity,
            COUNT(DISTINCT t.orderID) AS order_count
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY MONTH(t.order_date), DATE_FORMAT(t.order_date, '%%Y-%%m'), 
                 CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END,
                 p.Product_ID, p.Product_Category
    ),
    RankedProducts AS (
        SELECT 
            month,
            month_year,
            day_type,
            Product_ID,
            Product_Category,
            total_revenue,
            total_quantity,
            order_count,
            ROW_NUMBER() OVER (PARTITION BY month, day_type ORDER BY total_revenue DESC) AS revenue_rank
        FROM ProductRevenue
    )
    SELECT 
        month_year,
        day_type,
        revenue_rank,
        Product_ID,
        Product_Category,
        total_revenue,
        total_quantity,
        order_count
    FROM RankedProducts
    WHERE revenue_rank <= 5
    ORDER BY month, day_type DESC, revenue_rank
    """
    
    cur.execute(query, (target_year,))
    results = cur.fetchall()
    
    print("=" * 150)
    print(f"Q11: TOP 5 REVENUE-GENERATING PRODUCTS - WEEKDAY VS WEEKEND WITH MONTHLY DRILL-DOWN ({target_year})")
    print("=" * 150)
    
    current_month = None
    current_day_type = None
    
    for row in results:
        month_year, day_type, rank, product_id, category, revenue, qty, orders = row
        
        if month_year != current_month:
            current_month = month_year
            current_day_type = None
            print(f"\n{'='*150}")
            print(f"MONTH: {month_year}")
            print(f"{'='*150}")
        
        if day_type != current_day_type:
            current_day_type = day_type
            print(f"\n  --- {day_type.upper()} ---")
            print(f"  {'Rank':<6} {'Product ID':<15} {'Category':<15} {'Orders':>10} {'Quantity':>10} {'Revenue':>18}")
            print(f"  {'-'*6} {'-'*15} {'-'*15} {'-'*10} {'-'*10} {'-'*18}")
        
        print(f"  #{rank:<5} {product_id:<15} {category:<15} {orders:>10,} {qty:>10,} ${revenue:>17,.2f}")
    
    # Summary: Overall top products by day type
    print("\n\n" + "=" * 150)
    print(f"SUMMARY: OVERALL TOP 10 PRODUCTS BY DAY TYPE ({target_year})")
    print("=" * 150)
    
    summary_query = """
    WITH DayTypeRevenue AS (
        SELECT 
            CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            p.Product_ID,
            p.Product_Category,
            SUM(t.quantity * p.price) AS total_revenue,
            COUNT(DISTINCT t.orderID) AS total_orders
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END,
                 p.Product_ID, p.Product_Category
    ),
    RankedByDayType AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY day_type ORDER BY total_revenue DESC) AS rn
        FROM DayTypeRevenue
    )
    SELECT day_type, Product_ID, Product_Category, total_revenue, total_orders
    FROM RankedByDayType
    WHERE rn <= 10
    ORDER BY day_type DESC, rn
    """
    
    cur.execute(summary_query, (target_year,))
    
    current_day_type = None
    for row in cur.fetchall():
        day_type, product_id, category, revenue, orders = row
        
        if day_type != current_day_type:
            current_day_type = day_type
            print(f"\n--- TOP 10 {day_type.upper()} PRODUCTS ---")
            print(f"{'Product ID':<15} {'Category':<20} {'Orders':>12} {'Total Revenue':>20}")
            print(f"{'-'*15} {'-'*20} {'-'*12} {'-'*20}")
        
        print(f"{product_id:<15} {category:<20} {orders:>12,} ${revenue:>19,.2f}")
    
    # Comparison: Products that perform differently on weekdays vs weekends
    print("\n\n" + "=" * 150)
    print("INSIGHT: PRODUCTS WITH LARGEST WEEKDAY vs WEEKEND REVENUE DIFFERENCE")
    print("=" * 150)
    
    diff_query = """
    SELECT 
        p.Product_ID,
        p.Product_Category,
        SUM(CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN t.quantity * p.price ELSE 0 END) AS weekend_revenue,
        SUM(CASE WHEN DAYOFWEEK(t.order_date) NOT IN (1, 7) THEN t.quantity * p.price ELSE 0 END) AS weekday_revenue,
        SUM(t.quantity * p.price) AS total_revenue
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.Product_ID, p.Product_Category
    HAVING weekend_revenue > 0 AND weekday_revenue > 0
    ORDER BY ABS(weekend_revenue - weekday_revenue) DESC
    LIMIT 10
    """
    
    cur.execute(diff_query, (target_year,))
    
    print(f"\n{'Product ID':<15} {'Category':<15} {'Weekend':>18} {'Weekday':>18} {'Difference':>18} {'Bias':>12}")
    print(f"{'-'*15} {'-'*15} {'-'*18} {'-'*18} {'-'*18} {'-'*12}")
    
    for row in cur.fetchall():
        product_id, category, weekend, weekday, total = row
        diff = float(weekend) - float(weekday)
        bias = "Weekend" if diff > 0 else "Weekday"
        print(f"{product_id:<15} {category:<15} ${float(weekend):>17,.2f} ${float(weekday):>17,.2f} ${abs(diff):>17,.2f} {bias:>12}")
    
    print("\n" + "=" * 150)
    conn.close()

if __name__ == "__main__":
    run_query()
