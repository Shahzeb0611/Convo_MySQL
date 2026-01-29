"""
Q9: Monthly Sales Growth by Product Category
Measures month-over-month sales growth percentage for each product category in the current year.
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
    
    # Main query: Month-over-month sales growth by product category
    query = """
    WITH MonthlySales AS (
        SELECT 
            p.Product_Category,
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            DATE_FORMAT(t.order_date, '%%Y-%%m') AS month_year,
            SUM(t.quantity * p.price) AS monthly_revenue,
            COUNT(DISTINCT t.orderID) AS total_orders
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY p.Product_Category, YEAR(t.order_date), MONTH(t.order_date), DATE_FORMAT(t.order_date, '%%Y-%%m')
    ),
    SalesWithGrowth AS (
        SELECT 
            Product_Category,
            year,
            month,
            month_year,
            monthly_revenue,
            total_orders,
            LAG(monthly_revenue) OVER (PARTITION BY Product_Category ORDER BY year, month) AS prev_month_revenue,
            LAG(month_year) OVER (PARTITION BY Product_Category ORDER BY year, month) AS prev_month
        FROM MonthlySales
    )
    SELECT 
        Product_Category,
        month_year,
        monthly_revenue,
        total_orders,
        prev_month_revenue,
        CASE 
            WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
            ELSE ROUND((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100, 2)
        END AS growth_pct
    FROM SalesWithGrowth
    ORDER BY Product_Category, year, month
    """
    
    cur.execute(query, (current_year,))
    results = cur.fetchall()
    
    print("=" * 130)
    print(f"Q9: MONTHLY SALES GROWTH BY PRODUCT CATEGORY ({current_year})")
    print("=" * 130)
    
    current_category = None
    
    for row in results:
        category, month_year, revenue, orders, prev_revenue, growth = row
        
        if category != current_category:
            current_category = category
            print(f"\n{'='*130}")
            print(f"PRODUCT CATEGORY: {category}")
            print(f"{'='*130}")
            print(f"{'Month':<12} {'Revenue':>18} {'Orders':>12} {'Prev Revenue':>18} {'Growth %':>15} {'Trend':>12}")
            print(f"{'-'*12} {'-'*18} {'-'*12} {'-'*18} {'-'*15} {'-'*12}")
        
        prev_str = f"${prev_revenue:>17,.2f}" if prev_revenue else "N/A".rjust(18)
        
        if growth is None:
            growth_str = "N/A".rjust(15)
            trend = "-"
        elif growth > 0:
            growth_str = f"+{growth:>13.2f}%"
            trend = "📈 UP" if growth > 10 else "↑ up"
        elif growth < 0:
            growth_str = f"{growth:>14.2f}%"
            trend = "📉 DOWN" if growth < -10 else "↓ down"
        else:
            growth_str = f"{growth:>14.2f}%"
            trend = "→ flat"
        
        print(f"{month_year:<12} ${revenue:>17,.2f} {orders:>12,} {prev_str} {growth_str} {trend:>12}")
    
    # Summary: Average growth by category
    print("\n\n" + "=" * 130)
    print("SUMMARY: AVERAGE MONTHLY GROWTH BY CATEGORY")
    print("=" * 130)
    
    summary_query = """
    WITH MonthlySales AS (
        SELECT 
            p.Product_Category,
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            SUM(t.quantity * p.price) AS monthly_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY p.Product_Category, YEAR(t.order_date), MONTH(t.order_date)
    ),
    SalesWithGrowth AS (
        SELECT 
            Product_Category,
            monthly_revenue,
            LAG(monthly_revenue) OVER (PARTITION BY Product_Category ORDER BY year, month) AS prev_month_revenue
        FROM MonthlySales
    )
    SELECT 
        Product_Category,
        COUNT(*) AS months_analyzed,
        ROUND(AVG(CASE 
            WHEN prev_month_revenue IS NOT NULL AND prev_month_revenue > 0 
            THEN (monthly_revenue - prev_month_revenue) / prev_month_revenue * 100 
        END), 2) AS avg_growth_pct,
        SUM(monthly_revenue) AS total_revenue
    FROM SalesWithGrowth
    GROUP BY Product_Category
    ORDER BY avg_growth_pct DESC
    """
    
    cur.execute(summary_query, (current_year,))
    print(f"\n{'Category':<20} {'Months':>10} {'Avg Growth %':>15} {'Total Revenue':>20} {'Performance':>15}")
    print(f"{'-'*20} {'-'*10} {'-'*15} {'-'*20} {'-'*15}")
    
    for row in cur.fetchall():
        category, months, avg_growth, total = row
        if avg_growth is None:
            growth_str = "N/A"
            perf = "-"
        elif avg_growth > 5:
            growth_str = f"+{avg_growth:.2f}%"
            perf = "Strong Growth"
        elif avg_growth > 0:
            growth_str = f"+{avg_growth:.2f}%"
            perf = "Moderate Growth"
        elif avg_growth > -5:
            growth_str = f"{avg_growth:.2f}%"
            perf = "Slight Decline"
        else:
            growth_str = f"{avg_growth:.2f}%"
            perf = "Declining"
        
        print(f"{category:<20} {months:>10} {growth_str:>15} ${total:>19,.2f} {perf:>15}")
    
    print("\n" + "=" * 130)
    conn.close()

if __name__ == "__main__":
    run_query()
