"""
Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
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
    
    query = """
    WITH ProductRevenue AS (
        SELECT 
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            p.Product_ID,
            p.Product_Category,
            SUM(t.quantity * p.price) AS total_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY YEAR(t.order_date), MONTH(t.order_date), 
                 CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END,
                 p.Product_ID, p.Product_Category
    ),
    RankedProducts AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY year, month, day_type ORDER BY total_revenue DESC) AS rnk
        FROM ProductRevenue
    )
    SELECT year, month, day_type, Product_ID, Product_Category, total_revenue, rnk
    FROM RankedProducts
    WHERE rnk <= 5
    ORDER BY year, month, day_type DESC, rnk
    """
    
    cur.execute(query, (target_year,))
    results = cur.fetchall()
    
    print("=" * 110)
    print(f"Q1: TOP 5 REVENUE-GENERATING PRODUCTS - WEEKDAYS vs WEEKENDS (Year {target_year})")
    print("=" * 110)
    
    current_month = None
    current_type = None
    
    for row in results:
        yr, mth, day_type, prod_id, category, revenue, rnk = row
        
        if mth != current_month:
            current_month = mth
            print(f"\n--- MONTH {mth} ---")
            current_type = None
        
        if day_type != current_type:
            current_type = day_type
            print(f"\n  [{day_type.upper()}]")
            print(f"  {'Rank':<6} {'Product ID':<15} {'Category':<25} {'Revenue':>15}")
            print(f"  {'-'*6} {'-'*15} {'-'*25} {'-'*15}")
        
        print(f"  {rnk:<6} {prod_id:<15} {str(category):<25} ${revenue:>14,.2f}")
    
    # Yearly summary
    summary_query = """
    WITH ProductRevenue AS (
        SELECT 
            CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            p.Product_ID,
            p.Product_Category,
            SUM(t.quantity * p.price) AS total_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY CASE WHEN DAYOFWEEK(t.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END,
                 p.Product_ID, p.Product_Category
    ),
    RankedProducts AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY day_type ORDER BY total_revenue DESC) AS rnk
        FROM ProductRevenue
    )
    SELECT day_type, Product_ID, Product_Category, total_revenue, rnk
    FROM RankedProducts WHERE rnk <= 5
    ORDER BY day_type DESC, rnk
    """
    
    cur.execute(summary_query, (target_year,))
    summary = cur.fetchall()
    
    print("\n\n" + "=" * 110)
    print(f"YEARLY SUMMARY: TOP 5 PRODUCTS BY DAY TYPE ({target_year})")
    print("=" * 110)
    
    current_type = None
    for row in summary:
        day_type, prod_id, category, revenue, rnk = row
        if day_type != current_type:
            current_type = day_type
            print(f"\n[{day_type.upper()}]")
            print(f"{'Rank':<6} {'Product ID':<15} {'Category':<25} {'Total Revenue':>18}")
            print(f"{'-'*6} {'-'*15} {'-'*25} {'-'*18}")
        print(f"{rnk:<6} {prod_id:<15} {str(category):<25} ${revenue:>17,.2f}")
    
    print("\n" + "=" * 110)
    conn.close()

if __name__ == "__main__":
    run_query()
