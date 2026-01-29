"""
Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
Calculate the revenue growth rate for each store on a quarterly basis for 2017.
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
    
    # Check available years
    cur.execute("SELECT DISTINCT YEAR(order_date) FROM transactional_data ORDER BY 1")
    years = [r[0] for r in cur.fetchall()]
    print(f"Available years in data: {years}")
    
    # Use 2017 if available, otherwise use the latest year
    target_year = 2017 if 2017 in years else years[-1]
    print(f"Analyzing year: {target_year}\n")
    
    # Main query: Quarterly revenue growth rate by store
    query = """
    WITH QuarterlyRevenue AS (
        SELECT 
            p.storeID,
            p.storeName,
            QUARTER(t.order_date) AS quarter,
            CONCAT('Q', QUARTER(t.order_date)) AS quarter_label,
            SUM(t.quantity * p.price) AS quarterly_revenue,
            COUNT(DISTINCT t.orderID) AS total_orders,
            COUNT(DISTINCT t.Customer_ID) AS unique_customers
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY p.storeID, p.storeName, QUARTER(t.order_date)
    ),
    RevenueWithGrowth AS (
        SELECT 
            storeID,
            storeName,
            quarter,
            quarter_label,
            quarterly_revenue,
            total_orders,
            unique_customers,
            LAG(quarterly_revenue) OVER (PARTITION BY storeID ORDER BY quarter) AS prev_quarter_revenue,
            LAG(quarter_label) OVER (PARTITION BY storeID ORDER BY quarter) AS prev_quarter
        FROM QuarterlyRevenue
    )
    SELECT 
        storeID,
        storeName,
        quarter_label,
        quarterly_revenue,
        total_orders,
        unique_customers,
        prev_quarter_revenue,
        prev_quarter,
        CASE 
            WHEN prev_quarter_revenue IS NULL OR prev_quarter_revenue = 0 THEN NULL
            ELSE ROUND((quarterly_revenue - prev_quarter_revenue) / prev_quarter_revenue * 100, 2)
        END AS growth_rate_pct
    FROM RevenueWithGrowth
    ORDER BY storeID, quarter
    """
    
    cur.execute(query, (target_year,))
    results = cur.fetchall()
    
    print("=" * 150)
    print(f"Q12: STORE REVENUE GROWTH RATE - QUARTERLY ANALYSIS ({target_year})")
    print("=" * 150)
    
    current_store = None
    
    for row in results:
        store_id, store_name, quarter, revenue, orders, customers, prev_revenue, prev_quarter, growth = row
        
        if store_id != current_store:
            current_store = store_id
            print(f"\n{'='*150}")
            print(f"STORE: {store_name} (ID: {store_id})")
            print(f"{'='*150}")
            print(f"{'Quarter':<10} {'Revenue':>18} {'Orders':>12} {'Customers':>12} {'Prev Revenue':>18} {'Growth Rate':>15} {'Trend':>12}")
            print(f"{'-'*10} {'-'*18} {'-'*12} {'-'*12} {'-'*18} {'-'*15} {'-'*12}")
        
        prev_str = f"${prev_revenue:>17,.2f}" if prev_revenue else "N/A".rjust(18)
        
        if growth is None:
            growth_str = "N/A".rjust(15)
            trend = "Baseline"
        elif growth > 10:
            growth_str = f"+{growth:>13.2f}%"
            trend = "📈 Strong"
        elif growth > 0:
            growth_str = f"+{growth:>13.2f}%"
            trend = "↑ Growth"
        elif growth > -10:
            growth_str = f"{growth:>14.2f}%"
            trend = "↓ Decline"
        else:
            growth_str = f"{growth:>14.2f}%"
            trend = "📉 Sharp"
        
        print(f"{quarter:<10} ${revenue:>17,.2f} {orders:>12,} {customers:>12,} {prev_str} {growth_str} {trend:>12}")
    
    # Summary: Store performance overview
    print("\n\n" + "=" * 150)
    print(f"SUMMARY: STORE PERFORMANCE OVERVIEW ({target_year})")
    print("=" * 150)
    
    summary_query = """
    WITH QuarterlyRevenue AS (
        SELECT 
            p.storeID,
            p.storeName,
            QUARTER(t.order_date) AS quarter,
            SUM(t.quantity * p.price) AS quarterly_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        WHERE YEAR(t.order_date) = %s
        GROUP BY p.storeID, p.storeName, QUARTER(t.order_date)
    ),
    GrowthRates AS (
        SELECT 
            storeID,
            storeName,
            quarterly_revenue,
            LAG(quarterly_revenue) OVER (PARTITION BY storeID ORDER BY quarter) AS prev_revenue
        FROM QuarterlyRevenue
    )
    SELECT 
        storeID,
        storeName,
        COUNT(*) AS quarters_with_data,
        SUM(quarterly_revenue) AS total_revenue,
        ROUND(AVG(CASE 
            WHEN prev_revenue IS NOT NULL AND prev_revenue > 0 
            THEN (quarterly_revenue - prev_revenue) / prev_revenue * 100 
        END), 2) AS avg_growth_rate
    FROM GrowthRates
    GROUP BY storeID, storeName
    ORDER BY avg_growth_rate DESC
    """
    
    cur.execute(summary_query, (target_year,))
    
    print(f"\n{'Store ID':<10} {'Store Name':<30} {'Quarters':>10} {'Total Revenue':>20} {'Avg Growth Rate':>18} {'Rating':>15}")
    print(f"{'-'*10} {'-'*30} {'-'*10} {'-'*20} {'-'*18} {'-'*15}")
    
    for row in cur.fetchall():
        store_id, store_name, quarters, total, avg_growth = row
        
        if avg_growth is None:
            growth_str = "N/A"
            rating = "Insufficient Data"
        elif avg_growth > 15:
            growth_str = f"+{avg_growth:.2f}%"
            rating = "⭐ Excellent"
        elif avg_growth > 5:
            growth_str = f"+{avg_growth:.2f}%"
            rating = "Good"
        elif avg_growth > 0:
            growth_str = f"+{avg_growth:.2f}%"
            rating = "Moderate"
        elif avg_growth > -5:
            growth_str = f"{avg_growth:.2f}%"
            rating = "Needs Attention"
        else:
            growth_str = f"{avg_growth:.2f}%"
            rating = "⚠️ Critical"
        
        store_display = store_name[:28] + ".." if len(str(store_name)) > 30 else store_name
        print(f"{store_id:<10} {store_display:<30} {quarters:>10} ${total:>19,.2f} {growth_str:>18} {rating:>15}")
    
    print("\n" + "=" * 150)
    conn.close()

if __name__ == "__main__":
    run_query()
