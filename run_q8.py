"""
Q8: Top 5 Revenue-Generating Cities by Product Category
Ranks the top 5 city categories by revenue, grouped by product category.
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
    
    # Main query: Top 5 city categories by revenue per product category
    query = """
    WITH CityRevenue AS (
        SELECT 
            p.Product_Category,
            c.City_Category,
            COUNT(DISTINCT t.orderID) AS total_orders,
            COUNT(DISTINCT c.Customer_ID) AS unique_customers,
            SUM(t.quantity) AS total_quantity,
            SUM(t.quantity * p.price) AS total_revenue
        FROM transactional_data t
        JOIN customer_master c ON t.Customer_ID = c.Customer_ID
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_Category, c.City_Category
    ),
    RankedCities AS (
        SELECT 
            Product_Category,
            City_Category,
            total_orders,
            unique_customers,
            total_quantity,
            total_revenue,
            ROW_NUMBER() OVER (PARTITION BY Product_Category ORDER BY total_revenue DESC) AS revenue_rank,
            ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (PARTITION BY Product_Category), 2) AS pct_of_category
        FROM CityRevenue
    )
    SELECT 
        Product_Category,
        City_Category,
        total_orders,
        unique_customers,
        total_quantity,
        total_revenue,
        revenue_rank,
        pct_of_category
    FROM RankedCities
    WHERE revenue_rank <= 5
    ORDER BY Product_Category, revenue_rank
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 140)
    print("Q8: TOP 5 REVENUE-GENERATING CITIES BY PRODUCT CATEGORY")
    print("=" * 140)
    
    current_category = None
    
    for row in results:
        category, city, orders, customers, qty, revenue, rank, pct = row
        
        if category != current_category:
            current_category = category
            print(f"\n{'='*140}")
            print(f"PRODUCT CATEGORY: {category}")
            print(f"{'='*140}")
            print(f"{'Rank':<6} {'City':<10} {'Orders':>12} {'Customers':>12} {'Quantity':>12} {'Revenue':>18} {'% of Category':>15}")
            print(f"{'-'*6} {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*15}")
        
        print(f"#{rank:<5} City {city:<5} {orders:>12,} {customers:>12,} {qty:>12,} ${revenue:>17,.2f} {pct:>14.2f}%")
    
    # Summary: Overall city performance
    print("\n\n" + "=" * 140)
    print("SUMMARY: OVERALL CITY CATEGORY PERFORMANCE")
    print("=" * 140)
    
    summary_query = """
    SELECT 
        c.City_Category,
        COUNT(DISTINCT p.Product_Category) AS categories_sold,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_revenue,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.City_Category
    ORDER BY total_revenue DESC
    """
    
    cur.execute(summary_query)
    print(f"\n{'City':<10} {'Categories':<12} {'Customers':>12} {'Orders':>12} {'Total Revenue':>18} {'Avg/Trans':>16}")
    print(f"{'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*16}")
    
    total_revenue = 0
    city_data = []
    for row in cur.fetchall():
        city, cats, customers, orders, revenue, avg_trans = row
        total_revenue += float(revenue)
        city_data.append((city, cats, customers, orders, revenue, avg_trans))
    
    for city, cats, customers, orders, revenue, avg_trans in city_data:
        pct = float(revenue) / total_revenue * 100
        print(f"City {city:<5} {cats:<12} {customers:>12,} {orders:>12,} ${revenue:>17,.2f} ${avg_trans:>15,.2f} ({pct:.1f}%)")
    
    print(f"\n{'TOTAL':<10} {'':<12} {'':<12} {'':<12} ${total_revenue:>17,.2f}")
    
    print("\n" + "=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
