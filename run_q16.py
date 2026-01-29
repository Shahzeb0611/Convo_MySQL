"""
Q16: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
Identify the top 5 products frequently bought together within a set of orders
(i.e., multiple products purchased by the same customer on the same day).
This product affinity analysis could inform potential product bundling strategies.
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
    
    print("=" * 150)
    print("Q16: TOP PRODUCTS PURCHASED TOGETHER - PRODUCT AFFINITY ANALYSIS")
    print("=" * 150)
    print("\nAnalyzing products purchased by the same customer on the same day...")
    
    # Main query: Find product pairs purchased together (same customer, same day)
    query = """
    WITH CustomerDayOrders AS (
        -- Get all products purchased by each customer on each day
        SELECT 
            t.Customer_ID,
            t.order_date,
            t.Product_ID
        FROM transactional_data t
    ),
    ProductPairs AS (
        -- Create pairs of products bought together
        SELECT 
            a.Product_ID AS product_1,
            b.Product_ID AS product_2,
            COUNT(DISTINCT CONCAT(a.Customer_ID, '-', a.order_date)) AS times_bought_together
        FROM CustomerDayOrders a
        JOIN CustomerDayOrders b 
            ON a.Customer_ID = b.Customer_ID 
            AND a.order_date = b.order_date
            AND a.Product_ID < b.Product_ID  -- Avoid duplicates and self-pairs
        GROUP BY a.Product_ID, b.Product_ID
        HAVING COUNT(DISTINCT CONCAT(a.Customer_ID, '-', a.order_date)) > 1
    )
    SELECT 
        pp.product_1,
        p1.Product_Category AS category_1,
        pp.product_2,
        p2.Product_Category AS category_2,
        pp.times_bought_together
    FROM ProductPairs pp
    JOIN product_master p1 ON pp.product_1 = p1.Product_ID
    JOIN product_master p2 ON pp.product_2 = p2.Product_ID
    ORDER BY pp.times_bought_together DESC
    LIMIT 20
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("\n" + "-" * 150)
    print("TOP 20 PRODUCT PAIRS FREQUENTLY PURCHASED TOGETHER")
    print("-" * 150)
    print(f"\n{'Rank':<6} {'Product 1':<15} {'Category 1':<20} {'Product 2':<15} {'Category 2':<20} {'Times Together':>18}")
    print(f"{'-'*6} {'-'*15} {'-'*20} {'-'*15} {'-'*20} {'-'*18}")
    
    for i, row in enumerate(results, 1):
        p1, cat1, p2, cat2, times = row
        print(f"#{i:<5} {p1:<15} {cat1:<20} {p2:<15} {cat2:<20} {times:>18,}")
    
    # Category affinity analysis
    print("\n\n" + "=" * 150)
    print("CATEGORY AFFINITY: WHICH CATEGORIES ARE BOUGHT TOGETHER?")
    print("=" * 150)
    
    category_affinity_query = """
    WITH CustomerDayOrders AS (
        SELECT 
            t.Customer_ID,
            t.order_date,
            p.Product_Category
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
    ),
    CategoryPairs AS (
        SELECT 
            a.Product_Category AS category_1,
            b.Product_Category AS category_2,
            COUNT(DISTINCT CONCAT(a.Customer_ID, '-', a.order_date)) AS times_bought_together
        FROM CustomerDayOrders a
        JOIN CustomerDayOrders b 
            ON a.Customer_ID = b.Customer_ID 
            AND a.order_date = b.order_date
            AND a.Product_Category < b.Product_Category
        GROUP BY a.Product_Category, b.Product_Category
    )
    SELECT *
    FROM CategoryPairs
    ORDER BY times_bought_together DESC
    LIMIT 15
    """
    
    cur.execute(category_affinity_query)
    
    print(f"\n{'Category 1':<25} {'Category 2':<25} {'Times Together':>18} {'Affinity Score':>18}")
    print(f"{'-'*25} {'-'*25} {'-'*18} {'-'*18}")
    
    cat_results = cur.fetchall()
    max_times = cat_results[0][2] if cat_results else 1
    
    for row in cat_results:
        cat1, cat2, times = row
        affinity_score = round(times / max_times * 100, 1)
        print(f"{cat1:<25} {cat2:<25} {times:>18,} {affinity_score:>17.1f}%")
    
    # Same-category purchases (customers buying multiple from same category)
    print("\n\n" + "=" * 150)
    print("SAME-CATEGORY MULTI-PRODUCT PURCHASES")
    print("=" * 150)
    
    same_cat_query = """
    WITH CustomerDayCategoryProducts AS (
        SELECT 
            t.Customer_ID,
            t.order_date,
            p.Product_Category,
            COUNT(DISTINCT t.Product_ID) AS products_count
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY t.Customer_ID, t.order_date, p.Product_Category
        HAVING COUNT(DISTINCT t.Product_ID) > 1
    )
    SELECT 
        Product_Category,
        COUNT(*) AS multi_product_sessions,
        ROUND(AVG(products_count), 2) AS avg_products_per_session,
        MAX(products_count) AS max_products_in_session
    FROM CustomerDayCategoryProducts
    GROUP BY Product_Category
    ORDER BY multi_product_sessions DESC
    """
    
    cur.execute(same_cat_query)
    
    print("\nCategories where customers buy multiple products in same session:")
    print(f"\n{'Category':<25} {'Multi-Product Sessions':>22} {'Avg Products':>15} {'Max Products':>15}")
    print(f"{'-'*25} {'-'*22} {'-'*15} {'-'*15}")
    
    for row in cur.fetchall():
        cat, sessions, avg_prod, max_prod = row
        print(f"{cat:<25} {sessions:>22,} {avg_prod:>15.2f} {max_prod:>15}")
    
    # Bundling recommendations
    print("\n\n" + "=" * 150)
    print("PRODUCT BUNDLING RECOMMENDATIONS")
    print("=" * 150)
    
    print("\nBased on the affinity analysis, consider these bundle strategies:")
    print("-" * 80)
    
    if results:
        print("\n📦 TOP 5 SUGGESTED PRODUCT BUNDLES:")
        for i, row in enumerate(results[:5], 1):
            p1, cat1, p2, cat2, times = row
            print(f"\n  Bundle #{i}:")
            print(f"    • {p1} ({cat1})")
            print(f"    • {p2} ({cat2})")
            print(f"    → Purchased together {times} times")
    
    print("\n" + "=" * 150)
    conn.close()

if __name__ == "__main__":
    run_query()
