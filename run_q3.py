"""
Q3: Product Category Sales by Occupation
Examines total sales for each product category based on customer occupation.
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
    
    # Main query: Sales by occupation and product category
    query = """
    SELECT 
        c.Occupation,
        p.Product_Category,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_sale_per_transaction
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Occupation, p.Product_Category
    ORDER BY c.Occupation, total_sales DESC
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 120)
    print("Q3: PRODUCT CATEGORY SALES BY OCCUPATION")
    print("=" * 120)
    
    current_occ = None
    for row in results:
        occ, category, orders, qty, sales, avg_sale = row
        
        if occ != current_occ:
            current_occ = occ
            print(f"\n--- OCCUPATION {occ} ---")
            print(f"{'Category':<25} {'Orders':>10} {'Quantity':>10} {'Total Sales':>18} {'Avg/Transaction':>18}")
            print(f"{'-'*25} {'-'*10} {'-'*10} {'-'*18} {'-'*18}")
        
        print(f"{category:<25} {orders:>10,} {qty:>10,} ${sales:>17,.2f} ${avg_sale:>17,.2f}")
    
    # Summary by Occupation
    print("\n\n" + "=" * 120)
    print("SUMMARY BY OCCUPATION")
    print("=" * 120)
    
    occ_query = """
    SELECT 
        c.Occupation,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT c.Customer_ID), 2) AS avg_per_customer
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Occupation
    ORDER BY total_sales DESC
    """
    
    cur.execute(occ_query)
    print(f"\n{'Occupation':>10} {'Customers':>12} {'Orders':>12} {'Total Sales':>18} {'Avg/Trans':>14} {'Avg/Customer':>16}")
    print(f"{'-'*10} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    for row in cur.fetchall():
        occ, customers, orders, sales, avg_trans, avg_cust = row
        print(f"{occ:>10} {customers:>12,} {orders:>12,} ${sales:>17,.2f} ${avg_trans:>13,.2f} ${avg_cust:>15,.2f}")
    
    # Summary by Product Category
    print("\n\n" + "=" * 120)
    print("SUMMARY BY PRODUCT CATEGORY")
    print("=" * 120)
    
    cat_query = """
    SELECT 
        p.Product_Category,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction,
        ROUND(SUM(t.quantity * p.price) / COUNT(DISTINCT p.Product_ID), 2) AS avg_per_product
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY p.Product_Category
    ORDER BY total_sales DESC
    """
    
    cur.execute(cat_query)
    print(f"\n{'Category':<25} {'Orders':>12} {'Quantity':>12} {'Total Sales':>18} {'Avg/Trans':>14} {'Avg/Product':>16}")
    print(f"{'-'*25} {'-'*12} {'-'*12} {'-'*18} {'-'*14} {'-'*16}")
    for row in cur.fetchall():
        cat, orders, qty, sales, avg_trans, avg_prod = row
        print(f"{cat:<25} {orders:>12,} {qty:>12,} ${sales:>17,.2f} ${avg_trans:>13,.2f} ${avg_prod:>15,.2f}")
    
    # Top 5 Occupation-Category Combinations
    print("\n\n" + "=" * 120)
    print("TOP 10 OCCUPATION-CATEGORY COMBINATIONS BY SALES")
    print("=" * 120)
    
    top_query = """
    SELECT 
        c.Occupation,
        p.Product_Category,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Occupation, p.Product_Category
    ORDER BY total_sales DESC
    LIMIT 10
    """
    
    cur.execute(top_query)
    print(f"\n{'Rank':<6} {'Occupation':>10} {'Category':<25} {'Total Sales':>18}")
    print(f"{'-'*6} {'-'*10} {'-'*25} {'-'*18}")
    for i, row in enumerate(cur.fetchall(), 1):
        occ, cat, sales = row
        print(f"{i:<6} {occ:>10} {cat:<25} ${sales:>17,.2f}")
    
    # Pivot table: Category x Occupation (top categories)
    print("\n\n" + "=" * 120)
    print("SALES MATRIX: TOP CATEGORIES x OCCUPATIONS")
    print("=" * 120)
    
    pivot_query = """
    SELECT 
        p.Product_Category,
        c.Occupation,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE p.Product_Category IN (
        SELECT Product_Category FROM (
            SELECT Product_Category, SUM(quantity * price) as s
            FROM transactional_data t2
            JOIN product_master p2 ON t2.Product_ID = p2.Product_ID
            GROUP BY Product_Category
            ORDER BY s DESC
            LIMIT 5
        ) top_cats
    )
    GROUP BY p.Product_Category, c.Occupation
    ORDER BY p.Product_Category, c.Occupation
    """
    
    cur.execute(pivot_query)
    pivot_results = cur.fetchall()
    
    # Build pivot data
    categories = []
    occupations = set()
    data = {}
    for cat, occ, sales in pivot_results:
        if cat not in categories:
            categories.append(cat)
        occupations.add(occ)
        if cat not in data:
            data[cat] = {}
        data[cat][occ] = sales
    
    occupations = sorted(occupations)
    
    # Print header
    header = f"{'Category':<25}"
    for occ in occupations:
        header += f" {'Occ '+str(occ):>10}"
    print(f"\n{header}")
    print("-" * (25 + 11 * len(occupations)))
    
    for cat in categories:
        row_str = f"{cat:<25}"
        for occ in occupations:
            val = data.get(cat, {}).get(occ, 0)
            row_str += f" ${val/1000:>8,.1f}K"
        print(row_str)
    
    print("\n" + "=" * 120)
    conn.close()

if __name__ == "__main__":
    run_query()
