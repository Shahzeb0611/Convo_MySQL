"""
Q5: Top Occupations by Product Category Sales
Highlights the top 5 occupations driving sales within each product category.
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
    
    # Main query: Top 5 occupations by sales within each product category
    query = """
    WITH OccupationCategorySales AS (
        SELECT 
            p.Product_Category,
            c.Occupation,
            COUNT(DISTINCT t.orderID) AS total_orders,
            SUM(t.quantity) AS total_quantity,
            SUM(t.quantity * p.price) AS total_sales,
            COUNT(DISTINCT c.Customer_ID) AS unique_customers
        FROM transactional_data t
        JOIN customer_master c ON t.Customer_ID = c.Customer_ID
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_Category, c.Occupation
    ),
    RankedOccupations AS (
        SELECT 
            Product_Category,
            Occupation,
            total_orders,
            total_quantity,
            total_sales,
            unique_customers,
            ROW_NUMBER() OVER (PARTITION BY Product_Category ORDER BY total_sales DESC) AS sales_rank
        FROM OccupationCategorySales
    )
    SELECT 
        Product_Category,
        Occupation,
        total_orders,
        total_quantity,
        total_sales,
        unique_customers,
        sales_rank
    FROM RankedOccupations
    WHERE sales_rank <= 5
    ORDER BY Product_Category, sales_rank
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 130)
    print("Q5: TOP 5 OCCUPATIONS BY PRODUCT CATEGORY SALES")
    print("=" * 130)
    
    current_category = None
    category_total = 0
    
    for row in results:
        category, occupation, orders, qty, sales, customers, rank = row
        
        if category != current_category:
            if current_category is not None:
                print(f"{'':>6} {'CATEGORY TOTAL':>14} {'':<10} {'':<12} {'':<12} ${category_total:>17,.2f}")
                print()
            current_category = category
            category_total = 0
            print(f"\n{'='*130}")
            print(f"PRODUCT CATEGORY: {category}")
            print(f"{'='*130}")
            print(f"{'Rank':<6} {'Occupation':<14} {'Customers':<10} {'Orders':<12} {'Quantity':<12} {'Total Sales':>20}")
            print(f"{'-'*6} {'-'*14} {'-'*10} {'-'*12} {'-'*12} {'-'*20}")
        
        category_total += float(sales)
        print(f"#{rank:<5} {occupation:<14} {customers:<10,} {orders:<12,} {qty:<12,} ${sales:>19,.2f}")
    
    # Print last category total
    if current_category is not None:
        print(f"{'':>6} {'CATEGORY TOTAL':>14} {'':<10} {'':<12} {'':<12} ${category_total:>17,.2f}")
    
    # Summary: Top occupations overall
    print("\n\n" + "=" * 130)
    print("OVERALL TOP 10 OCCUPATIONS BY TOTAL SALES")
    print("=" * 130)
    
    overall_query = """
    SELECT 
        c.Occupation,
        COUNT(DISTINCT p.Product_Category) AS categories_purchased,
        COUNT(DISTINCT c.Customer_ID) AS unique_customers,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_per_transaction
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Occupation
    ORDER BY total_sales DESC
    LIMIT 10
    """
    
    cur.execute(overall_query)
    print(f"\n{'Occupation':<12} {'Categories':<12} {'Customers':<12} {'Orders':<12} {'Total Sales':>18} {'Avg/Trans':>16}")
    print(f"{'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*18} {'-'*16}")
    
    for row in cur.fetchall():
        occ, cats, customers, orders, sales, avg_trans = row
        print(f"{occ:<12} {cats:<12} {customers:<12,} {orders:<12,} ${sales:>17,.2f} ${avg_trans:>15,.2f}")
    
    # Occupation distribution by category count
    print("\n\n" + "=" * 130)
    print("OCCUPATION DIVERSITY: NUMBER OF CATEGORIES EACH OCCUPATION PURCHASES FROM")
    print("=" * 130)
    
    diversity_query = """
    SELECT 
        c.Occupation,
        COUNT(DISTINCT p.Product_Category) AS categories_count,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN customer_master c ON t.Customer_ID = c.Customer_ID
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY c.Occupation
    ORDER BY categories_count DESC, total_sales DESC
    """
    
    cur.execute(diversity_query)
    print(f"\n{'Occupation':<12} {'Categories Purchased':<22} {'Total Sales':>18}")
    print(f"{'-'*12} {'-'*22} {'-'*18}")
    
    for row in cur.fetchall():
        occ, cats, sales = row
        print(f"{occ:<12} {cats:<22} ${sales:>17,.2f}")
    
    print("\n" + "=" * 130)
    conn.close()

if __name__ == "__main__":
    run_query()
