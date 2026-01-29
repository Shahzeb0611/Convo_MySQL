"""
Q20: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
Create a view named STORE_QUARTERLY_SALES that aggregates total quarterly sales by store,
ordered by store name. This view allows quick retrieval of store-specific trends across quarters,
significantly improving query performance for regular sales analysis.
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
    
    print("=" * 140)
    print("Q20: CREATE STORE_QUARTERLY_SALES VIEW")
    print("=" * 140)
    
    # Drop existing view if exists
    print("\n1. Dropping existing view if it exists...")
    try:
        cur.execute("DROP VIEW IF EXISTS STORE_QUARTERLY_SALES")
        conn.commit()
        print("   ✓ Existing view dropped (or didn't exist)")
    except Exception as e:
        print(f"   Note: {e}")
    
    # Create the view
    print("\n2. Creating STORE_QUARTERLY_SALES view...")
    
    create_view_sql = """
    CREATE VIEW STORE_QUARTERLY_SALES AS
    SELECT 
        p.storeID,
        p.storeName,
        YEAR(t.order_date) AS sales_year,
        QUARTER(t.order_date) AS sales_quarter,
        CONCAT(YEAR(t.order_date), '-Q', QUARTER(t.order_date)) AS year_quarter,
        COUNT(DISTINCT t.orderID) AS total_orders,
        COUNT(DISTINCT t.Customer_ID) AS unique_customers,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_sales,
        ROUND(AVG(t.quantity * p.price), 2) AS avg_order_value,
        COUNT(DISTINCT p.Product_ID) AS products_sold,
        COUNT(DISTINCT p.supplierID) AS suppliers_involved
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY 
        p.storeID, 
        p.storeName, 
        YEAR(t.order_date), 
        QUARTER(t.order_date)
    ORDER BY 
        p.storeName, 
        sales_year, 
        sales_quarter
    """
    
    try:
        cur.execute(create_view_sql)
        conn.commit()
        print("   ✓ View STORE_QUARTERLY_SALES created successfully!")
    except Exception as e:
        print(f"   ✗ Error creating view: {e}")
        conn.close()
        return
    
    # Verify the view was created
    print("\n3. Verifying view structure...")
    cur.execute("DESCRIBE STORE_QUARTERLY_SALES")
    columns = cur.fetchall()
    
    print("\n   View columns:")
    print(f"   {'Column Name':<25} {'Type':<20} {'Null':<6} {'Key':<6}")
    print(f"   {'-'*25} {'-'*20} {'-'*6} {'-'*6}")
    for col in columns:
        col_name, col_type, null, key, default, extra = col
        print(f"   {col_name:<25} {col_type:<20} {null:<6} {key or '':<6}")
    
    # Query the view to demonstrate usage
    print("\n\n" + "=" * 140)
    print("DEMONSTRATION: QUERYING THE VIEW")
    print("=" * 140)
    
    print("\n--- All Quarterly Sales by Store ---")
    cur.execute("SELECT * FROM STORE_QUARTERLY_SALES LIMIT 20")
    results = cur.fetchall()
    
    print(f"\n{'Store ID':<10} {'Store Name':<30} {'Quarter':<10} {'Orders':>10} {'Customers':>10} {'Sales':>18}")
    print(f"{'-'*10} {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*18}")
    
    for row in results:
        store_id, store_name, year, quarter, yq, orders, customers, qty, sales, avg_val, products, suppliers = row
        store_display = store_name[:28] + ".." if len(str(store_name)) > 30 else store_name
        print(f"{store_id:<10} {store_display:<30} {yq:<10} {orders:>10,} {customers:>10,} ${sales:>17,.2f}")
    
    # Example analytical queries using the view
    print("\n\n" + "=" * 140)
    print("EXAMPLE ANALYTICAL QUERIES USING THE VIEW")
    print("=" * 140)
    
    # Query 1: Total sales by store (all time)
    print("\n--- Example 1: Total Sales by Store (All Time) ---")
    cur.execute("""
        SELECT storeName, SUM(total_sales) AS total_revenue
        FROM STORE_QUARTERLY_SALES
        GROUP BY storeID, storeName
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    
    print(f"\n{'Store Name':<40} {'Total Revenue':>20}")
    print(f"{'-'*40} {'-'*20}")
    for row in cur.fetchall():
        store, revenue = row
        store_display = store[:38] + ".." if len(str(store)) > 40 else store
        print(f"{store_display:<40} ${revenue:>19,.2f}")
    
    # Query 2: Year-over-year comparison
    print("\n\n--- Example 2: Quarterly Sales Trend ---")
    cur.execute("""
        SELECT year_quarter, SUM(total_sales) AS quarterly_total, SUM(total_orders) AS total_orders
        FROM STORE_QUARTERLY_SALES
        GROUP BY sales_year, sales_quarter, year_quarter
        ORDER BY sales_year, sales_quarter
    """)
    
    print(f"\n{'Quarter':<15} {'Total Sales':>20} {'Orders':>15}")
    print(f"{'-'*15} {'-'*20} {'-'*15}")
    for row in cur.fetchall():
        quarter, sales, orders = row
        print(f"{quarter:<15} ${sales:>19,.2f} {orders:>15,}")
    
    # Query 3: Best performing quarter per store
    print("\n\n--- Example 3: Best Quarter for Each Store ---")
    cur.execute("""
        WITH RankedQuarters AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY storeID ORDER BY total_sales DESC) AS rn
            FROM STORE_QUARTERLY_SALES
        )
        SELECT storeName, year_quarter, total_sales
        FROM RankedQuarters
        WHERE rn = 1
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    
    print(f"\n{'Store Name':<40} {'Best Quarter':<15} {'Sales':>20}")
    print(f"{'-'*40} {'-'*15} {'-'*20}")
    for row in cur.fetchall():
        store, quarter, sales = row
        store_display = store[:38] + ".." if len(str(store)) > 40 else store
        print(f"{store_display:<40} {quarter:<15} ${sales:>19,.2f}")
    
    # Benefits of the view
    print("\n\n" + "=" * 140)
    print("BENEFITS OF STORE_QUARTERLY_SALES VIEW")
    print("=" * 140)
    
    print("""
    1. PERFORMANCE OPTIMIZATION
       - Pre-aggregated data reduces query computation time
       - Eliminates repetitive JOIN and GROUP BY operations
       - Ideal for dashboards and regular reporting
    
    2. SIMPLIFIED QUERIES
       - Users can query quarterly data without complex SQL
       - Consistent metrics across all reports
       - Easy to use in BI tools (Tableau, Power BI, etc.)
    
    3. DATA CONSISTENCY
       - Single source of truth for quarterly metrics
       - Standardized calculations (avg order value, etc.)
       - Reduces risk of calculation errors
    
    4. EXAMPLE USE CASES
       - Executive quarterly business reviews
       - Store performance comparisons
       - Seasonal trend analysis
       - YoY growth calculations
    """)
    
    print("=" * 140)
    print("View STORE_QUARTERLY_SALES is now available for use!")
    print("=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
