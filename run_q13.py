"""
Q13: Detailed Supplier Sales Contribution by Store and Product Name
For each store, show the total sales contribution of each supplier broken down by product name.
The output groups results by store, then supplier, and then product name under each supplier.
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
    
    # Main query: Sales contribution by store, supplier, and product
    query = """
    SELECT 
        p.storeID,
        p.storeName,
        p.supplierID,
        p.supplierName,
        p.Product_ID,
        p.Product_Category,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY p.storeID, p.storeName, p.supplierID, p.supplierName, p.Product_ID, p.Product_Category
    ORDER BY p.storeName, p.supplierName, total_sales DESC
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 160)
    print("Q13: DETAILED SUPPLIER SALES CONTRIBUTION BY STORE AND PRODUCT")
    print("=" * 160)
    
    current_store = None
    current_supplier = None
    store_total = 0
    supplier_total = 0
    
    for row in results:
        store_id, store_name, supplier_id, supplier_name, product_id, category, orders, qty, sales = row
        
        # New store
        if store_id != current_store:
            if current_store is not None:
                # Print supplier total
                print(f"{'':>8} {'SUPPLIER TOTAL':>30} {'':<15} {'':<10} {'':<10} ${supplier_total:>17,.2f}")
                print(f"\n{'STORE TOTAL':>50} {'':<15} {'':<10} {'':<10} ${store_total:>17,.2f}")
            
            current_store = store_id
            current_supplier = None
            store_total = 0
            
            print(f"\n{'='*160}")
            print(f"STORE: {store_name} (ID: {store_id})")
            print(f"{'='*160}")
        
        # New supplier within store
        if supplier_id != current_supplier:
            if current_supplier is not None:
                print(f"{'':>8} {'SUPPLIER TOTAL':>30} {'':<15} {'':<10} {'':<10} ${supplier_total:>17,.2f}")
                print()
            
            current_supplier = supplier_id
            supplier_total = 0
            
            print(f"\n    SUPPLIER: {supplier_name} (ID: {supplier_id})")
            print(f"    {'-'*145}")
            print(f"    {'Product ID':<20} {'Category':<20} {'Orders':>12} {'Quantity':>12} {'Sales':>20}")
            print(f"    {'-'*20} {'-'*20} {'-'*12} {'-'*12} {'-'*20}")
        
        store_total += float(sales)
        supplier_total += float(sales)
        
        print(f"        {product_id:<20} {category:<20} {orders:>12,} {qty:>12,} ${sales:>19,.2f}")
    
    # Print final totals
    if current_supplier is not None:
        print(f"{'':>8} {'SUPPLIER TOTAL':>30} {'':<15} {'':<10} {'':<10} ${supplier_total:>17,.2f}")
    if current_store is not None:
        print(f"\n{'STORE TOTAL':>50} {'':<15} {'':<10} {'':<10} ${store_total:>17,.2f}")
    
    # Summary: Top suppliers by store
    print("\n\n" + "=" * 160)
    print("SUMMARY: TOP 3 SUPPLIERS PER STORE BY SALES")
    print("=" * 160)
    
    summary_query = """
    WITH SupplierSales AS (
        SELECT 
            p.storeID,
            p.storeName,
            p.supplierID,
            p.supplierName,
            SUM(t.quantity * p.price) AS total_sales,
            COUNT(DISTINCT p.Product_ID) AS products_sold,
            ROW_NUMBER() OVER (PARTITION BY p.storeID ORDER BY SUM(t.quantity * p.price) DESC) AS sales_rank
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.storeID, p.storeName, p.supplierID, p.supplierName
    )
    SELECT storeID, storeName, supplierID, supplierName, total_sales, products_sold, sales_rank
    FROM SupplierSales
    WHERE sales_rank <= 3
    ORDER BY storeName, sales_rank
    """
    
    cur.execute(summary_query)
    
    current_store = None
    for row in cur.fetchall():
        store_id, store_name, supplier_id, supplier_name, sales, products, rank = row
        
        if store_id != current_store:
            current_store = store_id
            print(f"\n{store_name} (Store ID: {store_id})")
            print(f"  {'Rank':<6} {'Supplier':<35} {'Products':>10} {'Total Sales':>20}")
            print(f"  {'-'*6} {'-'*35} {'-'*10} {'-'*20}")
        
        supplier_display = supplier_name[:33] + ".." if len(str(supplier_name)) > 35 else supplier_name
        print(f"  #{rank:<5} {supplier_display:<35} {products:>10} ${sales:>19,.2f}")
    
    print("\n" + "=" * 160)
    conn.close()

if __name__ == "__main__":
    run_query()
