"""
Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product,
enabling a comprehensive overview from individual product-level details up to total revenue per store.
This query provides an overview of cumulative and hierarchical sales figures.
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
    
    # Main query: ROLLUP for hierarchical revenue by store, supplier, product
    query = """
    SELECT 
        p.storeName,
        p.supplierName,
        p.Product_ID,
        SUM(t.quantity * p.price) AS total_revenue,
        SUM(t.quantity) AS total_quantity,
        COUNT(DISTINCT t.orderID) AS total_orders
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.storeName, p.supplierName, p.Product_ID WITH ROLLUP
    ORDER BY 
        CASE WHEN p.storeName IS NULL THEN 1 ELSE 0 END,
        p.storeName,
        CASE WHEN p.supplierName IS NULL THEN 1 ELSE 0 END,
        p.supplierName,
        CASE WHEN p.Product_ID IS NULL THEN 1 ELSE 0 END,
        total_revenue DESC
    """
    
    cur.execute(query, (target_year,))
    results = cur.fetchall()
    
    print("=" * 150)
    print(f"Q17: YEARLY REVENUE TRENDS WITH ROLLUP - HIERARCHICAL VIEW ({target_year})")
    print("=" * 150)
    print("\nHierarchy: Store → Supplier → Product")
    print("ROLLUP provides subtotals at each level and a grand total\n")
    
    current_store = "INIT"
    current_supplier = "INIT"
    
    for row in results:
        store, supplier, product, revenue, qty, orders = row
        
        # Grand total (all NULL)
        if store is None:
            print(f"\n{'='*150}")
            print(f"{'GRAND TOTAL':>50} {orders:>12,} orders {qty:>12,} units ${revenue:>20,.2f}")
            print(f"{'='*150}")
            continue
        
        # Store subtotal (supplier and product NULL)
        if supplier is None:
            print(f"\n{'='*150}")
            print(f"STORE TOTAL: {store}")
            print(f"{'':>50} {orders:>12,} orders {qty:>12,} units ${revenue:>20,.2f}")
            print(f"{'='*150}")
            current_store = store
            current_supplier = "INIT"
            continue
        
        # Supplier subtotal (product NULL)
        if product is None:
            if supplier != current_supplier:
                print(f"\n  SUPPLIER SUBTOTAL: {supplier}")
                print(f"  {'':>48} {orders:>12,} orders {qty:>12,} units ${revenue:>20,.2f}")
                print(f"  {'-'*140}")
                current_supplier = supplier
            continue
        
        # New store header
        if store != current_store:
            current_store = store
            current_supplier = "INIT"
            print(f"\n{'='*150}")
            print(f"STORE: {store}")
            print(f"{'='*150}")
        
        # New supplier header
        if supplier != current_supplier:
            current_supplier = supplier
            print(f"\n  SUPPLIER: {supplier}")
            print(f"  {'Product ID':<20} {'Orders':>12} {'Quantity':>12} {'Revenue':>20}")
            print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*20}")
        
        # Product detail
        print(f"    {product:<20} {orders:>12,} {qty:>12,} ${revenue:>19,.2f}")
    
    # Alternative summary view
    print("\n\n" + "=" * 150)
    print("SUMMARY VIEW: STORE-LEVEL TOTALS")
    print("=" * 150)
    
    store_summary = """
    SELECT 
        p.storeName,
        COUNT(DISTINCT p.supplierID) AS supplier_count,
        COUNT(DISTINCT p.Product_ID) AS product_count,
        COUNT(DISTINCT t.orderID) AS total_orders,
        SUM(t.quantity) AS total_quantity,
        SUM(t.quantity * p.price) AS total_revenue
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.storeName
    ORDER BY total_revenue DESC
    """
    
    cur.execute(store_summary, (target_year,))
    
    print(f"\n{'Store':<35} {'Suppliers':>10} {'Products':>10} {'Orders':>12} {'Quantity':>12} {'Revenue':>20}")
    print(f"{'-'*35} {'-'*10} {'-'*10} {'-'*12} {'-'*12} {'-'*20}")
    
    total_rev = 0
    for row in cur.fetchall():
        store, suppliers, products, orders, qty, revenue = row
        store_display = store[:33] + ".." if len(str(store)) > 35 else store
        print(f"{store_display:<35} {suppliers:>10} {products:>10} {orders:>12,} {qty:>12,} ${revenue:>19,.2f}")
        total_rev += float(revenue)
    
    print(f"{'-'*35} {'-'*10} {'-'*10} {'-'*12} {'-'*12} {'-'*20}")
    print(f"{'TOTAL':<35} {'':<10} {'':<10} {'':<12} {'':<12} ${total_rev:>19,.2f}")
    
    print("\n" + "=" * 150)
    conn.close()

if __name__ == "__main__":
    run_query()
