"""
Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
Calculate the month-to-month revenue volatility for each store and supplier pair.
Volatility is defined as the percentage change in revenue from one month to the next,
helping identify stores or suppliers with highly fluctuating sales.
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
    
    # Main query: Monthly revenue volatility by store-supplier pair
    query = """
    WITH MonthlyRevenue AS (
        SELECT 
            p.storeID,
            p.storeName,
            p.supplierID,
            p.supplierName,
            DATE_FORMAT(t.order_date, '%%Y-%%m') AS month_year,
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            SUM(t.quantity * p.price) AS monthly_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.storeID, p.storeName, p.supplierID, p.supplierName, 
                 DATE_FORMAT(t.order_date, '%%Y-%%m'), YEAR(t.order_date), MONTH(t.order_date)
    ),
    RevenueWithChange AS (
        SELECT 
            storeID,
            storeName,
            supplierID,
            supplierName,
            month_year,
            monthly_revenue,
            LAG(monthly_revenue) OVER (PARTITION BY storeID, supplierID ORDER BY year, month) AS prev_revenue,
            LAG(month_year) OVER (PARTITION BY storeID, supplierID ORDER BY year, month) AS prev_month
        FROM MonthlyRevenue
    )
    SELECT 
        storeID,
        storeName,
        supplierID,
        supplierName,
        month_year,
        monthly_revenue,
        prev_revenue,
        CASE 
            WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
            ELSE ROUND((monthly_revenue - prev_revenue) / prev_revenue * 100, 2)
        END AS pct_change
    FROM RevenueWithChange
    ORDER BY storeName, supplierName, month_year
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 160)
    print("Q15: STORE-WISE AND SUPPLIER-WISE MONTHLY REVENUE VOLATILITY")
    print("=" * 160)
    
    current_store = None
    current_supplier = None
    
    for row in results:
        store_id, store_name, supplier_id, supplier_name, month, revenue, prev_rev, pct_change = row
        
        if store_id != current_store or supplier_id != current_supplier:
            current_store = store_id
            current_supplier = supplier_id
            print(f"\n{'='*160}")
            print(f"STORE: {store_name} (ID: {store_id}) | SUPPLIER: {supplier_name} (ID: {supplier_id})")
            print(f"{'='*160}")
            print(f"{'Month':<12} {'Revenue':>18} {'Prev Revenue':>18} {'% Change':>15} {'Volatility':>15}")
            print(f"{'-'*12} {'-'*18} {'-'*18} {'-'*15} {'-'*15}")
        
        prev_str = f"${prev_rev:>17,.2f}" if prev_rev else "N/A".rjust(18)
        
        if pct_change is None:
            change_str = "N/A".rjust(15)
            volatility = "Baseline"
        elif abs(pct_change) > 50:
            change_str = f"{pct_change:>14.2f}%"
            volatility = "⚠️ EXTREME"
        elif abs(pct_change) > 25:
            change_str = f"{pct_change:>14.2f}%"
            volatility = "HIGH"
        elif abs(pct_change) > 10:
            change_str = f"{pct_change:>14.2f}%"
            volatility = "Moderate"
        else:
            change_str = f"{pct_change:>14.2f}%"
            volatility = "Stable"
        
        print(f"{month:<12} ${revenue:>17,.2f} {prev_str} {change_str} {volatility:>15}")
    
    # Summary: Most volatile store-supplier pairs
    print("\n\n" + "=" * 160)
    print("SUMMARY: VOLATILITY ANALYSIS BY STORE-SUPPLIER PAIR")
    print("=" * 160)
    
    volatility_query = """
    WITH MonthlyRevenue AS (
        SELECT 
            p.storeID,
            p.storeName,
            p.supplierID,
            p.supplierName,
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            SUM(t.quantity * p.price) AS monthly_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.storeID, p.storeName, p.supplierID, p.supplierName, 
                 YEAR(t.order_date), MONTH(t.order_date)
    ),
    RevenueWithChange AS (
        SELECT 
            storeID,
            storeName,
            supplierID,
            supplierName,
            monthly_revenue,
            LAG(monthly_revenue) OVER (PARTITION BY storeID, supplierID ORDER BY year, month) AS prev_revenue
        FROM MonthlyRevenue
    ),
    VolatilityStats AS (
        SELECT 
            storeID,
            storeName,
            supplierID,
            supplierName,
            COUNT(*) AS months_count,
            SUM(monthly_revenue) AS total_revenue,
            ROUND(AVG(CASE 
                WHEN prev_revenue IS NOT NULL AND prev_revenue > 0 
                THEN ABS((monthly_revenue - prev_revenue) / prev_revenue * 100)
            END), 2) AS avg_volatility,
            ROUND(MAX(CASE 
                WHEN prev_revenue IS NOT NULL AND prev_revenue > 0 
                THEN ABS((monthly_revenue - prev_revenue) / prev_revenue * 100)
            END), 2) AS max_volatility
        FROM RevenueWithChange
        GROUP BY storeID, storeName, supplierID, supplierName
        HAVING COUNT(*) > 1
    )
    SELECT *
    FROM VolatilityStats
    ORDER BY avg_volatility DESC
    LIMIT 20
    """
    
    cur.execute(volatility_query)
    
    print("\nTOP 20 MOST VOLATILE STORE-SUPPLIER PAIRS")
    print(f"\n{'Store':<25} {'Supplier':<25} {'Months':>8} {'Total Revenue':>18} {'Avg Vol %':>12} {'Max Vol %':>12} {'Risk Level':>12}")
    print(f"{'-'*25} {'-'*25} {'-'*8} {'-'*18} {'-'*12} {'-'*12} {'-'*12}")
    
    for row in cur.fetchall():
        store_id, store_name, supplier_id, supplier_name, months, total_rev, avg_vol, max_vol = row
        
        store_display = store_name[:23] + ".." if len(str(store_name)) > 25 else store_name
        supplier_display = supplier_name[:23] + ".." if len(str(supplier_name)) > 25 else supplier_name
        
        if avg_vol and avg_vol > 30:
            risk = "⚠️ HIGH"
        elif avg_vol and avg_vol > 15:
            risk = "Medium"
        else:
            risk = "Low"
        
        avg_vol_str = f"{avg_vol:.2f}%" if avg_vol else "N/A"
        max_vol_str = f"{max_vol:.2f}%" if max_vol else "N/A"
        
        print(f"{store_display:<25} {supplier_display:<25} {months:>8} ${total_rev:>17,.2f} {avg_vol_str:>12} {max_vol_str:>12} {risk:>12}")
    
    # Most stable pairs
    print("\n\nTOP 10 MOST STABLE STORE-SUPPLIER PAIRS")
    
    stable_query = """
    WITH MonthlyRevenue AS (
        SELECT 
            p.storeID,
            p.storeName,
            p.supplierID,
            p.supplierName,
            YEAR(t.order_date) AS year,
            MONTH(t.order_date) AS month,
            SUM(t.quantity * p.price) AS monthly_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.storeID, p.storeName, p.supplierID, p.supplierName, 
                 YEAR(t.order_date), MONTH(t.order_date)
    ),
    RevenueWithChange AS (
        SELECT 
            storeID,
            storeName,
            supplierID,
            supplierName,
            monthly_revenue,
            LAG(monthly_revenue) OVER (PARTITION BY storeID, supplierID ORDER BY year, month) AS prev_revenue
        FROM MonthlyRevenue
    ),
    VolatilityStats AS (
        SELECT 
            storeID,
            storeName,
            supplierID,
            supplierName,
            COUNT(*) AS months_count,
            SUM(monthly_revenue) AS total_revenue,
            ROUND(AVG(CASE 
                WHEN prev_revenue IS NOT NULL AND prev_revenue > 0 
                THEN ABS((monthly_revenue - prev_revenue) / prev_revenue * 100)
            END), 2) AS avg_volatility
        FROM RevenueWithChange
        GROUP BY storeID, storeName, supplierID, supplierName
        HAVING COUNT(*) > 3
    )
    SELECT *
    FROM VolatilityStats
    WHERE avg_volatility IS NOT NULL
    ORDER BY avg_volatility ASC
    LIMIT 10
    """
    
    cur.execute(stable_query)
    
    print(f"\n{'Store':<25} {'Supplier':<25} {'Months':>8} {'Total Revenue':>18} {'Avg Volatility':>15}")
    print(f"{'-'*25} {'-'*25} {'-'*8} {'-'*18} {'-'*15}")
    
    for row in cur.fetchall():
        store_id, store_name, supplier_id, supplier_name, months, total_rev, avg_vol = row
        store_display = store_name[:23] + ".." if len(str(store_name)) > 25 else store_name
        supplier_display = supplier_name[:23] + ".." if len(str(supplier_name)) > 25 else supplier_name
        print(f"{store_display:<25} {supplier_display:<25} {months:>8} ${total_rev:>17,.2f} {avg_vol:>14.2f}%")
    
    print("\n" + "=" * 160)
    conn.close()

if __name__ == "__main__":
    run_query()
