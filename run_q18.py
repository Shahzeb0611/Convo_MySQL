"""
Q18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
For each product, calculate the total revenue and quantity sold in the first and second halves
of the year, along with yearly totals. This split-by-time-period analysis can reveal changes
in product popularity or demand over the year.
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
    print(f"Analyzing year: {target_year}")
    print("H1 = January-June, H2 = July-December\n")
    
    # Main query: H1 vs H2 analysis by product
    query = """
    SELECT 
        p.Product_ID,
        p.Product_Category,
        -- H1 metrics (Jan-Jun)
        SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN t.quantity ELSE 0 END) AS h1_quantity,
        SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN t.quantity * p.price ELSE 0 END) AS h1_revenue,
        COUNT(DISTINCT CASE WHEN MONTH(t.order_date) <= 6 THEN t.orderID END) AS h1_orders,
        -- H2 metrics (Jul-Dec)
        SUM(CASE WHEN MONTH(t.order_date) > 6 THEN t.quantity ELSE 0 END) AS h2_quantity,
        SUM(CASE WHEN MONTH(t.order_date) > 6 THEN t.quantity * p.price ELSE 0 END) AS h2_revenue,
        COUNT(DISTINCT CASE WHEN MONTH(t.order_date) > 6 THEN t.orderID END) AS h2_orders,
        -- Yearly totals
        SUM(t.quantity) AS yearly_quantity,
        SUM(t.quantity * p.price) AS yearly_revenue,
        COUNT(DISTINCT t.orderID) AS yearly_orders
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.Product_ID, p.Product_Category
    ORDER BY yearly_revenue DESC
    """
    
    cur.execute(query, (target_year,))
    results = cur.fetchall()
    
    print("=" * 180)
    print(f"Q18: REVENUE AND VOLUME ANALYSIS - H1 vs H2 ({target_year})")
    print("=" * 180)
    
    print(f"\n{'Product':<15} {'Category':<15} {'H1 Qty':>10} {'H1 Revenue':>16} {'H2 Qty':>10} {'H2 Revenue':>16} {'Year Qty':>10} {'Year Revenue':>16} {'H1/H2 Trend':>15}")
    print(f"{'-'*15} {'-'*15} {'-'*10} {'-'*16} {'-'*10} {'-'*16} {'-'*10} {'-'*16} {'-'*15}")
    
    for row in results[:50]:  # Top 50 products
        product, category, h1_qty, h1_rev, h1_orders, h2_qty, h2_rev, h2_orders, yr_qty, yr_rev, yr_orders = row
        
        h1_rev = float(h1_rev)
        h2_rev = float(h2_rev)
        
        if h1_rev == 0 and h2_rev == 0:
            trend = "No Sales"
        elif h1_rev == 0:
            trend = "↑ H2 Only"
        elif h2_rev == 0:
            trend = "↓ H1 Only"
        elif h2_rev > h1_rev * 1.2:
            trend = "📈 H2 Strong"
        elif h1_rev > h2_rev * 1.2:
            trend = "📉 H2 Weak"
        else:
            trend = "→ Stable"
        
        cat_display = category[:13] + ".." if len(str(category)) > 15 else category
        print(f"{product:<15} {cat_display:<15} {h1_qty:>10,} ${h1_rev:>15,.2f} {h2_qty:>10,} ${h2_rev:>15,.2f} {yr_qty:>10,} ${float(yr_rev):>15,.2f} {trend:>15}")
    
    if len(results) > 50:
        print(f"\n... and {len(results) - 50} more products")
    
    # Category-level H1/H2 comparison
    print("\n\n" + "=" * 180)
    print("CATEGORY-LEVEL H1 vs H2 COMPARISON")
    print("=" * 180)
    
    cat_query = """
    SELECT 
        p.Product_Category,
        SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN t.quantity * p.price ELSE 0 END) AS h1_revenue,
        SUM(CASE WHEN MONTH(t.order_date) > 6 THEN t.quantity * p.price ELSE 0 END) AS h2_revenue,
        SUM(t.quantity * p.price) AS yearly_revenue
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.Product_Category
    ORDER BY yearly_revenue DESC
    """
    
    cur.execute(cat_query, (target_year,))
    
    print(f"\n{'Category':<25} {'H1 Revenue':>20} {'H2 Revenue':>20} {'Total':>20} {'H1 %':>10} {'H2 %':>10} {'Performance':>15}")
    print(f"{'-'*25} {'-'*20} {'-'*20} {'-'*20} {'-'*10} {'-'*10} {'-'*15}")
    
    grand_h1 = 0
    grand_h2 = 0
    
    for row in cur.fetchall():
        cat, h1_rev, h2_rev, total = row
        h1_rev = float(h1_rev)
        h2_rev = float(h2_rev)
        total = float(total)
        
        h1_pct = (h1_rev / total * 100) if total > 0 else 0
        h2_pct = (h2_rev / total * 100) if total > 0 else 0
        
        if h2_pct > h1_pct + 10:
            perf = "H2 Stronger"
        elif h1_pct > h2_pct + 10:
            perf = "H1 Stronger"
        else:
            perf = "Balanced"
        
        grand_h1 += h1_rev
        grand_h2 += h2_rev
        
        cat_display = cat[:23] + ".." if len(str(cat)) > 25 else cat
        print(f"{cat_display:<25} ${h1_rev:>19,.2f} ${h2_rev:>19,.2f} ${total:>19,.2f} {h1_pct:>9.1f}% {h2_pct:>9.1f}% {perf:>15}")
    
    grand_total = grand_h1 + grand_h2
    print(f"{'-'*25} {'-'*20} {'-'*20} {'-'*20} {'-'*10} {'-'*10}")
    print(f"{'GRAND TOTAL':<25} ${grand_h1:>19,.2f} ${grand_h2:>19,.2f} ${grand_total:>19,.2f} {(grand_h1/grand_total*100):>9.1f}% {(grand_h2/grand_total*100):>9.1f}%")
    
    # Products with significant H1/H2 difference
    print("\n\n" + "=" * 180)
    print("PRODUCTS WITH SIGNIFICANT H1 vs H2 REVENUE DIFFERENCE (>50%)")
    print("=" * 180)
    
    diff_query = """
    SELECT 
        p.Product_ID,
        p.Product_Category,
        SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN t.quantity * p.price ELSE 0 END) AS h1_revenue,
        SUM(CASE WHEN MONTH(t.order_date) > 6 THEN t.quantity * p.price ELSE 0 END) AS h2_revenue
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    WHERE YEAR(t.order_date) = %s
    GROUP BY p.Product_ID, p.Product_Category
    HAVING h1_revenue > 0 AND h2_revenue > 0
        AND (h1_revenue > h2_revenue * 1.5 OR h2_revenue > h1_revenue * 1.5)
    ORDER BY ABS(h1_revenue - h2_revenue) DESC
    LIMIT 20
    """
    
    cur.execute(diff_query, (target_year,))
    
    print(f"\n{'Product':<15} {'Category':<20} {'H1 Revenue':>18} {'H2 Revenue':>18} {'Difference':>18} {'Stronger Half':>15}")
    print(f"{'-'*15} {'-'*20} {'-'*18} {'-'*18} {'-'*18} {'-'*15}")
    
    for row in cur.fetchall():
        product, category, h1_rev, h2_rev = row
        h1_rev = float(h1_rev)
        h2_rev = float(h2_rev)
        diff = abs(h1_rev - h2_rev)
        stronger = "H1" if h1_rev > h2_rev else "H2"
        
        cat_display = category[:18] + ".." if len(str(category)) > 20 else category
        print(f"{product:<15} {cat_display:<20} ${h1_rev:>17,.2f} ${h2_rev:>17,.2f} ${diff:>17,.2f} {stronger:>15}")
    
    print("\n" + "=" * 180)
    conn.close()

if __name__ == "__main__":
    run_query()
