"""
Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall, Winter).
This helps understand product performance across seasonal periods.
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
    
    # Season definitions:
    # Spring: March, April, May (3, 4, 5)
    # Summer: June, July, August (6, 7, 8)
    # Fall: September, October, November (9, 10, 11)
    # Winter: December, January, February (12, 1, 2)
    
    # Main query: Product sales by season
    query = """
    WITH SeasonalSales AS (
        SELECT 
            p.Product_ID,
            p.Product_Category,
            CASE 
                WHEN MONTH(t.order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(t.order_date) IN (6, 7, 8) THEN 'Summer'
                WHEN MONTH(t.order_date) IN (9, 10, 11) THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            SUM(t.quantity * p.price) AS total_sales,
            SUM(t.quantity) AS total_quantity,
            COUNT(DISTINCT t.orderID) AS total_orders
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_ID, p.Product_Category,
            CASE 
                WHEN MONTH(t.order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(t.order_date) IN (6, 7, 8) THEN 'Summer'
                WHEN MONTH(t.order_date) IN (9, 10, 11) THEN 'Fall'
                ELSE 'Winter'
            END
    )
    SELECT 
        Product_ID,
        Product_Category,
        season,
        total_sales,
        total_quantity,
        total_orders
    FROM SeasonalSales
    ORDER BY Product_Category, Product_ID, 
        CASE season
            WHEN 'Spring' THEN 1
            WHEN 'Summer' THEN 2
            WHEN 'Fall' THEN 3
            WHEN 'Winter' THEN 4
        END
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("=" * 140)
    print("Q14: SEASONAL ANALYSIS OF PRODUCT SALES")
    print("Season Definitions: Spring (Mar-May), Summer (Jun-Aug), Fall (Sep-Nov), Winter (Dec-Feb)")
    print("=" * 140)
    
    current_category = None
    current_product = None
    
    for row in results:
        product_id, category, season, sales, qty, orders = row
        
        if category != current_category:
            current_category = category
            current_product = None
            print(f"\n{'='*140}")
            print(f"PRODUCT CATEGORY: {category}")
            print(f"{'='*140}")
        
        if product_id != current_product:
            current_product = product_id
            print(f"\n  Product: {product_id}")
            print(f"  {'Season':<12} {'Orders':>12} {'Quantity':>12} {'Sales':>18}")
            print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*18}")
        
        season_icon = {"Spring": "🌸", "Summer": "☀️", "Fall": "🍂", "Winter": "❄️"}.get(season, "")
        print(f"  {season_icon} {season:<10} {orders:>12,} {qty:>12,} ${sales:>17,.2f}")
    
    # Summary: Best season by category
    print("\n\n" + "=" * 140)
    print("SUMMARY: BEST PERFORMING SEASON BY PRODUCT CATEGORY")
    print("=" * 140)
    
    best_season_query = """
    WITH SeasonalCategorySales AS (
        SELECT 
            p.Product_Category,
            CASE 
                WHEN MONTH(t.order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(t.order_date) IN (6, 7, 8) THEN 'Summer'
                WHEN MONTH(t.order_date) IN (9, 10, 11) THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            SUM(t.quantity * p.price) AS total_sales
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_Category,
            CASE 
                WHEN MONTH(t.order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(t.order_date) IN (6, 7, 8) THEN 'Summer'
                WHEN MONTH(t.order_date) IN (9, 10, 11) THEN 'Fall'
                ELSE 'Winter'
            END
    ),
    RankedSeasons AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY Product_Category ORDER BY total_sales DESC) AS rank_num
        FROM SeasonalCategorySales
    )
    SELECT Product_Category, season, total_sales
    FROM RankedSeasons
    WHERE rank_num = 1
    ORDER BY total_sales DESC
    """
    
    cur.execute(best_season_query)
    
    print(f"\n{'Category':<25} {'Best Season':<15} {'Sales':>20}")
    print(f"{'-'*25} {'-'*15} {'-'*20}")
    
    for row in cur.fetchall():
        category, season, sales = row
        season_icon = {"Spring": "🌸", "Summer": "☀️", "Fall": "🍂", "Winter": "❄️"}.get(season, "")
        print(f"{category:<25} {season_icon} {season:<12} ${sales:>19,.2f}")
    
    # Cross-tab: Category x Season
    print("\n\n" + "=" * 140)
    print("CROSS-TAB: PRODUCT CATEGORY x SEASON (TOTAL SALES)")
    print("=" * 140)
    
    crosstab_query = """
    SELECT 
        p.Product_Category,
        SUM(CASE WHEN MONTH(t.order_date) IN (3, 4, 5) THEN t.quantity * p.price ELSE 0 END) AS spring_sales,
        SUM(CASE WHEN MONTH(t.order_date) IN (6, 7, 8) THEN t.quantity * p.price ELSE 0 END) AS summer_sales,
        SUM(CASE WHEN MONTH(t.order_date) IN (9, 10, 11) THEN t.quantity * p.price ELSE 0 END) AS fall_sales,
        SUM(CASE WHEN MONTH(t.order_date) IN (12, 1, 2) THEN t.quantity * p.price ELSE 0 END) AS winter_sales,
        SUM(t.quantity * p.price) AS total_sales
    FROM transactional_data t
    JOIN product_master p ON t.Product_ID = p.Product_ID
    GROUP BY p.Product_Category
    ORDER BY total_sales DESC
    """
    
    cur.execute(crosstab_query)
    
    print(f"\n{'Category':<20} {'🌸 Spring':>18} {'☀️ Summer':>18} {'🍂 Fall':>18} {'❄️ Winter':>18} {'Total':>18}")
    print(f"{'-'*20} {'-'*18} {'-'*18} {'-'*18} {'-'*18} {'-'*18}")
    
    for row in cur.fetchall():
        category, spring, summer, fall, winter, total = row
        cat_display = category[:18] + ".." if len(str(category)) > 20 else category
        print(f"{cat_display:<20} ${float(spring):>17,.2f} ${float(summer):>17,.2f} ${float(fall):>17,.2f} ${float(winter):>17,.2f} ${float(total):>17,.2f}")
    
    print("\n" + "=" * 140)
    conn.close()

if __name__ == "__main__":
    run_query()
