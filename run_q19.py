"""
Q19: Identify High Revenue Spikes in Product Sales and Highlight Outliers
Calculate daily average sales for each product and flag days where the sales exceed twice
the daily average by product as potential outliers or spikes. Explain any identified anomalies
in the report, as these may indicate unusual demand events.
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
    
    print("=" * 160)
    print("Q19: HIGH REVENUE SPIKES AND OUTLIER DETECTION")
    print("=" * 160)
    print("\nMethodology: Flag days where product sales exceed 2x the product's daily average")
    print("These spikes may indicate: promotions, seasonal events, inventory issues, or unusual demand\n")
    
    # Main query: Identify revenue spikes (>2x daily average)
    query = """
    WITH DailySales AS (
        SELECT 
            p.Product_ID,
            p.Product_Category,
            t.order_date,
            DAYNAME(t.order_date) AS day_of_week,
            SUM(t.quantity * p.price) AS daily_revenue,
            SUM(t.quantity) AS daily_quantity,
            COUNT(DISTINCT t.orderID) AS daily_orders
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_ID, p.Product_Category, t.order_date
    ),
    ProductStats AS (
        SELECT 
            Product_ID,
            AVG(daily_revenue) AS avg_daily_revenue,
            STDDEV(daily_revenue) AS stddev_revenue,
            COUNT(*) AS days_with_sales
        FROM DailySales
        GROUP BY Product_ID
    ),
    SpikeDetection AS (
        SELECT 
            ds.Product_ID,
            ds.Product_Category,
            ds.order_date,
            ds.day_of_week,
            ds.daily_revenue,
            ds.daily_quantity,
            ds.daily_orders,
            ps.avg_daily_revenue,
            ps.stddev_revenue,
            ROUND(ds.daily_revenue / ps.avg_daily_revenue, 2) AS multiple_of_avg,
            CASE 
                WHEN ds.daily_revenue > ps.avg_daily_revenue * 3 THEN 'EXTREME SPIKE (3x+)'
                WHEN ds.daily_revenue > ps.avg_daily_revenue * 2 THEN 'HIGH SPIKE (2x-3x)'
                ELSE 'Normal'
            END AS spike_classification
        FROM DailySales ds
        JOIN ProductStats ps ON ds.Product_ID = ps.Product_ID
        WHERE ps.days_with_sales >= 5  -- Only products with enough data
    )
    SELECT *
    FROM SpikeDetection
    WHERE spike_classification != 'Normal'
    ORDER BY multiple_of_avg DESC
    LIMIT 50
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    print("-" * 160)
    print("TOP 50 REVENUE SPIKES DETECTED")
    print("-" * 160)
    print(f"\n{'Product':<15} {'Category':<15} {'Date':<12} {'Day':<12} {'Revenue':>15} {'Avg Revenue':>15} {'Multiple':>10} {'Classification':>20}")
    print(f"{'-'*15} {'-'*15} {'-'*12} {'-'*12} {'-'*15} {'-'*15} {'-'*10} {'-'*20}")
    
    extreme_spikes = []
    high_spikes = []
    
    for row in results:
        product, category, date, day, revenue, qty, orders, avg_rev, stddev, multiple, classification = row
        
        cat_display = category[:13] + ".." if len(str(category)) > 15 else category
        date_str = str(date) if date else "N/A"
        
        if "EXTREME" in classification:
            extreme_spikes.append((product, category, date, revenue, multiple))
            indicator = "⚠️ "
        else:
            high_spikes.append((product, category, date, revenue, multiple))
            indicator = "📈 "
        
        print(f"{indicator}{product:<13} {cat_display:<15} {date_str:<12} {day:<12} ${revenue:>14,.2f} ${avg_rev:>14,.2f} {multiple:>9.2f}x {classification:>20}")
    
    # Summary by product
    print("\n\n" + "=" * 160)
    print("PRODUCTS WITH MOST FREQUENT SPIKES")
    print("=" * 160)
    
    freq_query = """
    WITH DailySales AS (
        SELECT 
            p.Product_ID,
            p.Product_Category,
            t.order_date,
            SUM(t.quantity * p.price) AS daily_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_ID, p.Product_Category, t.order_date
    ),
    ProductStats AS (
        SELECT 
            Product_ID,
            AVG(daily_revenue) AS avg_daily_revenue,
            COUNT(*) AS days_with_sales
        FROM DailySales
        GROUP BY Product_ID
        HAVING COUNT(*) >= 5
    ),
    SpikeCount AS (
        SELECT 
            ds.Product_ID,
            ds.Product_Category,
            ps.avg_daily_revenue,
            ps.days_with_sales,
            SUM(CASE WHEN ds.daily_revenue > ps.avg_daily_revenue * 2 THEN 1 ELSE 0 END) AS spike_count,
            MAX(ds.daily_revenue) AS max_daily_revenue
        FROM DailySales ds
        JOIN ProductStats ps ON ds.Product_ID = ps.Product_ID
        GROUP BY ds.Product_ID, ds.Product_Category, ps.avg_daily_revenue, ps.days_with_sales
    )
    SELECT *
    FROM SpikeCount
    WHERE spike_count > 0
    ORDER BY spike_count DESC
    LIMIT 20
    """
    
    cur.execute(freq_query)
    
    print(f"\n{'Product':<15} {'Category':<20} {'Days Active':>12} {'Spike Days':>12} {'Spike %':>10} {'Avg Revenue':>16} {'Max Revenue':>16}")
    print(f"{'-'*15} {'-'*20} {'-'*12} {'-'*12} {'-'*10} {'-'*16} {'-'*16}")
    
    for row in cur.fetchall():
        product, category, avg_rev, days, spikes, max_rev = row
        spike_pct = (spikes / days * 100) if days > 0 else 0
        cat_display = category[:18] + ".." if len(str(category)) > 20 else category
        print(f"{product:<15} {cat_display:<20} {days:>12} {spikes:>12} {spike_pct:>9.1f}% ${avg_rev:>15,.2f} ${max_rev:>15,.2f}")
    
    # Day of week analysis
    print("\n\n" + "=" * 160)
    print("SPIKE DISTRIBUTION BY DAY OF WEEK")
    print("=" * 160)
    
    dow_query = """
    WITH DailySales AS (
        SELECT 
            p.Product_ID,
            t.order_date,
            DAYNAME(t.order_date) AS day_of_week,
            SUM(t.quantity * p.price) AS daily_revenue
        FROM transactional_data t
        JOIN product_master p ON t.Product_ID = p.Product_ID
        GROUP BY p.Product_ID, t.order_date
    ),
    ProductStats AS (
        SELECT Product_ID, AVG(daily_revenue) AS avg_daily_revenue
        FROM DailySales
        GROUP BY Product_ID
        HAVING COUNT(*) >= 5
    )
    SELECT 
        ds.day_of_week,
        COUNT(*) AS total_days,
        SUM(CASE WHEN ds.daily_revenue > ps.avg_daily_revenue * 2 THEN 1 ELSE 0 END) AS spike_days,
        ROUND(SUM(CASE WHEN ds.daily_revenue > ps.avg_daily_revenue * 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS spike_pct
    FROM DailySales ds
    JOIN ProductStats ps ON ds.Product_ID = ps.Product_ID
    GROUP BY ds.day_of_week
    ORDER BY FIELD(ds.day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    """
    
    cur.execute(dow_query)
    
    print(f"\n{'Day':<15} {'Total Days':>15} {'Spike Days':>15} {'Spike %':>15}")
    print(f"{'-'*15} {'-'*15} {'-'*15} {'-'*15}")
    
    for row in cur.fetchall():
        day, total, spikes, pct = row
        bar = "█" * int(pct) if pct else ""
        print(f"{day:<15} {total:>15,} {spikes:>15,} {pct:>14.2f}% {bar}")
    
    # Analysis explanation
    print("\n\n" + "=" * 160)
    print("ANOMALY ANALYSIS & INSIGHTS")
    print("=" * 160)
    
    print("\n📊 SPIKE ANALYSIS SUMMARY:")
    print(f"   • Total extreme spikes (3x+ average): {len(extreme_spikes)}")
    print(f"   • Total high spikes (2x-3x average): {len(high_spikes)}")
    
    if extreme_spikes:
        print("\n⚠️ EXTREME SPIKE EVENTS (Require Investigation):")
        for product, category, date, revenue, multiple in extreme_spikes[:5]:
            print(f"   • {product} ({category}): ${revenue:,.2f} on {date} ({multiple}x normal)")
    
    print("\n🔍 POSSIBLE CAUSES FOR SPIKES:")
    print("   1. Promotional campaigns or sales events")
    print("   2. Seasonal demand patterns")
    print("   3. Stock replenishment after shortages")
    print("   4. Viral/trending products")
    print("   5. Bulk orders from B2B customers")
    print("   6. Data entry errors (should be verified)")
    
    print("\n" + "=" * 160)
    conn.close()

if __name__ == "__main__":
    run_query()
