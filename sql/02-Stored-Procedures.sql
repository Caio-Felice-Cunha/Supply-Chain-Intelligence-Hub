-- ============ ADVANCED STORED PROCEDURES ============

-- 1. Calculate Inventory Health Score (CTE + Window Functions)
DELIMITER //

CREATE PROCEDURE sp_inventory_health_analysis() 
-- This procedure functions as a stock-optimization engine. It identifies which products are at risk of running out and which are tying up capital.
BEGIN
    WITH inventory_stats AS (
        SELECT 
            p.product_id,
            p.product_name,
            SUM(i.quantity_on_hand) as total_stock,
            AVG(s.quantity_sold) as avg_daily_sales,
            MAX(s.sale_date) as last_sale_date,
            ROW_NUMBER() OVER (ORDER BY SUM(i.quantity_on_hand) DESC) as stock_rank,
            PERCENT_RANK() OVER (ORDER BY AVG(s.quantity_sold)) as sales_percentile
        FROM products p
        JOIN inventory i ON p.product_id = i.product_id
        LEFT JOIN sales s ON p.product_id = s.product_id
        -- WHERE i.snapshot_date = CURDATE()
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)
        GROUP BY p.product_id, p.product_name
    )
    SELECT 
        product_id,
        product_name,
        total_stock,
        avg_daily_sales,
        ROUND(total_stock / GREATEST(avg_daily_sales, 1), 2) as days_of_supply,
        CASE 
            WHEN total_stock / GREATEST(avg_daily_sales, 1) < 7 THEN 'CRITICAL'
            WHEN total_stock / GREATEST(avg_daily_sales, 1) < 30 THEN 'LOW'
            WHEN total_stock / GREATEST(avg_daily_sales, 1) < 90 THEN 'OPTIMAL'
            ELSE 'EXCESS'
        END as stock_health,
        stock_rank,
        ROUND(sales_percentile * 100, 2) as sales_percentile
    FROM inventory_stats
    ORDER BY stock_rank;
END //

DELIMITER ;

-- 2. On-Time Delivery Rate by Supplier
DELIMITER //

CREATE PROCEDURE sp_supplier_delivery_performance()
-- This is a scorecard for your supply chain. It moves past "how much did we buy" to "how reliable is this partner?"
BEGIN
    SELECT 
        s.supplier_id,
        s.supplier_name,
        COUNT(o.order_id) as total_orders,
        SUM(CASE WHEN o.actual_delivery_date <= o.expected_delivery_date THEN 1 ELSE 0 END) as on_time_orders,
        ROUND(
            SUM(CASE WHEN o.actual_delivery_date <= o.expected_delivery_date THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id),
            2
        ) as on_time_pct,
        ROUND(AVG(DATEDIFF(o.actual_delivery_date, o.expected_delivery_date)), 2) as avg_days_late,
        s.reliability_score
    FROM suppliers s
    LEFT JOIN orders o ON s.supplier_id = o.supplier_id
    WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    GROUP BY s.supplier_id, s.supplier_name, s.reliability_score
    HAVING total_orders > 0
    ORDER BY on_time_pct DESC;
END //

DELIMITER ;

-- 3. Rolling Sales and Forecast
DELIMITER //

CREATE PROCEDURE sp_sales_rolling_analysis()
-- This is the most technically complex of the three, focusing on Time-Series Analysis to smooth out daily fluctuations.
BEGIN
    WITH daily_sales AS (
        SELECT 
            sale_date,
            product_id,
            SUM(quantity_sold) as daily_quantity,
            SUM(revenue) as daily_revenue
        FROM sales
        WHERE sale_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
        GROUP BY sale_date, product_id
    ),
    rolling_stats AS (
        SELECT 
            sale_date,
            product_id,
            daily_quantity,
            daily_revenue,
            AVG(daily_quantity) OVER (
                PARTITION BY product_id 
                ORDER BY sale_date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as moving_avg_30day,
            AVG(daily_quantity) OVER (
                PARTITION BY product_id 
                ORDER BY sale_date 
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) as moving_avg_90day,
            STDDEV(daily_quantity) OVER (
                PARTITION BY product_id 
                ORDER BY sale_date 
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) as volatility_90day
        FROM daily_sales
    )
    SELECT 
        sale_date,
        product_id,
        daily_quantity,
        daily_revenue,
        moving_avg_30day,
        moving_avg_90day,
        volatility_90day,
        ROUND(moving_avg_30day * 1.2, 0) as conservative_forecast
    FROM rolling_stats
    WHERE sale_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    ORDER BY product_id, sale_date DESC;
END //

DELIMITER ;
