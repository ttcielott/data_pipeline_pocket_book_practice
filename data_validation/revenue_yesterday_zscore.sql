WITH revenue_by_day AS (
    SELECT CAST(OrderDate AS DATE) AS order_date,
            SUM(OrderTotal) AS revenue_total
    FROM Orders
    GROUP BY CAST(OrderDate AS DATE)
),
daily_revenue_zscore AS (
    SELECT order_date,
            revenue_total,
            (revenue_total - AVG(revenue_total) OVER())
            / (stddev(revenue_total) OVER()) AS revenue_zscore
    FROM revenue_by_day
)
SELECT ABS(revenue_zscore) as twosided_score
FROM daily_revenue_zscore
-- WHERE order_date = 
--         CAST(current_timestamp) - interval '1 day' 
WHERE order_date = '2020-07-12';