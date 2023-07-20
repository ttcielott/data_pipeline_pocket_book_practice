WITH orders_by_day AS (
    SELECT CAST(OrderDate AS DATE) AS order_date,
            COUNT(*) AS order_count
    FROM Orders
    GROUP BY CAST(OrderDate AS DATE)
),
order_count_zscore AS(
    SELECT order_date,
           order_count, 
           (order_count - AVG(order_count) OVER())
           / (stddev(order_count) OVER()) AS z_score  
    FROM orders_by_day
)
-- get the z score of yesterday's order count
SELECT ABS(z_score) as twosided_score
FROM order_count_zscore
-- WHERE order_date = 
--         CAST(current_timestamp AS DATE) - interval '1 day'
WHERE order_date = '2020-07-12';