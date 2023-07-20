WITH order_dups AS
(
    SELECT OrderId, COUNT(*)
    FROM Orders
    GROUP BY OrderId
    HAVING COUNT(*) > 1
)
SELECT COUNT(*)
FROM order_dups;