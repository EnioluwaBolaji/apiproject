-- models/employee_performance.sql

SELECT
    sE."employeeId",
    sE."firstName",
    sE."lastName",
    sE."firstName" || ' ' || sE."lastName" AS EmployeeName,
    COUNT(DISTINCT o."orderId") AS TotalOrders,
    ROUND(SUM(sP."unitPrice" * od.quantity)) AS TotalProfit
FROM
    {{ ref('stgemployees') }} sE
JOIN
    {{ ref('stgorders') }} o
ON
    o."employeeId" = sE."employeeId"
JOIN
    {{ ref('stgordersdetails') }} od
ON
    o."orderId" = od."orderID"
JOIN
    {{ ref('stgproducts') }} sP
ON
    sP."productId" = od."productID"
GROUP BY
    1, 2, 3, 4
ORDER BY
    TotalProfit DESC
