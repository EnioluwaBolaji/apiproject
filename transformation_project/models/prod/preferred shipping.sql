SELECT
od."shipVia" AS ShipPingID,
sh."companyName" AS Shipping_method,
COUNT (od."orderId") AS total_no_of_orders
FROM
staging."stgorders" AS od
LEFT JOIN
staging."stgshippers" as sh
ON
od."shipVia"= sh."shipperId"
GROUP BY 1,2
ORDER BY 3 desc