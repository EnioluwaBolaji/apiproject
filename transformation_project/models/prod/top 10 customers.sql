SELECT 
    sc."contactName" AS customerName,
    COUNT(o."orderId") AS numberofpurchases
FROM 
    {{ ref('stgorders') }}  o
LEFT JOIN 
       {{ ref('stgcustomers') }} sc
ON 
    o."customerId"  = sc."customerId"
GROUP BY 1
ORDER BY 2 desc
LIMIT 10    