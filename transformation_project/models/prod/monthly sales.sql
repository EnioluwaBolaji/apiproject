SELECT
        date_trunc('month', o."orderDate"::date) as order_month,
        sum(od."unitPrice" * od."quantity") as total_revenue
    from
        {{ ref('stgorders') }} o
    join
        {{ ref('stgordersdetails') }} od on o."orderId" = od."orderID"
    group by
        1 
