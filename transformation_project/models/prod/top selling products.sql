-- models/top_selling_products.sql

with product_sales as (
    select
        p."productId",
        p."productName",
        sum(od."unitPrice" * od."quantity") as total_sales
    from
        {{ ref('stgordersdetails') }} od
    join
        {{ ref('stgproducts') }} p on od."productID" = p."productId"
    group by
        1, 2
)

select
    product_sales."productId",
    product_sales."productName",
    product_sales."total_sales",
    rank() over (order by total_sales desc) as sales_rank
from
    product_sales
