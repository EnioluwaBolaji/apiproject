-- models/sales_growth_rate.sql


with monthly_sales as (
    select
        date_trunc('month', o."orderDate"::date) as order_month,
        sum(od."unitPrice" * od."quantity") as total_revenue
    from
        {{ ref('stgorders') }} o
    join
        {{ ref('stgordersdetails') }} od on o."orderId" = od."orderID"
    group by
        1
),


previous_month_sales as (
    select
        order_month,
        total_revenue,
        lag(total_revenue) over (order by order_month) as previous_month_sales
    from
        monthly_sales
)


select
    order_month,
     total_revenue,
    previous_month_sales,
    case
        when previous_month_sales = 0 then null
        else (total_revenue - previous_month_sales) / previous_month_sales * 100
    end as sales_growth_rate
from
    previous_month_sales
