
with yearly_sales as (
    select
        extract(year from o."orderDate"::date) as order_year,
        sum(od."unitPrice" * od."quantity") as total_sales
    from
        {{ ref('stgorders') }} o
    join
        {{ ref('stgordersdetails') }} od on o."orderId" = od."orderID"
    group by
        1
)

select
    current_year.order_year as current_year,
    current_year.total_sales as current_year_sales,
    previous_year.total_sales as previous_year_sales,
    (current_year.total_sales - previous_year.total_sales) / previous_year.total_sales * 100 as yoy_growth_rate
from
    yearly_sales current_year
left join
    yearly_sales previous_year on current_year.order_year = previous_year.order_year + 1
