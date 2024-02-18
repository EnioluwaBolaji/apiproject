-- models/retention.sql

-- Calculate the number of customers who made repeat purchases in each subsequent month
with retention_data as (
    select
        c."customerId",
        date_trunc('month', o."orderDate"::date) as order_month,
        count(distinct o."orderId") as repeat_purchases
    from
        {{ ref('stgorders') }} o
    join
        {{ ref('cohort') }} c on o."customerId" = c."customerId"
    group by
        1,2
)

select * from retention_data