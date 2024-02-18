select
        date_trunc('month', o."orderDate"::date) as order_month,
        count(distinct o."customerId") as active_customers
    from
        {{ ref('stgorders') }} o
    group by
        1
