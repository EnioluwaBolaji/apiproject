-- models/cohort.sql

with cohort as (
    select
        "customerId",
        date_trunc('month', min("orderDate"::date)) as first_purchase_month
    from
        {{ ref('stgorders') }}
    group by
        1
)

select * from cohort
