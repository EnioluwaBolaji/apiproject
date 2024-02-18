-- models/employee_retention.sql

with employee_retention as (
    select
        date_trunc('month', e."hireDate"::date) as hire_date,
        count(distinct e."employeeId") as employees_retained
    from
        {{ ref('stgemployees') }} e
    left join
        {{ ref('stgorders') }} o on e."employeeId" = o."employeeId"
    where
        o."orderId" is not null  
    group by
        1
)

-- Calculate the employee retention rate
select
    er.hire_date,
    er.employees_retained,
    count(distinct e."employeeId") as total_employees,
    (er.employees_retained / count(distinct e."employeeId")) * 100 as retention_rate
from
    employee_retention er
cross join
    {{ ref('stgemployees') }} e
group by
    1,2
order by
    1
