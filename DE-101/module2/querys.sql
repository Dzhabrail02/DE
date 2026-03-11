-- 1 
select segment, sum(o.profit)
from staging.orders o 
group by 1 

select segment, round(sum(o.profit)/sum(o.sales)*100, 2)
from staging.orders o 
group by 1 

select order_id, round(sum(o.profit)/sum(o.sales)*100, 2)
from staging.orders o 
group by 1 

select o.customer_name, sum(o.sales)
from staging.orders o 
group by 1 

select to_char(order_date,'YYYY-MM'), segment, sum(sales)
from staging.orders 
group by 1, 2

select to_char(order_date,'YYYY-MM'), product_name, sum(sales)
from staging.orders 
group by 1, 2

--2
select category, round(sum(sales))
from staging.orders 
group by 1

--3 
select 
customer_name, round(sum(sales)), round(sum(profit))
from staging.orders
group by 1

with dd as (
select 
customer_name, round(sum(sales)) as sales, round(sum(profit)) as profit
from staging.orders
group by 1
)
select *, row_number() over(order by sales desc), row_number() over(order by profit desc)
from dd

select region, round(sum(sales))
from staging.orders o 
group by region
