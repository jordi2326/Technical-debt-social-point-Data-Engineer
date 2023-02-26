-- Please add your queries here.
--1-> revenue per user by aplication and platform
a)
select A.app_name , A.platform ,round(cast((sum(B.order_amount_gross_dollar)/count(distinct A.user_id)) as numeric),2) as revenue_average_per_user
from tx_log_user_register A
left join tx_log_monetization_transaction B on A.user_id = B.user_id
group by  A.app_name , A.platform
order by app_name
--2 ->average revenue per customer (player who has at least one transaction) by registration country
b)
select t.*
from(
	select A.ip_country,round(cast((sum(B.order_amount_gross_dollar)/count( distinct B.user_id)) as numeric),2) as revenue_usd_average_per_customer
	from tx_log_user_register A
	 join tx_log_monetization_transaction B on A.user_id = B.user_id
	group by  A.ip_country
)t
order by revenue_usd_average_per_customer desc

--3 ->We would like to know the generated revenue by register date for users registered in May 2020.
c)
select  date(A.datetime) as register_date,sum(B.order_amount_gross_dollar) as revenue_USD
from tx_log_user_register A 
left join tx_log_monetization_transaction B on A.user_id =B.user_id 
where A.datetime >= '2020-05-01' and A.datetime < '2020-06-01'
group by date(A.datetime)
order by date(A.datetime)

d)

select t.*
from (
	(select date(A.datetime) as fecha ,0 as interval , sum(B.order_amount_gross_dollar) as revenue_dollar
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id =B.user_id and( date(B.datetime)>= date(A.datetime) and  date(B.datetime) <= date(A.datetime))
	where A.datetime >= '2020-06-01' and A.datetime < '2020-07-01'
	group by date(A.datetime)
	)
	union 
	(
	select date(A.datetime) as fecha ,3 as interval , sum(B.order_amount_gross_dollar) as revenue_dollar
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id =B.user_id and( date(B.datetime) >= date(A.datetime) and  date(B.datetime) <= date(A.datetime )+ INTERVAL '3 day')
	where A.datetime >= '2020-06-01' and A.datetime < '2020-07-01'
	group by date(A.datetime)
	)
	
	union
	(
	select date(A.datetime) as fecha ,7 as interval , sum(B.order_amount_gross_dollar) as revenue_dollar
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id =B.user_id and( date(B.datetime) >= date(A.datetime) and  date(B.datetime) <= date(A.datetime )+ INTERVAL '7 day')
	where A.datetime >= '2020-06-01' and A.datetime < '2020-07-01'
	group by date(A.datetime)
	)
	union 
	(
	select date(A.datetime) as fecha ,15 as interval , sum(B.order_amount_gross_dollar) as revenue_dollar
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id =B.user_id and( date(B.datetime) >= date(A.datetime) and  date(B.datetime) <= date(A.datetime )+ INTERVAL '15 day')
	where A.datetime >= '2020-06-01' and A.datetime < '2020-07-01'
	group by date(A.datetime)
	)
	union 
	(
	select date(A.datetime) as fecha ,30 as interval , sum(B.order_amount_gross_dollar) as revenue_dollar
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id =B.user_id and( date(B.datetime) >= date(A.datetime) and  date(B.datetime) <= date(A.datetime )+ INTERVAL '30 day')
	where A.datetime >= '2020-06-01' and A.datetime < '2020-07-01'
	group by date(A.datetime)
	)
)t 
order by t.fecha asc

