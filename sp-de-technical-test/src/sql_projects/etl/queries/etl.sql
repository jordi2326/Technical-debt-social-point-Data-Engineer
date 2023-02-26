-- You can add here as many queries as you like. :)

-- For example:

--Drop tables taux and final table if execute more than one time the process

drop table if exists taux_log_user_register;
drop table if exists taux_log_user_register2;
drop table if exists taux_log_monetization_transaction;
drop table if exists taux_log_monetization_transaction2;
drop table if exists taux_log_monetization_transaction_orderamountdollar;
drop table if exists tx_final_kpi_view;



--Delete duplicate value by user_id if exists diferent register date  , user_id is a unique value then only one row for user_id . When a user_id is more than one time it's select the minimum datetime register
create table taux_log_user_register as(
    select lur.client_mobile_device ,lur.client_mobile_os ,lur.datetime ,lur.ip_country ,lower(platform) as platform,lur.user_id,lur.version,lur.app_code,lur.app_name
    from log_user_register lur 
    join (  select user_id,min(datetime) as fecha 
            from sp_technical_test.log_user_register lur 
            group by user_id 
	) B on lur.user_id =B.user_id and lur.datetime = B.fecha 
    where lur.user_id is not null or lur.user_id != '');

-- Unique row for taux_log_user_register2
create table taux_log_user_register2 as(
    select lur.client_mobile_device ,lur.client_mobile_os ,lur.datetime ,lur.ip_country , platform,lur.user_id,lur.version,lur.app_code,lur.app_name
    from taux_log_user_register lur 
    group by lur.client_mobile_device ,lur.client_mobile_os ,lur.datetime ,lur.ip_country , platform,lur.user_id,lur.version,lur.app_code,lur.app_name
);  

--Transform currency unknown to USD and evalute only order_transaction_id informate with user_id asociate
create table taux_log_monetization_transaction as(
	select datetime, game_basic_level,ip_country,order_amount_gross,order_payment_provider,order_transaction_id,lower(platform) as platform,A.user_id,A.version,app_code,app_name,
	case when currency= 'unknown' then 'USD' else currency end as currency  
	from log_monetization_transaction  A 
    where (user_id is not null or user_id != '' ) and (order_transaction_id is not null or order_transaction_id != ''));


-- Delete the duplicate transaction and making some colaesce for controling null values like user_id and platform 
--In this ETL process, if it finds two same  order_transaction_id with the same values and diferent order_amount_gross then it's going to sum the values and transform in only one row because order_transaction_id is going to be primary key in the final table 
create table taux_log_monetization_transaction2 as(
    select A.datetime,A.game_basic_level,A.ip_country,sum(A.order_amount_gross) as order_amount_gross ,A.order_payment_provider ,A.order_transaction_id,A.platform,A.user_id,A.version,A.app_code,A.app_name,A.currency
	from (
		select tlmt.datetime , tlmt.game_basic_level,tlmt.ip_country , tlmt.order_amount_gross,tlmt.order_payment_provider  ,tlmt.order_transaction_id, case when tlmt.platform = '' then lower(tlur.platform) else lower(tlmt.platform) end as platform ,tlmt.user_id, tlmt.version,tlmt.app_code,tlmt.app_name , tlmt.currency
		from taux_log_monetization_transaction tlmt 
		left join taux_log_user_register2 tlur on tlmt.user_id = tlur.user_id
	)A
	group by A.datetime,A.game_basic_level,A.ip_country, A.order_transaction_id,A.order_payment_provider,A.platform,A.user_id,A.version,A.app_code,A.app_name,A.currency
);
--Transform all order_amount_gross to dollars 
create table taux_log_monetization_transaction_orderamountdollar as (
    select  A.datetime,A.game_basic_level,A.ip_country, order_amount_gross ,round(A.order_amount_gross/A.exchange_rate,2) as  order_amount_gross_dollar ,A.order_payment_provider ,A.order_transaction_id,A.platform,A.user_id,A.version,A.app_code,A.app_name,A.currency
    from(
        select A.datetime,A.game_basic_level,A.ip_country,round( CAST( A.order_amount_gross as numeric), 2) as order_amount_gross ,A.order_payment_provider ,A.order_transaction_id,A.platform,A.user_id,A.version,A.app_code,A.app_name,A.currency,round( CAST(cr.exchange_rate as numeric), 2) as  exchange_rate
        from sp_technical_test.taux_log_monetization_transaction2 A
        left join change_rate cr on cr.country = A.currency 	and cr.fecha = date(A.datetime)
    )A

);

--Creating the stagging tables of the 2 tables with clean data and  without duplicate values , also define primary key  and foreign key 
-- tx_log_user_register is the finall table result. It  is going to create with user_id as a primary key, in this table is going yo have only one register per user_id, user_id must be exist to insert in the table then null values 
create table if not exists tx_log_user_register (
    client_mobile_device varchar (64),
    client_mobile_os varchar(64),
    datetime timestamp,
    ip_country char(2),
    platform varchar(8),	
    user_id varchar(64) PRIMARY KEY   ,	
    version varchar(32),	
    app_code varchar(10),
    app_name varchar(64) 
);

--tx_log_monetization_transaction
--The only diference between this table and the other it is order_amount_gross_dollar that contain amount in dollars
create table if not exists tx_log_monetization_transaction(
    datetime timestamp,
    game_basic_level smallint,
    ip_country char(2),
    order_amount_gross double precision,
    order_amount_gross_dollar double precision,
    order_payment_provider varchar(16),
    order_transaction_id varchar(255) PRIMARY KEY ,
    platform varchar(8),
    user_id varchar(64) ,
    version varchar(32),
    app_code varchar(10),
    app_name varchar (64),
    currency varchar(10),
    CONSTRAINT fk_user_id
    FOREIGN KEY (user_id) REFERENCES tx_log_user_register(user_id)
);


--In  this stept one of the last one , the objective is only insert user that we don't have in the final table tx_log_user_register.
--To achive the objective , the taux table compare the data from tx_log_user_registration. In this process find the new user_id that it is going to insert.
-- When the new user_id finf then it is going to make a join with the taux_log_user_register2 to obtain all information of new user and insert to the final table 

insert into tx_log_user_register(
select t.*
from(
	select user_id from taux_log_user_register2
	except
	select user_id from tx_log_user_register
)A
join taux_log_user_register2 t on t.user_id = A.user_id);

--The same process than before, identify the new transaction that it is going to insert and making the join with the taux table to find all the information 
-- and insert to tx_log_monetization_transaction to have the final data

INSERT INTO tx_log_monetization_transaction(
select t.*
from(
	(select  A.order_transaction_id,A.user_id from  taux_log_monetization_transaction_orderamountdollar A)
	EXCEPT 
	(select A.order_transaction_id,A.user_id  from tx_log_monetization_transaction A)
)A
join taux_log_monetization_transaction_orderamountdollar t on t.user_id = A.user_id and t.order_transaction_id = A.order_transaction_id
);


--Using tx_log_monetization_transaction and tx_log_user_register the final table with clean data , it is going to make the calculation and insert in the table to check de information

create table tx_final_kpi_view as (
	select count (distinct A.user_id) as number_player,	ROUND(CAST(sum(b.order_amount_gross_dollar) AS numeric),2) as revenue, count(distinct B.user_id) as number_customers, 	ROUND(CAST((sum(b.order_amount_gross_dollar)/count (distinct A.user_id)) AS numeric),4)
	 as revenues_per_user,	count(B.order_transaction_id) as number_of_transaction , round( (cast(count(B.order_transaction_id) as numeric )/cast(count (distinct A.user_id) AS numeric)),4)  as Average_of_transactions_per_user
	from tx_log_user_register A 
	left join tx_log_monetization_transaction B on A.user_id = B.user_id
)
