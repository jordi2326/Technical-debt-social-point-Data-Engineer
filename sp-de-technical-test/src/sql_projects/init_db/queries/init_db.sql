drop schema if exists sp_technical_test cascade;

create schema sp_technical_test;

set search_path to sp_technical_test;

create table sp_test as
select now() as created_at, current_database() as database, current_schema() as schema;

create table sp_technical_test.log_user_register as 
select * from public.log_user_register