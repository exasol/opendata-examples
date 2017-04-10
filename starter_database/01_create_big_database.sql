drop schema start_big cascade;
create schema start_big;
-- this big version has around 200 million sales transactions, 100,000 sales agents, 10,000 sales territories 
-- it can take up to 10 minutes to build if run on a laptop   

create table sales as
select cast(trunc(random(1,1000000)) as int) sales_agent_id, 
current_date-random()*1000 sales_date, 
cast(trunc(RANDOM(1, 10000),2) as decimal(10,2)) software_sales,
cast(trunc(RANDOM(1, 1000),2) as decimal (10,2)) services_sales,
trunc(RANDOM(10,30),0) sales_commission_percent
from (select row_number() over (order by 1)
from exa_time_zones a, exa_time_zones b, exa_time_zones c);

create table sales_agent 
(sales_agent_id integer identity,
sales_agent_name varchar(100) default 'Unknown',
sales_territory_id integer);

insert into sales_agent(sales_territory_id) 
(select cast(trunc(random(1,10000)) as int) sales_territory_id 
from (select row_number() over (order by 1)
from exa_time_zones, exa_time_zones)
limit 100000);

create table sales_territory 
(sales_territory_id integer identity,
sales_territory_name varchar(100) default 'Unknown',
country_id integer);


insert into sales_territory(country_id) 
(select cast(trunc(random(1,620)) as int) country_id 
from (select row_number() over (order by 1)
from exa_time_zones a, exa_time_zones b)
limit 10000);


CREATE TABLE COUNTRY (
    COUNTRY_ID        DECIMAL(18,0),
    COUNTRY_NAME      VARCHAR(100) UTF8,
    ANTHEM_NAME       VARCHAR(2000) UTF8,
    CAPITAL_CITY      VARCHAR(2000) UTF8,
    CONTINENT_NAME    VARCHAR(2000) UTF8,
    COUNTRY_LONG_NAME VARCHAR(2000) UTF8,
    NATIONALITY_NAME  VARCHAR(2000) UTF8,
    USA_COMPARISON    VARCHAR(2000) UTF8
);

