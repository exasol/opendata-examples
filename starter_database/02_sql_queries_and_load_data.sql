--
-- 01. Basic SQL
-- Top 10 software sales agents in last 30 days
--
select sales_agent_id, sum(software_sales), count(*) number_of_sales
from sales
where sales_date > current_date-30
group by sales_agent_id
order by 2 desc 
limit 10;

--
-- 02. Joining tables
-- Worst countries in last 30 days
--
select country_id, sum(software_sales) software_sales, count(*) number_of_sales
from sales s join sales_agent a on (s.sales_agent_id=a.sales_agent_id)
join sales_territory t on (a.sales_territory_id=t.sales_territory_id)
where sales_date > current_date-30
group by 1
order by 2
limit 10;

--
-- NOW LOAD SOME DATA to the country table so that we have a country name on the report
--

--
-- First, download the country.sql file, save it to a suitable location
-- and change the directory name in the following script accordingly 
--

IMPORT INTO COUNTRY FROM LOCAL CSV FILE 'C:\temp\country.csv' 
ENCODING = 'UTF-8' 
ROW SEPARATOR = 'CRLF' 
COLUMN SEPARATOR = ',' 
COLUMN DELIMITER = '''' 
SKIP = 0 
REJECT LIMIT 0;

--
-- Now you can run the query again - this time including the country name
--

select country_name, sum(software_sales) software_sales, count(*) number_of_sales
from sales s join sales_agent a on (s.sales_agent_id=a.sales_agent_id)
join sales_territory t on (a.sales_territory_id=t.sales_territory_id)
join country c on (c.country_id=t.country_id)
where sales_date > current_date-30
group by 1
order by 2
limit 10;


--
-- 03. Using an outer join 
-- Which sales agents have made no sales in last month
--
select a.* from 
(select * from sales s where sales_date > current_date - interval '1' month) s
right outer join 
sales_agent a 
on (s.sales_agent_id=a.sales_agent_id)
where software_sales is NULL;

---
--- 04. HAVING clause and date arithmetic and NULL handling 
--- How long has it been since the last sale for these sales agents
--- Some agents may have never made any sales  - so we need to cope with NULL values 
---
select a.sales_agent_id, current_date-max(nvl(sales_date,current_date-9999)) days_since_last_sale
from 
sales s 
right outer join 
sales_agent a 
on (s.sales_agent_id=a.sales_agent_id)
group by 1
having current_date-max(nvl(sales_date,current_date-9999)) >30
order by 2 desc;

