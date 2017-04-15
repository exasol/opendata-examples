-- refreshing the metadata for the dataset to check for new files


--for delta-loads re-create the staging tbl and then re-load the data
create or replace table &STAGESCM..stage_raw_data_urls as
select a.*,cast(substr(data_path,7,6) as decimal(6)) as period,
substr(url,0,instr(url,'/',-1)-1) as site_url,
substr(url,instr(url,'/',-1)+1) as file_name
 from (
select &STAGESCM..json_parsing_datapackage('https://data.gov.uk/dataset/prescribing-by-gp-practice-presentation-level/datapackage.json')) a
where data_format ='CSV' and DESCRIPTION not like 'Sample%' order by 1
;

merge into &STAGESCM..raw_data_urls tgt using &STAGESCM..stage_raw_data_urls src on tgt.data_path = src.data_path when not matched then insert
(description, data_format,data_path,site_url,file_name,period) values (description,data_format,data_path,site_url,file_name,period);


--seems to be an error in the source data
update &STAGESCM..raw_data_urls set FILE_NAME='T201009CHEM%20SUBS.CSV' where PERIOD='201009' and DESCRIPTION='Chemical names and BNF codes';
update &STAGESCM..raw_data_urls set FILE_NAME='T201103ADDR%20BNFT.CSV' where PERIOD='201103' and DESCRIPTION='Practice codes, names and addresses';


EXECUTE SCRIPT &STAGESCM..PRESCRIPTIONS_LOAD();

select * from &STAGESCM..JOB_DETAILS order by run_id desc,detail_id desc;
