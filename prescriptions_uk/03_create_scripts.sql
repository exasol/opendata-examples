--script for parsing site data and file URLs
--/
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT "PRESCRIPTIONS_UK_STAGE"."JSON_PARSING_DATAPACKAGE" () EMITS ("DESCRIPTION" VARCHAR(100) UTF8,"PERIOD" VARCHAR(20), "URL" VARCHAR(100) UTF8) AS
import requests
import json
import re
import time

counters = [3]
json_dict = {}


def check_retry_count(counter):
    if counters[0] > 0:
        counter[0] -= 1
        return True
    else:
        raise requests.exceptions.RequestException


def run(ctx):
    url = r"https://data.gov.uk/dataset/prescribing-by-gp-practice-presentation-level"
    expression = r'{"@.+]}'

    while True:
        try:
            site_content = requests.get(url).text
            filtered = re.findall(expression, site_content)[0]
            json_dict = json.loads(filtered)
            break
        except (json.JSONDecodeError, re.error) as errj:
            ctx.emit("Site JSON is malformed, trying again.\n" + str(errj), "Null", "Null")
            if check_retry_count(counters):
                time.sleep(3)
                continue
        except requests.exceptions.HTTPError as errh:
            ctx.emit("HTTP Error occurred, trying again: \n" + str(errh), "Null", "Null")
            if check_retry_count(counters):
                time.sleep(3)
                continue
        except requests.exceptions.ConnectionError as errc:
            ctx.emit("Site could not be reached, trying again: \n" + str(errc), "Null", "Null")
            if check_retry_count(counters):
                time.sleep(3)
                continue
        except requests.exceptions.Timeout as errt:
            ctx.emit("Request timed out, trying again: \n" + str(errt), "Null", "Null")
            if check_retry_count(counters):
                time.sleep(3)
                continue
        except requests.exceptions.RequestException as err:
            ctx.emit("Undefined exception occurred that is NOT\n"
                  "-JSON error\n"
                  "-HTTP error\n"
                  "-Connection error\n"
                  "-Timeout\n"
                  "OR maximum amount of retries was reached.\n", "Null", "Null")

    for e in json_dict.get("distribution"):
        if e.get("fileFormat") in ("CSV/ZIP", "ZIP"):
            continue
        description = e.get("name")
        period = re.findall(r"\w+ \w+", e.get("name"))[0]
        url = e.get("contentUrl")
        ctx.emit(description, period, url)
/
--
----load script (executing this again will only load new data if metadata has been refreshed)
--CREATE OR REPLACE LUA SCRIPT PRESCRIPTIONS_UK_STAGE.PRESCRIPTIONS_LOAD RETURNS TABLE AS
--import('SCRIPTING.QUERY_WRAPPER','QW')
--
--function process_file(PERIOD,SITE_URL,FILE_NAME)
--        wrapper:set_param('SITE_URL',SITE_URL)
--	wrapper:set_param('PERIOD',PERIOD)
--	wrapper:set_param('FILE_NAME',FILE_NAME)
--	--cleanup to ensure there is no data left in case of an earlier failed run
--	wrapper:query([[DELETE FROM ::STAGE_SCM.::STAGE_TBL where PERIOD=:PERIOD]])
--	wrapper:query([[DELETE FROM ::PROD_SCM.::PROD_TBL where PERIOD=:PERIOD]])
--	_,res = wrapper:query([[import into ::STAGE_SCM.::STAGE_TBL from csv at :SITE_URL FILE :FILE_NAME ]]..wrapper:get_param('COL_SELECTION')..[[
--		 row separator='LF' TRIM SKIP=]]..wrapper:get_param('SKIP_ROWS')..[[ ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE]])
--	if (res.etl_rows_with_error > 0) then
--		wrapper:log('WARN','FILE for PERIOD='..wrapper:get_param('PERIOD')..' found rows with errors',res.etl_rows_with_error)
--	end	
--end
--
----initialize query wrapper
--wrapper = QW.new( 'PRESCRIPTIONS_UK_STAGE.JOB_LOG', 'PRESCRIPTIONS_UK_STAGE.JOB_DETAILS', 'PRESCRIPTIONS_LOAD')
--
----set SCHEMA / TABLE for staging table
--wrapper:set_param('STAGE_SCM',quote('PRESCRIPTIONS_UK_STAGE')) 
--wrapper:set_param('PROD_SCM',quote('&PRODSCM')) 
--
---- ****** SUBSTANCES *********
--wrapper:set_param('STAGE_TBL',quote('CHEMICAL_SUBSTANCES'))
--wrapper:set_param('PROD_TBL',quote('CHEMICAL_SUBSTANCES'))
--wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRACTICAL_ADDRESS'))
--wrapper:set_param('GRP_DESCRIPTION','Chemical names and BNF codes')
--wrapper:set_param('COL_SELECTION','')
--wrapper:set_param('SKIP_ROWS',1)
--wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10)
--
--for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values( [[select PERIOD,SITE_URL,FILE_NAME from ::STAGE_SCM.RAW_DATA_URLS 
--where LOADED_TIMESTAMP is NULL 
--and DESCRIPTION=:GRP_DESCRIPTION
--order by period]]) do
--	wrapper:query([[truncate table ::STAGE_SCM.::STAGE_TBL]])
--    wrapper:set_param('SITE_URL',SITE_URL)
--	wrapper:set_param('PERIOD',PERIOD)
--	wrapper:set_param('FILE_NAME',FILE_NAME)
--
--
--	_,res = wrapper:query([[import into ::STAGE_SCM.::STAGE_TBL from csv at :SITE_URL FILE :FILE_NAME ]]..wrapper:get_param('COL_SELECTION')..[[
--		 row separator='LF' TRIM SKIP=]]..wrapper:get_param('SKIP_ROWS')..[[ ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE]])
--	if (res.etl_rows_with_error > 0) then
--		wrapper:log('WARN','FILE for PERIOD='..wrapper:get_param('PERIOD')..' found rows with errors',res.etl_rows_with_error)
--	end		
--	--preparation finished -> move from STAGE into PROD table
--	wrapper:query([[merge into ::PROD_SCM.::PROD_TBL tgt using ::STAGE_SCM.::STAGE_TBL src 
--		on TGT.CHEM_SUB = src.CHEM_SUB
--		when matched then update set tgt.CHEMICAL_NAME = src.CHEMICAL_NAME
--		when not matched then insert (CHEM_SUB,CHEMICAL_NAME) values (CHEM_SUB, CHEMICAL_NAME)]])
--	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
--	wrapper:query([[update ::STAGE_SCM.RAW_DATA_URLS set LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
--		WHERE PERIOD=:PERIOD and DESCRIPTION=:GRP_DESCRIPTION]])
--end
--
---- ****** ADDRESS *******
--wrapper:set_param('STAGE_TBL',quote('PRACTICE_ADDRESS'))
--wrapper:set_param('PROD_TBL',quote('PRACTICE_ADDRESS'))
--wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRACTICAL_ADDRESS'))
--wrapper:set_param('GRP_DESCRIPTION','Practice codes, names and addresses')
--wrapper:set_param('COL_SELECTION','(1..8)')
--wrapper:set_param('SKIP_ROWS','0')
--wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10)
--
--for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values( [[select PERIOD,SITE_URL,FILE_NAME from ::STAGE_SCM.RAW_DATA_URLS 
--where LOADED_TIMESTAMP is NULL 
--and DESCRIPTION=:GRP_DESCRIPTION
--order by period]]) do
--	process_file(PERIOD,SITE_URL,FILE_NAME) 
--
--	--preparation finished -> move from STAGE into PROD table
--	wrapper:query([[insert into ::PROD_SCM.::PROD_TBL 
--		(PERIOD, PRACTICE, PRACTICE_NAME, ADDRESS_PART1, ADDRESS_PART2, ADDRESS_PART3, ADDRESS_PART4, POSTCODE_FULL,POSTCODE) 
--		select PERIOD, PRACTICE, PRACTICE_NAME, ADDRESS_PART1, ADDRESS_PART2, ADDRESS_PART3, ADDRESS_PART4, POSTCODE_FULL,substr(postcode_full,0,instr(POSTCODE_FULL,' ')-1) as POSTCODE from ::STAGE_SCM.::STAGE_TBL t 
--		where PERIOD=:PERIOD]])
--	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
--	wrapper:query([[update ::STAGE_SCM.RAW_DATA_URLS set LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
--		WHERE PERIOD=:PERIOD and DESCRIPTION=:GRP_DESCRIPTION]])
--
--end
--
---- ****** PRESCRIPTIONS ******
--wrapper:set_param('STAGE_TBL',quote('PRESCRIPTIONS'))
--wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRESCIPTIONS'))
--wrapper:set_param('PROD_TBL',quote('PRESCRIPTIONS'))
--wrapper:set_param('GRP_DESCRIPTION','Practice prescribing data')
--wrapper:set_param('COL_SELECTION','(1..10)')
--wrapper:set_param('SKIP_ROWS','1')
--wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10000)
--
--for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values( [[select PERIOD,SITE_URL,FILE_NAME from ::STAGE_SCM.RAW_DATA_URLS 
--where LOADED_TIMESTAMP is NULL 
--and DESCRIPTION=:GRP_DESCRIPTION
--order by period]]) do
--	process_file(PERIOD,SITE_URL,FILE_NAME) 
--	--preparation finished -> move from STAGE into PROD table
--	wrapper:query([[insert into ::PROD_SCM.::PROD_TBL 
--select p.*,TO_DATE(to_char(a.PERIOD),'YYYYMM'),NVL(NVL(c.SK_CHEM_SUB,c2.SK_CHEM_SUB),(select min(SK_CHEM_SUB) from PRESCRIPTIONS_UK.CHEMICAL_SUBSTANCES where CHEM_SUB='*NO_DATA*')),a.SK_PRACTICE_ADDRESS from ::STAGE_SCM.::STAGE_TBL p 
--left join ::PROD_SCM.PRACTICE_ADDRESS a on p.PRACTICE=a.PRACTICE and p.PERIOD=a.PERIOD
--left join (select * from ::PROD_SCM.CHEMICAL_SUBSTANCES where length(trim(chem_sub))>4) c on LEFT(p.bnf_code,9) = trim(c.chem_sub)
--left join (select * from ::PROD_SCM.CHEMICAL_SUBSTANCES where length(trim(chem_sub))=4) c2 on LEFT(p.bnf_code,4) = trim(c2.chem_sub)
--		where p.PERIOD=:PERIOD
--]])
--	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
--	wrapper:query([[update ::STAGE_SCM.RAW_DATA_URLS set LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
--		WHERE PERIOD=:PERIOD and DESCRIPTION=:GRP_DESCRIPTION]])
--end
----TODO: setup process to re-check if we now have data for *NO_DATA* values
--return wrapper:finish()
--/
