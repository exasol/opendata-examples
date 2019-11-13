--script for parsing site data and file URLs
--/
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT "PRESCRIPTIONS_UK_STAGE"."JSON_PARSING_DATAPACKAGE" () 
EMITS ("DESCRIPTION" VARCHAR(100) UTF8,"PERIOD" VARCHAR(20) UTF8, "SITE_URL" VARCHAR(100) UTF8, "FILE_NAME" VARCHAR(30) UTF8) AS
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
        description = e.get("name").lower()
        period = re.findall(r"\w+ \w+", e.get("name"))[0]
        temp_url = e.get("contentUrl")
        url = re.findall(r".*[\/]", temp_url)[0]
        file_name = re.findall(r"(?:.(?!\/))+$", temp_url)[0][1:]
        ctx.emit(description, period, url, file_name)
/

--load script (executing this again will only load new data if metadata has been refreshed)
CREATE OR REPLACE LUA SCRIPT PRESCRIPTIONS_UK_STAGE.PRESCRIPTIONS_LOAD RETURNS TABLE AS
import('ETL.QUERY_WRAPPER','QW')

function process_file(PERIOD,SITE_URL,FILE_NAME)
        wrapper:set_param('SITE_URL',SITE_URL)
	wrapper:set_param('FILE_NAME',FILE_NAME)
	wrapper:set_param('PERIOD',PERIOD)
	       
	--cleanup to ensure there is no data left in case of an earlier failed run
	--period based cleanup is only supported by ADDRESS and PRESCRIPTIONS table
	wrapper:query([[TRUNCATE TABLE ::STAGE_SCM.::STAGE_TBL]])
	if(wrapper:get_param('STAGE_TBL') ~= quote('CHEMICAL_SUBSTANCES')) then
                wrapper:query([[DELETE FROM ::STAGE_SCM.::STAGE_TBL WHERE PERIOD=:PERIOD]])
                wrapper:query([[DELETE FROM ::PROD_SCM.::PROD_TBL WHERE PERIOD=:PERIOD]])
        end
	
	--import full csv file into stage table
	_,res = wrapper:query([[
	       IMPORT INTO ::STAGE_SCM.::STAGE_TBL]]..
	       wrapper:get_param('COL_DESTINATION')..
	       [[FROM CSV AT :SITE_URL 
	       FILE :FILE_NAME]].. 
	       wrapper:get_param('COL_SELECTION')..
	       [[ROW SEPARATOR=:ROW_SEPARATOR
	       TRIM 
	       SKIP=:SKIP_ROWS
	       ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL 
	       REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE]])
	if (res.etl_rows_with_error > 0) then
		wrapper:log('WARN','FILE for PERIOD='..wrapper:get_param('PERIOD')..' found rows with errors',res.etl_rows_with_error)
	end	
end

--initialize query wrapper
wrapper = QW.new( 'PRESCRIPTIONS_UK_STAGE.JOB_LOG', 'PRESCRIPTIONS_UK_STAGE.JOB_DETAILS', 'PRESCRIPTIONS_LOAD')

--set SCHEMA / TABLE for staging table
wrapper:set_param('STAGE_SCM',quote('PRESCRIPTIONS_UK_STAGE')) 
wrapper:set_param('PROD_SCM',quote('PRESCRIPTIONS_UK')) 

-- ****** SUBSTANCES *********
wrapper:set_param('STAGE_TBL',quote('CHEMICAL_SUBSTANCES'))
wrapper:set_param('PROD_TBL',quote('CHEMICAL_SUBSTANCES'))
wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_SUBSTANCES'))
wrapper:set_param('GRP_DESCRIPTION','%chemical%')
wrapper:set_param('COL_SELECTION', '(1,2)')
wrapper:set_param('COL_DESTINATION', '("CHEM_SUB", "CHEMICAL_NAME")')
wrapper:set_param('SKIP_ROWS',1)
wrapper:set_param('ROW_SEPARATOR', 'LF')
wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10)

for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values( [[
        SELECT PERIOD,SITE_URL,FILE_NAME 
        FROM ::STAGE_SCM.RAW_DATA_URLS 
        WHERE LOADED_TIMESTAMP IS NULL 
        AND DESCRIPTION LIKE :GRP_DESCRIPTION
        ORDER BY PERIOD]]) do
        wrapper:set_param('SITE_URL',SITE_URL)
	wrapper:set_param('PERIOD',PERIOD)
	wrapper:set_param('FILE_NAME',FILE_NAME)

        -- import csv into staging table
	process_file(PERIOD,SITE_URL,FILE_NAME)
			
	--preparation finished -> move from STAGE into PROD table
	wrapper:query([[
                MERGE INTO ::PROD_SCM.::PROD_TBL tgt 
                USING ::STAGE_SCM.::STAGE_TBL src 
		ON TGT.CHEM_SUB = src.CHEM_SUB
		WHEN MATCHED THEN 
		      UPDATE SET tgt.CHEMICAL_NAME = src.CHEMICAL_NAME
		WHEN NOT MATCHED THEN 
		      INSERT (CHEM_SUB,CHEMICAL_NAME) 
		      VALUES (CHEM_SUB, CHEMICAL_NAME)]])
		
	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
	wrapper:query([[
                UPDATE::STAGE_SCM.RAW_DATA_URLS 
                SET LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
		WHERE PERIOD=:PERIOD 
		AND DESCRIPTION LIKE :GRP_DESCRIPTION]])
end

-- ****** ADDRESS *******
wrapper:set_param('STAGE_TBL',quote('PRACTICE_ADDRESS'))
wrapper:set_param('PROD_TBL',quote('PRACTICE_ADDRESS'))
wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRACTICAL_ADDRESS'))
wrapper:set_param('GRP_DESCRIPTION','%addresses%')
wrapper:set_param('COL_SELECTION','(1..8)')
wrapper:set_param('COL_DESTINATION', ' ')
wrapper:set_param('SKIP_ROWS',0)
wrapper:set_param('ROW_SEPARATOR', 'LF')
wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10)

for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values( [[
        SELECT PERIOD,SITE_URL,FILE_NAME 
        FROM ::STAGE_SCM.RAW_DATA_URLS 
        WHERE LOADED_TIMESTAMP is NULL 
        AND DESCRIPTION LIKE:GRP_DESCRIPTION
        ORDER BY PERIOD]]) do
	process_file(PERIOD,SITE_URL,FILE_NAME) 
        
        --clean up corrupt data before loading into PROD
        wrapper:query([[DELETE FROM ::STAGE_SCM.::STAGE_TBL WHERE POSTCODE_FULL = 'N/A']])
        
	--preparation finished -> move from STAGE into PROD table
	wrapper:query([[
                INSERT INTO ::PROD_SCM.::PROD_TBL (
                                                PERIOD, 
                                                PRACTICE, 
                                                PRACTICE_NAME, 
                                                ADDRESS_PART1, 
                                                ADDRESS_PART2, 
                                                ADDRESS_PART3, 
                                                ADDRESS_PART4, 
                                                POSTCODE_FULL, 
                                                POSTCODE) 
		SELECT        PERIOD, 
		              PRACTICE, 
		              PRACTICE_NAME, 
		              ADDRESS_PART1, 
		              ADDRESS_PART2, 
		              ADDRESS_PART3, 
		              ADDRESS_PART4, 
		              POSTCODE_FULL, 
		              substr(
		                      postcode_full, 0, instr(POSTCODE_FULL,' ')-1) AS POSTCODE 
                FROM ::STAGE_SCM.::STAGE_TBL t 
		WHERE PERIOD=:PERIOD]])

	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
	wrapper:query([[
                UPDATE::STAGE_SCM.RAW_DATA_URLS 
                SET LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
		WHERE PERIOD=:PERIOD AND DESCRIPTION LIKE:GRP_DESCRIPTION]])

end


-- ****** PRESCRIPTIONS ******
wrapper:set_param('STAGE_TBL',quote('PRESCRIPTIONS'))
wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_PRESCIPTIONS'))
wrapper:set_param('PROD_TBL',quote('PRESCRIPTIONS'))
wrapper:set_param('GRP_DESCRIPTION','%prescribing%')
wrapper:set_param('COL_SELECTION','(1..10)')
wrapper:set_param('SKIP_ROWS',1)
wrapper:set_param('ROW_SEPARATOR', 'LF')
wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10000)

for PERIOD,SITE_URL,FILE_NAME in wrapper:query_values([[
        SELECT PERIOD,SITE_URL,FILE_NAME FROM ::STAGE_SCM.RAW_DATA_URLS 
        WHERE LOADED_TIMESTAMP is NULL 
        AND DESCRIPTION LIKE :GRP_DESCRIPTION
        ORDER BY PERIOD]]) do
	process_file(PERIOD,SITE_URL,FILE_NAME) 
	
	--preparation finished -> move from STAGE into PROD table
	-- edit: left join in first join statement produced errors in prod table because of missing SK_PRACTICE_ADDRESS
	wrapper:query([[
                INSERT INTO ::PROD_SCM.::PROD_TBL 
                SELECT  p.*, 
                        TO_DATE(to_char(a.PERIOD),'YYYYMM'),
                        NVL(NVL(c.SK_CHEM_SUB,c2.SK_CHEM_SUB),
                        (select min(SK_CHEM_SUB) 
                                FROM PRESCRIPTIONS_UK.CHEMICAL_SUBSTANCES 
                                WHERE CHEM_SUB='*NO_DATA*')),
                        a.SK_PRACTICE_ADDRESS FROM ::STAGE_SCM.::STAGE_TBL p 
                JOIN ::PROD_SCM.PRACTICE_ADDRESS a ON p.PRACTICE = a.PRACTICE AND p.PERIOD = a.PERIOD
                LEFT JOIN (select * from ::PROD_SCM.CHEMICAL_SUBSTANCES WHERE length(trim(chem_sub))>4) c ON LEFT(p.bnf_code,9) = trim(c.chem_sub)
                LEFT JOIN (select * from ::PROD_SCM.CHEMICAL_SUBSTANCES WHERE length(trim(chem_sub))=4) c2 ON LEFT(p.bnf_code,4) = trim(c2.chem_sub)
		WHERE p.PERIOD=:PERIOD]])
		
	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
	wrapper:query([[
	       UPDATE ::STAGE_SCM.RAW_DATA_URLS 
	       SET LOADED_TIMESTAMP=CURRENT_TIMESTAMP
	       WHERE PERIOD=:PERIOD AND DESCRIPTION LIKE :GRP_DESCRIPTION]])
end
--TODO: setup process to re-check if we now have data for *NO_DATA* values
return wrapper:finish()
/
