-- refreshing the metadata for the dataset to check for new files
-- for delta-loads re-create the staging tbl and then re-load the data
CREATE OR REPLACE TABLE PRESCRIPTIONS_UK_STAGE.stage_raw_data_urls AS
SELECT  description, TO_NUMBER(TO_CHAR(TO_DATE('August 2018','MONTH YYYY'), 'YYYYMM'), '999999') period, url
FROM   (SELECT PRESCRIPTIONS_UK_STAGE.json_parsing_datapackage()) a
ORDER BY period;

MERGE
INTO    PRESCRIPTIONS_UK_STAGE.raw_data_urls tgt
USING   PRESCRIPTIONS_UK_STAGE.stage_raw_data_urls src
ON tgt.url = src.url
WHEN NOT MATCHED THEN
INSERT
	(
		description,
		period,
		url
	)
	VALUES
	(
		description,
		period,
		url
	) ;
	
----seems to be an error in the source data
--UPDATE PRESCRIPTIONS_UK_STAGE.raw_data_urls
--SET     FILE_NAME = 'T201009CHEM%20SUBS.CSV'
--WHERE   PERIOD = '201009'
--AND     DESCRIPTION = 'Chemical names and BNF codes';
--UPDATE PRESCRIPTIONS_UK_STAGE.raw_data_urls
--SET     FILE_NAME = 'T201103ADDR%20BNFT.CSV'
--WHERE   PERIOD = '201103'
--AND     DESCRIPTION = 'Practice codes, names and addresses';

----take a look at the new files to load
--SELECT  *
--FROM    PRESCRIPTIONS_UK_STAGE.raw_data_urls
--WHERE   loaded_timestamp IS NULL;
----TIP: all data will be loaded, that has no loaded_timestamp.
----     If you want to load the data on a small machine, you can simply set a dummy loaded_timestamp
---- for older data
----e.g.:
---- update PRESCRIPTIONS_UK_STAGE.raw_data_urls set loaded_timestamp='0001-01-01 00:00:00' where period <=
---- 201702;
----this script loads all new data (delta load)
--EXECUTE SCRIPT PRESCRIPTIONS_UK_STAGE.PRESCRIPTIONS_LOAD();
--SELECT  *
--FROM    PRESCRIPTIONS_UK_STAGE.JOB_DETAILS
--ORDER BY run_id DESC,
--	detail_id DESC;
