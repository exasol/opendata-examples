--import zones
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.TMP_TAXI_ZONES (
        LOCATION_ID SMALLINT, 
        BOROUGH VARCHAR(15), 
        "ZONE" VARCHAR(50), 
        SERVICE_ZONE VARCHAR(20));   

IMPORT INTO NYC_TAXI_STAGE."TMP_TAXI_ZONES" 
FROM CSV AT 'https://s3.amazonaws.com/nyc-tlc/misc/'
FILE 'taxi+_zone_lookup.csv'
SKIP = 1
ROW SEPARATOR = 'CRLF'
COLUMN DELIMITER = '"';
        
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.TMP_TAXI_ZONE_POLYGONS (
        LOCATION_ID SMALLINT, 
        ZONE_LENGTH DOUBLE,
        POLYGON VARCHAR(500000),
        ZONE_AREA DOUBLE,
        ZONE_NAME VARCHAR(200),
        BOROUGH VARCHAR(200));

--import polygon strings
IMPORT INTO NYC_TAXI_STAGE."TMP_TAXI_ZONE_POLYGONS" 
FROM CSV AT 'https://raw.githubusercontent.com/exasol/opendata-examples/LN_update_11_2019/ny_taxi/' FILE 'taxi_zones.csv' (1..5, 7)
SKIP = 1
ROW SEPARATOR = 'LF'
COLUMN SEPARATOR = ';'
COLUMN DELIMITER = '"';

--join polygons and zones into PROD
INSERT INTO NYC_TAXI.TAXI_ZONES
select z.LOCATION_ID, z.BOROUGH, p.ZONE_NAME, z.SERVICE_ZONE, p.ZONE_LENGTH, p.POLYGON, p.ZONE_AREA
from NYC_TAXI_STAGE.TMP_TAXI_ZONES z
join NYC_TAXI_STAGE.TMP_TAXI_ZONE_POLYGONS p on z.location_id = p.location_id;

drop table NYC_TAXI_STAGE."TMP_TAXI_ZONE_POLYGONS" ;
drop table NYC_TAXI_STAGE."TMP_TAXI_ZONES" ;

-- cleanup
truncate table NYC_TAXI_STAGE.STAGE_RAW_DATA_URLS;
--import URLs
import into NYC_TAXI_STAGE.STAGE_RAW_DATA_URLS
from csv at 'https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/setup_files/' file 'raw_data_urls.txt' row separator = 'LF';

--manipulate URLS
merge into NYC_TAXI_STAGE.RAW_DATA_URLS tgt 
using (
	with vv_filename as (  select 
	                       replace(substr(url,instr(url,'/',-1)+1),'.csv','') as filename,url 
	                       from    NYC_TAXI_STAGE.STAGE_RAW_DATA_URLS),
	                               vv_splitted as (select  substr(filename,0,instr(filename,'_')-1) as type,
	                                                       to_date(substr(filename,instr(filename,'tripdata_') + length('tripdate_')),'YYYY-MM') as trip_month,
	                                                       substr(url,0,instr(url,'/',-1)-1) as site_url,
	                                                       substr(url,instr(url,'/',-1)+1) as full_file 
	                                               from vv_filename)
	select * from vv_splitted) src on tgt.type=src.type and tgt.trip_month=src.trip_month
when not matched then insert (type,trip_month,site_url,filename) values (type,trip_month,site_url,full_file);

--fill cab_types
INSERT INTO NYC_TAXI.cab_types (type) SELECT 'yellow';
INSERT INTO NYC_TAXI.cab_types (type) SELECT 'green';
INSERT INTO NYC_TAXI.cab_types (type) SELECT 'uber';

--fill vendor_lookup
INSERT INTO NYC_TAXI.vendor_lookup VALUES (1, 'Creative Mobile Technologies', 'CMT');
INSERT INTO NYC_TAXI.vendor_lookup VALUES (2, 'VeriFone Inc.', 'VTS');
INSERT INTO NYC_TAXI.vendor_lookup VALUES (3, 'Digital Dispatch Systems', 'DDS');

--fill trip_type_lookup
INSERT INTO NYC_TAXI.trip_type_lookup VALUES (1, 'Street-hail');
INSERT INTO NYC_TAXI.trip_type_lookup VALUES (2, 'Dispatch');

--fill rate_code_lookup
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (1, 'Standard rate');
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (2, 'JFK');
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (3, 'Newark');
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (4, 'Nassau or Westchester');
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (5, 'Negotiated fare');
INSERT INTO NYC_TAXI.rate_code_lookup VALUES (6, 'Group ride');

-- load data
--TIP: all data will be loaded, that has no loaded_timestamp.
--     If you want to load the data on a small machine, you can simply set a dummy loaded_timestamp
--     for older data e.g.:
-- update NYC_TAXI_STAGE.RAW_DATA_URLS set loaded_timestamp='0001-01-01 00:00:00' where trip_month > '2009-12-01';

--update NYC_TAXI_STAGE.RAW_DATA_URLS set loaded_timestamp='0001-01-01 00:00:00' where trip_month > '2009-12-01';
--update NYC_TAXI_STAGE.RAW_DATA_URLS set loaded_timestamp=Null where true;

EXECUTE SCRIPT NYC_TAXI_STAGE.TRIPS_LOAD();

select * from NYC_TAXI_STAGE.JOB_DETAILS order by run_id desc,detail_id desc;
select * from NYC_TAXI_STAGE.ERRORS_TRIPDATA;

