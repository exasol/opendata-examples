--polygon grid script
--/
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT NYC_TAXI_STAGE.create_polygon_grid(     min_x DECIMAL(15,13), 
                                                                                max_x DECIMAL(15,13), 
                                                                                min_y DECIMAL(15,13), 
                                                                                max_y DECIMAL(15,13)) EMITS (GRID_FIELD VARCHAR(300), SEGMENT_ID DECIMAL(3)) AS
 
def run(ctx):
        min_x = ctx.min_x
        min_y = ctx.min_y
        max_x = ctx.max_x
        max_y = ctx.max_y
 
        grid_width = 10  #GRID-SIZE DEFINITION: 10: 10 * 10 segments = 100 segments
        x_step_width = (max_x-min_x)/grid_width
        y_step_width = (max_y-min_y)/grid_width
 
        segment_id = 0
        for y in range (grid_width):
                y_step = y * y_step_width
                for x in range(grid_width):
                        segment_id += 1
                        x_step = x * x_step_width
                        ctx.emit(f"POLYGON(({min_x + x_step} {min_y + y_step}, "
                                        f"{min_x + x_step + x_step_width} {min_y + y_step}, "
                                        f"{min_x + x_step + x_step_width} {min_y + y_step + y_step_width}, "
                                        f"{min_x + x_step} {min_y + y_step + y_step_width}, "
                                        f"{min_x + x_step} {min_y + y_step}))", segment_id)
/

--load script
CREATE OR REPLACE LUA SCRIPT NYC_TAXI_STAGE.TRIPS_LOAD RETURNS TABLE AS
import('ETL.QUERY_WRAPPER','QW')

function process_file(FILE_ID, TRIP_MONTH,SITE_URL, FILENAME, INSERT_COLS, CREATE_COLS, FILE_OPTIONS)
	wrapper:set_param('TRIP_MONTH',TRIP_MONTH)  
        wrapper:set_param('SITE_URL',SITE_URL) 
	wrapper:set_param('FILENAME',FILENAME) 
	wrapper:set_param('FILE_ID',FILE_ID)
        
        wrapper:set_param('LOCATION_MAP',quote('TRIP_LOCATION_ID_MAP'))
        wrapper:set_param('ACCEPTED_ERRORS_PER_FILE',10000)
	wrapper:set_param('STAGE_ERROR_TBL',quote('ERRORS_TRIPDATA'))
	
	TMP_FILE_OPTIONS = ''
	if FILE_OPTIONS ~= '' then
		TMP_FILE_OPTIONS='('..FILE_OPTIONS..')'
	end
        
        --cleanup
        wrapper:query([[DELETE FROM ::STAGE_SCM.::STAGE_TBL WHERE FILE_ID=:FILE_ID]])
        wrapper:query([[TRUNCATE TABLE ::STAGE_SCM.::STAGE_TBL]])
        wrapper:query([[TRUNCATE TABLE ::STAGE_SCM.::LOCATION_MAP]])

        --import data into target table (insert with import as a subselect to add metadata like file_id)
        wrapper:query([[CREATE OR REPLACE TABLE ::STAGE_SCM.::STAGE_TMP_TBL (]]..CREATE_COLS..[[)]])
        
	_,res = wrapper:query([[
	       IMPORT INTO ::STAGE_SCM.::STAGE_TMP_TBL (]]..INSERT_COLS..[[) 
	       FROM CSV AT :SITE_URL FILE :FILENAME ]]..TMP_FILE_OPTIONS..[[
	       SKIP=1 
	       ROW SEPARATOR='CRLF' 
	       ERRORS INTO ::STAGE_SCM.::STAGE_ERROR_TBL 
	       REJECT LIMIT :ACCEPTED_ERRORS_PER_FILE]])
	       
	if (res.etl_rows_with_error > 0) then
		wrapper:log('WARN','FILE='..wrapper:get_param('FILE_ID')..' found rows with errors',res.etl_rows_with_error)
	end
	
	--insert into stage from tmp_stage
	wrapper:query([[INSERT INTO ::STAGE_SCM.::STAGE_TBL (FILE_ID, VENDOR_TYPE, ]]..INSERT_COLS..[[)
	                       select :FILE_ID,:VENDOR_TYPE,]]..INSERT_COLS..[[ 
	                       from ::STAGE_SCM.::STAGE_TMP_TBL]])
	
	--drop tmp_stage               
	wrapper:query([[DROP TABLE ::STAGE_SCM.::STAGE_TMP_TBL]])          
	
	--Calculate geometric points for pickup and dropff LONG/LAT and update in stage_table
        wrapper:query([[UPDATE ::STAGE_SCM.::STAGE_TBL 
                        SET PICK_GEOM = 'POINT(' || pickup_longitude  || ' ' || pickup_latitude  || ')' 
                        WHERE pickup_longitude IS NOT NULL 
                                AND pickup_latitude IS NOT NULL
                                AND PICK_GEOM IS NULL]])
                                
        wrapper:query([[UPDATE ::STAGE_SCM.::STAGE_TBL
                        SET DROP_GEOM = 'POINT(' || dropoff_longitude || ' ' || dropoff_latitude || ')' 
                        WHERE dropoff_longitude IS NOT NULL 
                                AND dropoff_latitude IS NOT NULL
                                AND DROP_GEOM IS NULL]])
        
        --Insert trip id's where location_map calculation is necessary into location_map
        wrapper:query([[INSERT INTO ::STAGE_SCM.::LOCATION_MAP (trip_id)
                        SELECT id FROM ::STAGE_SCM.::STAGE_TBL
                        WHERE dropoff_locationid IS NULL
                                OR pickup_locationid IS NULL]])

	-- Cluster geo data into taxi zones
	wrapper:query([[MERGE INTO ::STAGE_SCM.::LOCATION_MAP insert_target
                        USING   (SELECT DISTINCT t.id,
                                        FIRST_VALUE(zd.location_id) OVER (PARTITION BY t.id) AS dropoff,
                                        FIRST_VALUE(zp.location_id) OVER (PARTITION BY t.id) AS pickup
                                 FROM ::STAGE_SCM.::STAGE_TBL t
                                 JOIN ::STAGE_SCM.::LOCATION_MAP m    ON m.trip_id = t.id 
                                 JOIN ::PROD_SCM.taxi_zones zp       ON ST_WITHIN(t.pick_geom, zp.polygon) = true
                                 JOIN ::PROD_SCM.taxi_zones zd       ON ST_WITHIN(t.drop_geom, zd.polygon) = true
                                 /*WHERE t.dropoff_locationid IS NULL OR
                                        t.pickup_locationid IS NULL*/) /*Diese Where clause könnte die temporäre trip_locaton_id_map ersetzen. der marge ginge dann in tripdata*/
                        subselect
                        ON      insert_target.trip_id = subselect.id
                        WHEN MATCHED THEN 
                                UPDATE SET      dropoff_locationid = dropoff,
                                                pickup_locationid  = pickup]])

	--preparation finished -> move from STAGE into PROD table
        wrapper:query([=[INSERT INTO ::PROD_SCM.::PROD_TBL
                        SELECT  t.id,
                                t.file_id AS src_file_id,
                                c.cab_type_id,
                                CASE    WHEN vendor_id = 'CMT' THEN 1
                                        WHEN vendor_id = 'VTS' THEN 2
                                        WHEN vendor_id = 'DDS' THEN 3
                                        ELSE CAST(vendor_id AS DECIMAL(1))
                                        END AS vendor_id,
                                l.id,
                                TO_DATE(t.pickup_datetime, 'YYYY-MM-DD') AS pickup_date,
                                TO_DATE(t.dropoff_datetime, 'YYYY-MM-DD') AS dropoff_date,
                                t.pickup_datetime,
                                t.dropoff_datetime,
                                t.store_and_fwd_flag,
                                t.rate_code_id,
                                t.pickup_longitude,
                                t.pickup_latitude,
                                CASE    WHEN z.trip_id IS NULL THEN t.pickup_locationid ELSE z.pickup_locationid END AS pickup_locationid,
                                t.dropoff_longitude,
                                t.dropoff_latitude,
                                CASE    WHEN z.trip_id IS NULL THEN t.dropoff_locationid ELSE z.dropoff_locationid END AS dropoff_locationid,
                                t.passenger_count,
                                t.trip_distance,
                                t.fare_amount,
                                t.extra,
                                t.mta_tax,
                                t.tip_amount,
                                t.tolls_amount,
                                t.ehail_fee,
                                t.improvement_surcharge,
                                t.total_amount,
                                CASE    WHEN t.payment_type = '1' OR LOWER(t.payment_type) REGEXP_LIKE 'cr'       THEN 'credit'
                                        WHEN t.payment_type = '2' OR LOWER(t.payment_type) REGEXP_LIKE 'c[as]'    THEN 'cash'
                                        WHEN t.payment_type = '3' OR LOWER(t.payment_type) REGEXP_LIKE 'no'       THEN 'no charge'
                                        WHEN t.payment_type = '4' OR LOWER(t.payment_type) REGEXP_LIKE 'dis'      THEN 'dispute'
                                        ELSE Null
                                        END AS payment_type,
                                t.trip_type
                        FROM ::STAGE_SCM.::STAGE_TBL t
                        LEFT OUTER JOIN ::PROD_SCM.CAB_TYPES c ON t.vendor_type = c.type
                        LEFT OUTER JOIN ::STAGE_SCM.::LOCATION_MAP z ON t.id = z.trip_id
                        LEFT OUTER JOIN ::PROD_SCM.HVFHS_LICENSE_LOOKUP l ON t.hvfhs_license_num = l.high_volume_license_number]=])
        
	--load successful -> update loaded_timestamp to indicate file doesn't need to be loaded anymore
	wrapper:query([[UPDATE ::STAGE_SCM.RAW_DATA_URLS 
	                SET LOADED_TIMESTAMP=CURRENT_TIMESTAMP 
	                WHERE ID=:FILE_ID]])
        wrapper:commit()
end

--initialize query wrapper
wrapper = QW.new( 'NYC_TAXI_STAGE.JOB_LOG', 'NYC_TAXI_STAGE.JOB_DETAILS', 'TRIPS_LOAD')

--set SCHEMA / TABLE for staging table
wrapper:set_param('STAGE_SCM',quote('NYC_TAXI_STAGE'))
wrapper:set_param('STAGE_TBL',quote('TRIPDATA'))

wrapper:set_param('PROD_SCM',quote('NYC_TAXI_')) 
wrapper:set_param('PROD_TBL',quote('TRIPS'))

_,SESSION_ID = wrapper:query([[SELECT TO_CHAR(CURRENT_SESSION)]])
wrapper:log('INFO','SESSION_ID='..SESSION_ID[1][1],null)
wrapper:set_param('STAGE_TMP_TBL','TRIPDATA_'..SESSION_ID[1][1])

/*
############################## YELLOW DATA ############################## 
*/
--load yellow from start to 2015
wrapper:set_param('VENDOR_TYPE','yellow')  
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    SELECT ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        FROM ::STAGE_SCM.RAW_DATA_URLS 
                                                                        WHERE LOADED_TIMESTAMP IS NULL 
                                                                        AND TYPE=:VENDOR_TYPE 
                                                                        AND TRIP_MONTH < '2015-01-01']]) do
	INSERT_COLS=[[VENDOR_ID, 
	               PICKUP_DATETIME, 
	               DROPOFF_DATETIME, 
	               PASSENGER_COUNT, 
	               TRIP_DISTANCE, 
	               PICKUP_LONGITUDE, 
	               PICKUP_LATITUDE, 
	               RATE_CODE_ID, 
	               STORE_AND_FWD_FLAG, 
	               DROPOFF_LONGITUDE, 
	               DROPOFF_LATITUDE, 
	               PAYMENT_TYPE, 
	               FARE_AMOUNT, 
	               EXTRA, 
	               MTA_TAX, 
	               TIP_AMOUNT, 
	               TOLLS_AMOUNT, 
	               TOTAL_AMOUNT]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3), 
	               PICKUP_LONGITUDE DOUBLE, 
	               PICKUP_LATITUDE DOUBLE, 
	               RATE_CODE_ID DECIMAL(9,0), 
	               STORE_AND_FWD_FLAG VARCHAR(10), 
	               DROPOFF_LONGITUDE DOUBLE, 
	               DROPOFF_LATITUDE DOUBLE, 
	               PAYMENT_TYPE varchar(20), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2), 
	               TOTAL_AMOUNT DECIMAL(9,2)]]
	FILE_OPTIONS=''
	output('loading file: '..tostring(FILE_ID))
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

--load yellow from 2015 to 2016-07 (Pickup/Dropoff Longitude/Latitude replaced with LocationID)
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE 
                                                                        and TRIP_MONTH >= '2015-01-01'
                                                                        and TRIP_MONTH < '2016-07-01']]) do
	INSERT_COLS=[[VENDOR_ID, 
	               PICKUP_DATETIME, 
	               DROPOFF_DATETIME, 
	               PASSENGER_COUNT, 
	               TRIP_DISTANCE, 
	               PICKUP_LONGITUDE, 
	               PICKUP_LATITUDE, 
	               RATE_CODE_ID, 
	               STORE_AND_FWD_FLAG, 
	               DROPOFF_LONGITUDE, 
	               DROPOFF_LATITUDE, 
	               PAYMENT_TYPE, 
	               FARE_AMOUNT, 
	               EXTRA, 
	               MTA_TAX, 
	               TIP_AMOUNT, 
	               TOLLS_AMOUNT, 
	               IMPROVEMENT_SURCHARGE, 
	               TOTAL_AMOUNT]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3), 
	               PICKUP_LONGITUDE DOUBLE, 
	               PICKUP_LATITUDE DOUBLE, 
	               RATE_CODE_ID DECIMAL(9,0), 
	               STORE_AND_FWD_FLAG VARCHAR(10), 
	               DROPOFF_LONGITUDE DOUBLE, 
	               DROPOFF_LATITUDE DOUBLE, 
	               PAYMENT_TYPE varchar(20), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2), 
	               IMPROVEMENT_SURCHARGE DECIMAL(9,2), 
	               TOTAL_AMOUNT DECIMAL(9,2)]]
	FILE_OPTIONS=''
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

--load yellow from 2016-07 to end
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE 
                                                                        and TRIP_MONTH >= '2016-07-01'
                                                                        and TRIP_MONTH < '2020-01-01']]) do
	INSERT_COLS=[[VENDOR_ID,
	               PICKUP_DATETIME,
	               DROPOFF_DATETIME,
	               PASSENGER_COUNT,
	               TRIP_DISTANCE,
	               RATE_CODE_ID, 
	               STORE_AND_FWD_FLAG,
                       PICKUP_LOCATIONID,
	               DROPOFF_LOCATIONID,
	               PAYMENT_TYPE, 
	               FARE_AMOUNT, 
	               EXTRA, 
	               MTA_TAX, 
	               TIP_AMOUNT, 
	               TOLLS_AMOUNT, 
	               IMPROVEMENT_SURCHARGE, 
	               TOTAL_AMOUNT]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3),
	               RATE_CODE_ID DECIMAL(9,0), 
	               STORE_AND_FWD_FLAG VARCHAR(10),
	               PICKUP_LOCATIONID SMALLINT,
	               DROPOFF_LOCATIONID SMALLINT,
	               PAYMENT_TYPE varchar(20), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2), 
	               IMPROVEMENT_SURCHARGE DECIMAL(9,2), 
	               TOTAL_AMOUNT DECIMAL(9,2)]]
	FILE_OPTIONS='1..17'
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

/*
############################## GREEN DATA ############################## 
*/
wrapper:set_param('VENDOR_TYPE','green')  
--load green from start to 2015
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE 
                                                                        and TRIP_MONTH < '2015-01-01']]) do
	INSERT_COLS=[[VENDOR_ID,
                       PICKUP_DATETIME,
                       DROPOFF_DATETIME,
                       STORE_AND_FWD_FLAG,
                       RATE_CODE_ID,
                       PICKUP_LONGITUDE, 
	               PICKUP_LATITUDE, 
                       DROPOFF_LONGITUDE, 
	               DROPOFF_LATITUDE, 
                       PASSENGER_COUNT,
                       TRIP_DISTANCE,
                       FARE_AMOUNT,
                       EXTRA,
                       MTA_TAX,
                       TIP_AMOUNT,
                       TOLLS_AMOUNT,
                       EHAIL_FEE,
                       TOTAL_AMOUNT,
                       PAYMENT_TYPE,
                       TRIP_TYPE]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               STORE_AND_FWD_FLAG VARCHAR(10), 
	               RATE_CODE_ID DECIMAL(9,0), 
	               PICKUP_LONGITUDE DOUBLE, 
	               PICKUP_LATITUDE DOUBLE, 
	               DROPOFF_LONGITUDE DOUBLE, 
	               DROPOFF_LATITUDE DOUBLE, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2),
	               EHAIL_FEE DECIMAL(9,2),
	               TOTAL_AMOUNT DECIMAL(9,2), 
	               PAYMENT_TYPE varchar(20), 
	               TRIP_TYPE varchar(20)]]
	FILE_OPTIONS='1..20'
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

--load green from 2015-2017 (IMPROVEMENT_SURCHARGE was added)
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE 
                                                                        and TRIP_MONTH >= '2015-01-01'
                                                                        and TRIP_MONTH < '2017-01-01']]) do
	INSERT_COLS=[[VENDOR_ID,
                       PICKUP_DATETIME,
                       DROPOFF_DATETIME,
                       STORE_AND_FWD_FLAG,
                       RATE_CODE_ID,
                       PICKUP_LONGITUDE, 
	               PICKUP_LATITUDE, 
                       DROPOFF_LONGITUDE, 
	               DROPOFF_LATITUDE, 
                       PASSENGER_COUNT,
                       TRIP_DISTANCE,
                       FARE_AMOUNT,
                       EXTRA,
                       MTA_TAX,
                       TIP_AMOUNT,
                       TOLLS_AMOUNT,
                       EHAIL_FEE,
                       IMPROVEMENT_SURCHARGE,
                       TOTAL_AMOUNT,
                       PAYMENT_TYPE,
                       TRIP_TYPE]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               STORE_AND_FWD_FLAG VARCHAR(10), 
	               RATE_CODE_ID DECIMAL(9,0),
	               PICKUP_LONGITUDE DOUBLE, 
	               PICKUP_LATITUDE DOUBLE, 
	               DROPOFF_LONGITUDE DOUBLE, 
	               DROPOFF_LATITUDE DOUBLE, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2),
	               EHAIL_FEE DECIMAL(9,2), 
	               IMPROVEMENT_SURCHARGE DECIMAL(9,2), 
	               TOTAL_AMOUNT DECIMAL(9,2), 
	               PAYMENT_TYPE varchar(20), 
	               TRIP_TYPE varchar(20)]]
	FILE_OPTIONS='1..21'
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

--load green from 2017-2019 (PICKUP_LAT/LONG and DROPOFF_LAT/LONG were replaced by PICKUP/DROPOFF_LOCATIONID)
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE 
                                                                        and TRIP_MONTH >= '2017-01-01'
                                                                        and TRIP_MONTH < '2019-12-01']]) do
	INSERT_COLS=[[VENDOR_ID,
                       PICKUP_DATETIME,
                       DROPOFF_DATETIME,
                       STORE_AND_FWD_FLAG,
                       RATE_CODE_ID,
                       PICKUP_LOCATIONID,
	               DROPOFF_LOCATIONID, 
                       PASSENGER_COUNT,
                       TRIP_DISTANCE,
                       FARE_AMOUNT,
                       EXTRA,
                       MTA_TAX,
                       TIP_AMOUNT,
                       TOLLS_AMOUNT,
                       EHAIL_FEE,
                       IMPROVEMENT_SURCHARGE,
                       TOTAL_AMOUNT,
                       PAYMENT_TYPE,
                       TRIP_TYPE]]
	CREATE_COLS=[[VENDOR_ID VARCHAR(20), 
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP, 
	               STORE_AND_FWD_FLAG VARCHAR(10), 
	               RATE_CODE_ID DECIMAL(9,0),
	               PICKUP_LOCATIONID SMALLINT,
	               DROPOFF_LOCATIONID SMALLINT, 
	               PASSENGER_COUNT DECIMAL(10,0), 
	               TRIP_DISTANCE DECIMAL(18,3), 
	               FARE_AMOUNT DECIMAL(9,2),
	               EXTRA DECIMAL(9,2), 
	               MTA_TAX DECIMAL(9,2), 
	               TIP_AMOUNT DECIMAL(9,2), 
	               TOLLS_AMOUNT DECIMAL(9,2),
	               EHAIL_FEE DECIMAL(9,2), 
	               IMPROVEMENT_SURCHARGE DECIMAL(9,2), 
	               TOTAL_AMOUNT DECIMAL(9,2), 
	               PAYMENT_TYPE varchar(20), 
	               TRIP_TYPE varchar(20)]]
	FILE_OPTIONS='1..19'
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

/*
############################## HVFHV DATA ############################## 
*/
wrapper:set_param('VENDOR_TYPE','fhvhv')  
--load high volume-fhv from 2019
for FILE_ID,TRIP_MONTH,SITE_URL,FILENAME in wrapper:query_values( [[    select ID, TRIP_MONTH, SITE_URL,FILENAME 
                                                                        from ::STAGE_SCM.RAW_DATA_URLS 
                                                                        where LOADED_TIMESTAMP is NULL 
                                                                        and TYPE=:VENDOR_TYPE
                                                                        and TRIP_MONTH < '2019-12-01']]) do
	INSERT_COLS=[[HVFHS_LICENSE_NUM,
	               PICKUP_DATETIME,
                       DROPOFF_DATETIME,
                       PICKUP_LOCATIONID,
	               DROPOFF_LOCATIONID, 
                       STORE_AND_FWD_FLAG
                       ]]
       CREATE_COLS=[[HVFHS_LICENSE_NUM VARCHAR(20),
	               PICKUP_DATETIME TIMESTAMP, 
	               DROPOFF_DATETIME TIMESTAMP,
	               PICKUP_LOCATIONID SMALLINT,
	               DROPOFF_LOCATIONID SMALLINT,
                       STORE_AND_FWD_FLAG VARCHAR(10)]]
	FILE_OPTIONS='1,3..7'
	process_file(  FILE_ID,TRIP_MONTH,
	               SITE_URL,FILENAME,
	               INSERT_COLS,
	               CREATE_COLS,
	               FILE_OPTIONS)
end

return wrapper:finish()
/