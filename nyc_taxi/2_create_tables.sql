--setup logging
CREATE OR REPLACE TABLE
	NYC_TAXI_STAGE.job_log(
		run_id INT IDENTITY,
		script_name VARCHAR(100),
		status VARCHAR(100),
		start_time TIMESTAMP DEFAULT SYSTIMESTAMP,
		end_time TIMESTAMP
	);
		
CREATE OR REPLACE TABLE
	NYC_TAXI_STAGE.job_details(
		detail_id INT IDENTITY,
		run_id INT,
		log_time TIMESTAMP,
		log_level VARCHAR(10),
		log_message VARCHAR(2000),
		rowcount INT
	);

--create staging tables for URLs
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.stage_raw_data_urls(url VARCHAR(1000));
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.raw_data_urls(   id DECIMAL IDENTITY, 
                                                        type VARCHAR(50), 
                                                        trip_month date, 
                                                        site_url VARCHAR(1000),
                                                        filename VARCHAR(1000), 
                                                        loaded_timestamp TIMESTAMP);
--create staging table for geo clustering
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.trip_location_id_map (   trip_id decimal(12), 
                                                                PICKUP_LOCATIONID DECIMAL(3), 
                                                                DROPOFF_LOCATIONID DECIMAL(3));
                                                                
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.spatial_grid (   location_id DECIMAL(3),
                                                        grid_segment GEOMETRY(4326),
                                                        segment_id DECIMAL(3));

--stage and prod trip tables
CREATE OR REPLACE TABLE NYC_TAXI_STAGE.TRIPDATA  (
        ID                      DECIMAL(12) IDENTITY,
        FILE_ID		        DECIMAL(9),
        VENDOR_TYPE             VARCHAR(20),
        VENDOR_ID               VARCHAR(20),
        HVFHS_LICENSE_NUM       VARCHAR(20),
        PICKUP_DATETIME         TIMESTAMP,
        DROPOFF_DATETIME        TIMESTAMP,
        PASSENGER_COUNT         DECIMAL(10),
        TRIP_DISTANCE           DECIMAL(18,3),
        PICKUP_LONGITUDE        DOUBLE PRECISION,
        PICKUP_LATITUDE         DOUBLE PRECISION,
        PICKUP_LOCATIONID       SMALLINT,
        RATE_CODE_ID            DECIMAL(9,0),
        STORE_AND_FWD_FLAG      VARCHAR(10) ASCII,
        DROPOFF_LONGITUDE       DOUBLE PRECISION,
        DROPOFF_LATITUDE        DOUBLE PRECISION,
        DROPOFF_LOCATIONID      SMALLINT,
        PAYMENT_TYPE            varchar(20),
        FARE_AMOUNT             DECIMAL(9,2),
        EXTRA                   DECIMAL(9,2),
        MTA_TAX                 DECIMAL(9,2),
        TIP_AMOUNT              DECIMAL(9,2),
        TOLLS_AMOUNT            DECIMAL(9,2),
        EHAIL_FEE               DECIMAL(9,2),
        IMPROVEMENT_SURCHARGE   DECIMAL(9,2),
        TOTAL_AMOUNT            DECIMAL(9,2),
        TRIP_TYPE		DECIMAL(9),
        PICK_GEOM               GEOMETRY(4326),
        DROP_GEOM               GEOMETRY(4326)
);

CREATE OR REPLACE TABLE NYC_TAXI.TRIPS (
        ID		      DECIMAL(12),
        SRC_FILE_ID	      DECIMAL(9),
        CAB_TYPE_ID           DECIMAL(9),
        VENDORID              VARCHAR(9) ASCII,
        HVFHS_LICENSE_NUM_ID  DECIMAL(1),
        PICKUP_DATE           DATE,
        DROPOFF_DATE          DATE,
        PICKUP_DATETIME       TIMESTAMP,
        DROPOFF_DATETIME      TIMESTAMP,
        STORE_AND_FWD_FLAG    CHAR(1) ASCII,
        RATE_CODE_ID          DECIMAL(3),
        PICKUP_LONGITUDE      DOUBLE,
        PICKUP_LATITUDE       DOUBLE,
        PICKUP_LOCATION_ID     DECIMAL(3),
        DROPOFF_LONGITUDE     DOUBLE,
        DROPOFF_LATITUDE      DOUBLE,
        DROPOFF_LOCATION_ID    DECIMAL(3),
        PASSENGER_COUNT       DECIMAL(10),
        TRIP_DISTANCE         DECIMAL(12,3),
        FARE_AMOUNT           DECIMAL(9,2),
        EXTRA                 DECIMAL(9,2),
        MTA_TAX               DECIMAL(9,2),
        TIP_AMOUNT            DECIMAL(9,2),
        TOLLS_AMOUNT          DECIMAL(9,2),
        EHAIL_FEE             DECIMAL(9,2),
        IMPROVEMENT_SURCHARGE DECIMAL(9,2),
        TOTAL_AMOUNT          DECIMAL(9,2),
        PAYMENT_TYPE          VARCHAR(20) ASCII,
        TRIP_TYPE             VARCHAR(10) ASCII);

-- LOOKUP TABLES
CREATE OR REPLACE TABLE NYC_TAXI.cab_types (    cab_type_id     DECIMAL(1) IDENTITY PRIMARY KEY,
                                                type            VARCHAR(20) ASCII);

CREATE OR REPLACE TABLE NYC_TAXI.taxi_zones(    location_id     DECIMAL(3), 
                                                borough         VARCHAR(20), 
                                                zone_name       VARCHAR(50), 
                                                service_zone    VARCHAR(20),
                                                zone_length     DOUBLE,
                                                polygon         GEOMETRY(4326),
                                                zone_area       DOUBLE);
                        
CREATE OR REPLACE TABLE NYC_TAXI.vendor_lookup( vendor_id       SMALLINT,
                                                vendor_name     VARCHAR(200),
                                                vendor_short    VARCHAR(200));

CREATE OR REPLACE TABLE NYC_TAXI.trip_type_lookup(      trip_type_id    DECIMAL(1),
                                                        trip_type       VARCHAR(200));
                
CREATE OR REPLACE TABLE NYC_TAXI.rate_code_lookup(      rate_code_id    DECIMAL(1),
                                                        rate_code       VARCHAR(100));
                                                        
CREATE OR REPLACE TABLE NYC_TAXI.hvfhs_license_lookup(  id decimal(3) IDENTITY PRIMARY KEY,
                                                        high_volume_license_number varchar(10),
                                                        name varchar(10));