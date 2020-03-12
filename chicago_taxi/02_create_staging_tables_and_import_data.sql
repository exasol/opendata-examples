-- Create staging tables and import data from sources
-- Prerequisites: your EXASOL database should be able to connect to the internet and should have a dns-server defined

CREATE OR REPLACE TABLE &STAGESCM..trips_raw (
  trip_id varchar(40),
  taxi_id varchar(255),
  trip_start_timestamp timestamp without time zone,
  trip_end_timestamp timestamp without time zone,
  trip_seconds decimal(9),
  trip_miles decimal(18,4),
  pickup_census_tract varchar(100),
  dropoff_census_tract varchar(100),
  pickup_community_area decimal,
  dropoff_community_area decimal,
  fare varchar(100),
  tips varchar(100),
  tolls varchar(100),
  extras varchar(100),
  trip_total varchar(100),
  payment_type varchar(100),
  company varchar(100),
  pickup_centroid_latitude decimal(18,9),
  pickup_centroid_longitude decimal(18,9),
  pickup_centroid_location varchar(100),
  dropoff_centroid_latitude numeric(18,9),
  dropoff_centroid_longitude numeric(18,9),
  dropoff_centroid_location varchar(100),
  community_areas decimal
);

-- Import all staging tables directly from the Chicago Taxi site

IMPORT INTO &STAGESCM..trips_raw FROM CSV AT 'https://data.cityofchicago.org/api/views/wrvz-psew' FILE 'rows.csv?accessType=DOWNLOAD'
(1..2,3 FORMAT='MM/DD/YYYY HH12:MI:SS AM',4  FORMAT='MM/DD/YYYY HH12:MI:SS AM',5..24) SKIP=1;

CREATE OR REPLACE TABLE	&STAGESCM..CENSUS_TRACTS(
		THE_GEOM VARCHAR(2000000),
		STATEFP10 decimal(9),
		COUNTYFP10 DECIMAL(9),
		TRACTCE10 decimal(9),
		GEOID10 decimal(11),
		NAME10 DECIMAL(9),
		NAMELSAD10 varchar(100),
		COMMAREA decimal(9),
		COMMAREA_N decimal(9),
		NOTES varchar(1000)
	);
IMPORT INTO &STAGESCM..CENSUS_TRACTS FROM CSV AT 'https://data.cityofchicago.org/api/views/74p9-q2aq' FILE 'rows.csv?accessType=DOWNLOAD' SKIP=1;

CREATE OR REPLACE TABLE	&STAGESCM..COMMUNITY_AREAS(
		the_geom varchar(2000000),
		PERIMETER DECIMAL(9),
		AREA DECIMAL(9),
		COMAREA_ DECIMAL(9),
		COMAREA_ID DECIMAL(9),
		AREA_NUMBE DECIMAL(9),
		COMMUNITY varchar(100),
		AREA_NUM_1 DECIMAL(9),
		SHAPE_AREA double,
		SHAPE_LEN double
	);
IMPORT INTO &STAGESCM..COMMUNITY_AREAS FROM CSV AT 'https://data.cityofchicago.org/api/views/igwz-8jzy' FILE 'rows.csv?accessType=DOWNLOAD' SKIP=1;
