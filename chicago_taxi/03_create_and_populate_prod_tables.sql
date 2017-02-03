

/********** create PROD tables  ********/

CREATE or replace TABLE &PRODSCM..taxis (
  taxi_sid decimal(9) identity primary key not null,
  taxi_id varchar(128) not null
);

create or replace table &PRODSCM..companies(company_sid decimal(9) identity primary key not null, company varchar(100) not null);


--attention: company can be null insert dummy company to ensure also inner joins match
create or replace table &PRODSCM..payment_types(payment_type_sid decimal(9) identity primary key, payment_type varchar(100));

create or replace table
	&PRODSCM..COMMUNITY_AREAS(
		COMMUNITY_AREA_ID DECIMAL(9) primary key not null,
		COMMUNITY_NAME varchar(100)
--,
--		SHAPE_AREA double,
--		SHAPE_LEN double
	);

create or
replace table
	&PRODSCM..CENSUS_TRACTS(
		CENSUS_TRACT_ID decimal(11) primary key not null,
		TRACTCE10 decimal(9),
		NAME10 DECIMAL(9),
		NAMELSAD10 varchar(100),
		COMMAREA decimal(9),
		NOTES varchar(1000)
	);


create or replace table &PRODSCM..taxi_trips(
  trip_id char(40) ascii primary key,
  taxi_sid decimal(9) REFERENCES &PRODSCM..TAXIS(TAXI_SID),
  trip_start_timestamp timestamp without time zone,
  trip_end_timestamp timestamp without time zone,
  trip_seconds decimal(9),
  trip_miles decimal(9,2),
  pickup_census_tract decimal(11) REFERENCES &PRODSCM..CENSUS_TRACTS(CENSUS_TRACT_ID),
  dropoff_census_tract decimal(11) REFERENCES &PRODSCM..CENSUS_TRACTS(CENSUS_TRACT_ID), 
  pickup_community_area decimal(9) REFERENCES &PRODSCM..COMMUNITY_AREAS (COMMUNITY_AREA_ID),
  dropoff_community_area decimal(9) REFERENCES &PRODSCM..COMMUNITY_AREAS (COMMUNITY_AREA_ID),
  fare decimal(9,2),
  tips decimal(9,2),
  tolls decimal(9,2),
  extras decimal(9,2),
  trip_total decimal(9,2),
  payment_type_sid decimal(9) REFERENCES &PRODSCM..PAYMENT_TYPES(PAYMENT_TYPE_SID),
  company_sid decimal(9) REFERENCES &PRODSCM..COMPANIES(COMPANY_SID),
  pickup_centroid_latitude decimal(18,9),
  pickup_centroid_longitude decimal(18,9),
--  pickup_centroid_location varchar(100),
  dropoff_centroid_latitude numeric(18,9),
  dropoff_centroid_longitude numeric(18,9)
--,
--  dropoff_centroid_location varchar(100),
--  community_areas decimal
)
;



/******** populate PROD tables ***************/

/**  
Notes: 

Adjustments to make it easier for people working with the dataset:

Community and census_track information are sometimes masked due to privacy reasons.
For details see http://digital.cityofchicago.org/index.php/chicago-taxi-data-released/
To let users be aware of these entries the string '** masked for privacy **' has been inserted into dimension tables

null values for COMPANY (an in the future payment_type if any) are replaced with 'UNKNOWN'

for more information take a look at the code below.

**/



INSERT INTO &PRODSCM..taxis (taxi_id) SELECT DISTINCT taxi_id FROM &STAGESCM..trips_raw WHERE taxi_id NOT IN (SELECT distinct taxi_id FROM &PRODSCM..taxis);

insert into &PRODSCM..companies(company) select distinct NVL(COMPANY,'UNKNOWN') comp from &STAGESCM..trips_raw where local.comp not in (select company from &PRODSCM..companies);

insert into &PRODSCM..payment_types(payment_Type) select distinct NVL(payment_type,'UNKNOWN') pt from &STAGESCM..trips_raw where local.pt not in (select payment_type from &PRODSCM..payment_types);



insert into &PRODSCM..community_areas (COMMUNITY_AREA_ID, COMMUNITY_NAME) 
select * from (
	select -1 as COMMUNITY_AREA_ID,'** masked for privacy **' from dual
	union all
	select area_numbe, community from
	&STAGESCM..COMMUNITY_AREAS 
)where community_area_id not in (select community_area_id from &PRODSCM..community_areas);


--for ease of use via frontends -> create dimension entries + information about masking
insert into &PRODSCM..census_tracts (CENSUS_TRACT_ID, TRACTCE10, NAME10, NAMELSAD10, COMMAREA, NOTES) 
select * from (
with vv_ct as (
select GEOID10, TRACTCE10, NAME10, NAMELSAD10, COMMAREA, NOTES from &STAGESCM..CENSUS_TRACTS
)
, vv_pickups as (
select distinct cast(PICKUP_CENSUS_TRACT as decimal(11)) as geoid10,null as tractce10,null as name10,cast(PICKUP_CENSUS_TRACT as varchar(100)) as namelsad10,null as commarea,'** not found in census tracts data from 2010 **' as notes
 from &STAGESCM..TRIPS_RAW where PICKUP_CENSUS_TRACT not in (select geoid10 from vv_ct )
)
, vv_dropoffs as (
select distinct cast(DROPOFF_CENSUS_TRACT as decimal(11)) as geoid10,null as tractce10,null as name10,cast(DROPOFF_CENSUS_TRACT as varchar(100)) as namelsad10,null as commarea,'** not found in census tracts data from 2010 **' as notes
 from &STAGESCM..TRIPS_RAW where DROPOFF_CENSUS_TRACT not in (select geoid10 from vv_ct union all select geoid10 from vv_pickups )
)
select * from vv_ct
union all
select * from vv_pickups
union all
select * from vv_dropoffs
UNION ALL
select -1 as geoid10,null,null,'** masked for privacy **',null,'** masked for privacy **' from dual
)
where geoid10 not in (select census_tract_id from &PRODSCM..census_tracts)
;

merge into
	&PRODSCM..taxi_trips tgt
using
	(
		select
			raw.*,
			taxi_sid,
			payment_type_sid,
			company_sid
		from
				&STAGESCM..trips_raw raw
			left join
				&PRODSCM..taxis ta
			on
				raw.taxi_id = ta.taxi_id
			left join
				&PRODSCM..payment_types pt
			on
				pt.payment_type = NVL(raw.payment_type,'UNKNOWN')
			left join
				&PRODSCM..companies c
			on
				c.company = NVL(raw.COMPANY, 'UNKNOWN')
	) src
on
	tgt.trip_id = src.trip_Id
when not matched then
	insert values
		(
			trip_id,
			taxi_sid,
			trip_start_timestamp,
			trip_end_timestamp,
			trip_seconds,
			trip_miles,
			NVL(PICKUP_CENSUS_TRACT,-1),
			NVL(DROPOFF_CENSUS_TRACT,-1),
			nvl(PICKUP_COMMUNITY_AREA,-1),
			nvl(DROPOFF_COMMUNITY_AREA,-1),
			replace(FARE, '$', ''),
			replace(TIPS, '$', ''),
			replace(TOLLS, '$', ''),
			replace(EXTRAS, '$', ''),
			replace(TRIP_TOTAL, '$', ''),
			PAYMENT_TYPE_sid,
			COMPANY_sid,
			PICKUP_CENTROID_LATITUDE,
			PICKUP_CENTROID_LONGITUDE,
			DROPOFF_CENTROID_LATITUDE,
			DROPOFF_CENTROID_LONGITUDE
		);


-- now have fun with your data