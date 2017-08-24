--!!!PRECONDITION!!!! create QueryWrapper Scripts (used for logging)
--load it from https://raw.githubusercontent.com/EXASOL/etl-utils/master/query_wrapper.sql 

create or replace table
	&STAGESCM..PRESCRIPTIONS(
		SHA CHAR(3),
		PCT CHAR(3),
		PRACTICE CHAR(6),
		BNF_CODE CHAR(15),
		BNF_NAME VARCHAR(50),
		ITEMS DECIMAL(9),
		NIC DECIMAL(10, 2),
		ACT_COST DECIMAL(10, 2),
		QUANTITY DECIMAL(9),
		PERIOD DECIMAL(6)
	); 

create or replace table &STAGESCM..CHEMICAL_SUBSTANCES(
CHEM_SUB CHAR(9),CHEMICAL_NAME varchar(1000), unknown_field varchar(1000));


create or replace table
	&PRODSCM..CHEMICAL_SUBSTANCES(
		SK_CHEM_SUB DECIMAL(9) IDENTITY NOT NULL,
		CHEM_SUB CHAR(9) NOT NULL,
		CHEMICAL_NAME VARCHAR(40) NOT NULL,
		PRIMARY KEY(SK_CHEM_SUB)
	);

--insert dummy value cause some BNF_CODE values have no data in Lookup table
insert into &PRODSCM..CHEMICAL_SUBSTANCES (CHEM_SUB,CHEMICAL_NAME) values ('*NO_DATA*','*** DATA MISSING IN LOOKUP TBL ***');


create or replace table
	&STAGESCM..PRACTICE_ADDRESS(
		PERIOD decimal(6),
		PRACTICE CHAR(6),
		PRACTICE_NAME VARCHAR(50),
		ADDRESS_PART1 varchar(50),
		ADDRESS_PART2 varchar(50),
		ADDRESS_PART3 VARCHAR(50),
		ADDRESS_PART4 varchar(50),
		POSTCODE_FULL varchar(50)
	);




create or replace table &PRODSCM..PRACTICE_ADDRESS (
		SK_PRACTICE_ADDRESS DECIMAL(9) IDENTITY NOT NULL,
		PERIOD decimal(6) NOT NULL,
		PRACTICE CHAR(6) NOT NULL,
		PRACTICE_NAME VARCHAR(50),
		ADDRESS_PART1 varchar(50),
		ADDRESS_PART2 varchar(50),
		ADDRESS_PART3 VARCHAR(50),
		ADDRESS_PART4 varchar(50),
		POSTCODE_FULL varchar(50) NOT NULL, --detailed information (as it is in the data with outwards and inwards code
		POSTCODE VARCHAR(5) NOT NULL -- only outwards code
		, PRIMARY KEY(SK_PRACTICE_ADDRESS)
	);



create or
replace table
	&PRODSCM..PRESCRIPTIONS(
		SHA CHAR(3) NOT NULL,
		PCT CHAR(3) NOT NULL,
		PRACTICE CHAR(6) NOT NULL,
		BNF_CODE CHAR(15) NOT NULL,
		BNF_NAME VARCHAR(50),
		ITEMS DECIMAL(9),
		NIC DECIMAL(10, 2),
		ACT_COST DECIMAL(10, 2),
		QUANTITY DECIMAL(9),
		PERIOD DECIMAL(6),
		PERIOD_FIRST_DAY_AS_DATE DATE, -- for convenience when using frontends
		SK_CHEM_SUB decimal(9) NOT NULL, -- added to make the lookup easier
		SK_PRACTICE_ADDRESS DECIMAL(9) NOT NULL, -- added to make the lookup easier
		FOREIGN KEY(SK_CHEM_SUB) REFERENCES &PRODSCM..CHEMICAL_SUBSTANCES(SK_CHEM_SUB),
		FOREIGN KEY(SK_PRACTICE_ADDRESS) REFERENCES &PRODSCM..PRACTICE_ADDRESS(SK_PRACTICE_ADDRESS)
	); 





create or replace table
	&STAGESCM..raw_data_urls(
		description varchar(1000),
		data_format varchar(100),
		data_path varchar(1000),
		site_url varchar(10000),
		file_name varchar(1000),
		period decimal(6),
		loaded_timestamp timestamp
	);




--setup logging
create or
replace table
	&STAGESCM..job_log(
		run_id int identity,
		script_name varchar(100),
		status varchar(100),
		start_time timestamp default systimestamp,
		end_time timestamp
	);
		
create or
replace table
	&STAGESCM..job_details(
		detail_id int identity,
		run_id int,
		log_time timestamp,
		log_level varchar(10),
		log_message varchar(2000),
		rowcount int
	);


--set distribution keys for local joins (reduced network communication)
ALTER TABLE &PRODSCM..PRACTICE_ADDRESS DISTRIBUTE BY SK_PRACTICE_ADDRESS;
ALTER TABLE &PRODSCM..PRESCRIPTIONS DISTRIBUTE BY SK_PRACTICE_ADDRESS;




