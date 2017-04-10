These scripts are intended to create a tutorial database for people who have downloaded EXASOL's EXASolo/FSBE.

You have the choice to create two sizes of database with the attached code  - you can either create START_BIG which has about 6GB of RAW data or START_SMALL which has only 10MB.

These have the following row counts :-

	                START_SMALL	        START_BIG
SALES	            approx 350,000      approx 200 million
SALES AGENT	      10,000	            100,000
SALES TERRITORY	  1,000	              10,000
COUNTRY	          218	                218

The START_BIG database is intended for those with more powerful hardware - it is possible to generate it on a laptop, but it will take anything up to 10 minutes to run.

Everything you need to create the database is in these scripts - the data is generated randomly and so there is no need to download any files. 

Also included here is a file of sample SQL statements that can be run against these databases and also a COUNTRY.CSV file which you can upload.
