# Chicago Taxi Dataset

Example to load [Chicago's taxi dataset](http://digital.cityofchicago.org/index.php/chicago-taxi-data-released/) in an EXASOL database.

##Instructions

### Prerequisites: 
- EXASOL database, that is allowed to connect to the Internet and that has access to a nameserver
- EXAplus (SQL Client)

### Importing the dataset
Run the sql files in the following order via EXAplus
- 01_setup_schema.sql
- 02_create_staging_tables_and_import_data.sql
- 03_create_and_populate_prod_tables.sql

## Related Material
City of Chicago: 
- [Taxi Trips](https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew)
- [Community Areas](https://data.cityofchicago.org/d/cauq-8yn6)
- [Census Tracks](https://data.cityofchicago.org/d/5jrd-6zik)

Todd W. Schneider:
- Nice preparation to load the dataset into postgres: [toddwschneider/chicago-taxi-data](https://github.com/toddwschneider/chicago-taxi-data)
