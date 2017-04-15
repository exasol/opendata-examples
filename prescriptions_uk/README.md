# UK Prescribing Data

Example to load [GP practice prescribing data - Presentation level](https://data.gov.uk/dataset/prescribing-by-gp-practice-presentation-level) in an EXASOL database.


### Prerequisites: 
- EXASOL database, that is allowed to connect to the Internet and that has access to a nameserver
- EXAplus (SQL Client)

### Importing the dataset
Run the sql files in the following order via EXAplus
- 01_setup_schema.sql
- 02_create_tables.sql
- 03_create_scripts.sql
- 04_load_data.sql

## Related Material
data.gov.uk: 
- [GP practice prescribing data](https://data.gov.uk/dataset/prescribing-by-gp-practice-presentation-level)
