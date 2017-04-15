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

Scripts are implemented in a way, that a delta load should be easily possible in case new data gets published (as long as the format stays the same).
Furthermore the load-script can be resumed (reloading the last/current file) in case a filetransfer got interrupted).

## Related Material
data.gov.uk: 
- [GP practice prescribing data](https://data.gov.uk/dataset/prescribing-by-gp-practice-presentation-level)
