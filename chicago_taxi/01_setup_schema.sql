
--use variables for the SCHEMA names
define STAGESCM=CHICAGO_TAXI_STAGE;
define PRODSCM=CHICAGO_TAXI;

create schema &PRODSCM;
create schema &STAGESCM;
