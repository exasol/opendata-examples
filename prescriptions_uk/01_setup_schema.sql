
--use variables for the SCHEMA names
define STAGESCM=PRESCRIPTIONS_UK_STAGE;
define PRODSCM=PRESCRIPTIONS_UK;

create schema &PRODSCM;
create schema &STAGESCM;
