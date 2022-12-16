options armsubsys = (arm_none);

%LET message = Hello silly SAS user, this is just a test;
%PUT &message.;

PROC SQL NOPRINT;
    create table _tmp AS
    SELECT *
    FROM SASHELP.cars
    ;
QUIT;
