%let log_name = &jordan_dir.\logs\test-log3.log;
PROC PRINTTO LOG="&log_name."; RUN;
%LET rootdir = \\JDCSASOAPRD01\SASData\CPRU;
%LET jordan_dir = &rootdir.\Jordan\;



%LET message = Hello silly SAS user, this is just a test;
%PUT &message.;
