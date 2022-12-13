%LET rootdir = \\JDCSASOAPRD01\SASData\CPRU;
%LET jordan_dir = &rootdir.\Jordan;

%let log_name = &jordan_dir\logs\test-log4.log;
{{%sas%}}
PROC PRINTTO LOG="&log_name."; RUN;
%LET message = Hello silly SAS user, this is just a test;
%PUT &message.;
