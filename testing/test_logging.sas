%LET rootdir = \\JDCSASOAPRD01\SASData\CPRU;
%LET jordan_dir = &rootdir.\Jordan;

%let log_name = &jordan_dir\logs\test-log4.log;

%LET message = Hello World;
%PUT &message.;
